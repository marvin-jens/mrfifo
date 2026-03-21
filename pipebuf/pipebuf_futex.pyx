# cython: language_level=3
# pipebuf_futex.pyx -- SPSC ring-buffer using 32-bit futex cursors (closed in top bit)
# Aligned messages: all messages (header+payload) are padded to 4 bytes, and capacity is multiple-of-4.

from libc.stdint cimport uint32_t, uint64_t, int64_t, uint8_t
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, memset
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AS_STRING

# errno constants and errno variable
from libc.errno cimport errno, EAGAIN, EINTR

cdef extern from "unistd.h":
    int close(int fd)
    long syscall(long number, ...)
    int ftruncate(int fd, int length)
    int unlink(const char *pathname)

cdef extern from "sys/mman.h":
    void *mmap(void *addr, size_t length, int prot, int flags, int fd, size_t offset) nogil
    int munmap(void *addr, size_t length)
    int PROT_READ
    int PROT_WRITE
    int MAP_SHARED
    void *NULL

cdef extern from "fcntl.h":
    int open(const char *pathname, int flags, int mode)
    int O_CREAT
    int O_RDWR
    int O_EXCL
    int O_CLOEXEC

cdef extern from "sys/stat.h":
    int S_IRUSR
    int S_IWUSR
    int S_IRGRP
    int S_IWGRP
    int S_IROTH
    int S_IWOTH

# syscall number for futex
cdef extern from "sys/syscall.h":
    int SYS_futex

# futex ops
cdef int FUTEX_WAIT = 0
cdef int FUTEX_WAKE = 1

# atomic builtins declarations
cdef extern from *:
    unsigned int __atomic_load_n(unsigned int *ptr, int memorder)
    void __atomic_store_n(unsigned int *ptr, unsigned int val, int memorder)
    unsigned int __atomic_fetch_add(unsigned int *ptr, unsigned int val, int memorder)
    unsigned int __atomic_fetch_or(unsigned int *ptr, unsigned int val, int memorder)
    bint __atomic_compare_exchange_n(unsigned int *ptr,
                                     unsigned int *expected,
                                     unsigned int desired,
                                     bint weak,
                                     int success_memorder,
                                     int failure_memorder)

# atomic memory order constants
cdef int __ATOMIC_RELAXED = 0
cdef int __ATOMIC_CONSUME = 1
cdef int __ATOMIC_ACQUIRE = 2
cdef int __ATOMIC_RELEASE = 3
cdef int __ATOMIC_ACQ_REL = 4
cdef int __ATOMIC_SEQ_CST = 5

# helpers for futex
cdef enum FutexWaitResult:
    FUTEX_OK = 0
    FUTEX_EAGAIN = 1
    FUTEX_EINTR = 2
    FUTEX_ERR = -1

cdef inline int _raw_futex_wait(unsigned int *uaddr, unsigned int expected):
    """
    Direct futex syscall wrapper. Returns syscall return value (0 or -1).
    Caller should inspect errno for EINTR/EAGAIN.
    """
    cdef long r
    r = syscall(SYS_futex, <void*>uaddr, FUTEX_WAIT, expected, NULL, NULL, 0)
    return <int> r

cdef inline int futex_wait_checked(unsigned int *uaddr, unsigned int expected):
    """
    Wrapper that handles EINTR/EAGAIN gracefully:
      - returns FUTEX_OK (0) if futex returned 0 (we slept & were woken)
      - returns FUTEX_EAGAIN if value already changed
      - returns FUTEX_EINTR if interrupted
      - returns FUTEX_ERR for other errors
    The caller should re-check their condition after any return from this function.
    """
    cdef int r
    cdef int e
    r = _raw_futex_wait(uaddr, expected)
    if r == 0:
        return FutexWaitResult.FUTEX_OK
    else:
        e = errno
        if e == EAGAIN:
            return FutexWaitResult.FUTEX_EAGAIN
        if e == EINTR:
            return FutexWaitResult.FUTEX_EINTR
        return FutexWaitResult.FUTEX_EAGAIN

cdef inline int futex_wake_checked(unsigned int *uaddr, int count):
    cdef long r
    r = syscall(SYS_futex, <void*>uaddr, FUTEX_WAKE, count, NULL, NULL, 0)
    return <int> r

# header layout (32-bit cursors)
cdef unsigned int HEADER_MAGIC = 0x70697062  # "pipb"
cdef unsigned int HEADER_VERSION = 1

cdef packed struct Header:
    uint32_t magic
    uint32_t version
    uint32_t writer_cursor   # futex word: high bit = closed, low 31 bits = sequence (bytes written)
    uint32_t reader_cursor   # futex word: high bit = closed, low 31 bits = sequence (bytes read)
    uint32_t capacity        # bytes (must be multiple of 4 and < 1<<31)
    uint8_t padding[4]       # pad

cdef size_t HEADER_SIZE = sizeof(Header)

# top-bit closed and 31-bit sequence mask
cdef unsigned int CLOSED_BIT = 0x80000000
cdef unsigned int SEQ_MASK   = 0x7FFFFFFF

cdef inline unsigned int seq_of(unsigned int v):
    return v & SEQ_MASK

cdef inline bint is_closed(unsigned int v):
    return (v & CLOSED_BIT) != 0

cdef inline unsigned int pack_seq_closed(unsigned int seq, bint closed):
    return (seq & SEQ_MASK) | (CLOSED_BIT if closed else 0)

cdef inline bytes shm_path_from_name(bytes name):
    return b"/dev/shm/pipebuf-" + name

cdef class PipeBufFutex:
    cdef public str name
    cdef int fd
    cdef void* base
    cdef size_t map_size
    cdef Header* hdr
    cdef char* buf
    cdef unsigned int capacity

    def __dealloc__(self):
        try:
            self.close()
        except:
            pass

    @staticmethod
    def create(name: str, capacity: int):
        cdef PipeBufFutex pb = PipeBufFutex.__new__(PipeBufFutex)
        if capacity <= 0 or capacity >= (1 << 31):
            raise ValueError("capacity must be >0 and < 2^31")
        # enforce multiple-of-4 capacity for alignment
        cdef unsigned int cap = <unsigned int> capacity
        cap &= ~0x3u
        if cap < 4:
            raise ValueError("capacity must be at least 4 and multiple of 4")
        cdef bytes path = shm_path_from_name(name.encode('utf-8'))
        cdef int fd = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
                          S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)
        if fd < 0:
            raise OSError("open/create shm failed (exists?)")
        cdef size_t map_size = HEADER_SIZE + <size_t> cap
        if ftruncate(fd, <int>map_size) != 0:
            close(fd)
            raise OSError("ftruncate failed")
        cdef void* base = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
        if base == <void*> -1:
            close(fd)
            raise OSError("mmap failed")
        cdef Header* hdr = <Header*> base
        hdr.magic = HEADER_MAGIC
        hdr.version = HEADER_VERSION
        hdr.writer_cursor = 0
        hdr.reader_cursor = 0
        hdr.capacity = <uint32_t> cap
        memset(<void*> ( (<char*> base) + HEADER_SIZE ), 0, cap)
        pb.fd = fd
        pb.base = base
        pb.map_size = map_size
        pb.hdr = hdr
        pb.buf = <char*> base + HEADER_SIZE
        pb.capacity = cap
        pb.name = name
        return pb

    @staticmethod
    def open(name: str):
        cdef PipeBufFutex pb = PipeBufFutex.__new__(PipeBufFutex)
        cdef bytes path = shm_path_from_name(name.encode('utf-8'))
        cdef int fd = open(path, O_RDWR | O_CLOEXEC, 0)
        if fd < 0:
            raise OSError("open shm failed (not found?)")
        import os
        st = os.fstat(fd)
        cdef size_t map_size = st.st_size
        cdef void* base = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
        if base == <void*> -1:
            close(fd)
            raise OSError("mmap failed")
        cdef Header* hdr = <Header*> base
        if hdr.magic != HEADER_MAGIC:
            munmap(base, map_size); close(fd)
            raise OSError("bad magic")
        # sanity-check capacity multiple-of-4
        if (hdr.capacity & 0x3u) != 0:
            munmap(base, map_size); close(fd)
            raise OSError("shm capacity not multiple of 4")
        pb.fd = fd
        pb.base = base
        pb.map_size = map_size
        pb.hdr = hdr
        pb.buf = <char*> base + HEADER_SIZE
        pb.capacity = hdr.capacity
        pb.name = name
        return pb

    cpdef close(self):
        if self.base != NULL:
            munmap(self.base, self.map_size)
            self.base = NULL
        if self.fd:
            try:
                close(self.fd)
            except:
                pass
            self.fd = 0

    cpdef unlink(self):
        cdef bytes path = shm_path_from_name(self.name.encode('utf-8'))
        unlink(path)

    cpdef close_for_write(self):
        """Writer: mark writer cursor closed and wake reader (conservative wake both words)."""
        cdef unsigned int *wptr = &self.hdr.writer_cursor
        __atomic_fetch_or(wptr, CLOSED_BIT, __ATOMIC_RELEASE)
        futex_wake_checked(wptr, 1)
        futex_wake_checked(&self.hdr.reader_cursor, 1)

    cpdef close_for_read(self):
        """Reader: indicates reader closed (rare error path) and wake writer (conservative)."""
        cdef unsigned int *rptr = &self.hdr.reader_cursor
        __atomic_fetch_or(rptr, CLOSED_BIT, __ATOMIC_RELEASE)
        futex_wake_checked(rptr, 1)
        futex_wake_checked(&self.hdr.writer_cursor, 1)

    cdef void _write_bytes_at(self, unsigned int start, const char *src, unsigned int total):
        cdef unsigned int cap = self.capacity
        cdef unsigned int first
        if start + total <= cap:
            memcpy(<void*> (self.buf + start), <const void*> src, total)
        else:
            first = cap - start
            memcpy(<void*> (self.buf + start), <const void*> src, first)
            memcpy(<void*> self.buf, <const void*> (src + first), total - first)

    cdef void _read_bytes_at(self, unsigned int start, char *dst, unsigned int total):
        cdef unsigned int cap = self.capacity
        cdef unsigned int first
        if start + total <= cap:
            memcpy(<void*> dst, <void*> (self.buf + start), total)
        else:
            first = cap - start
            memcpy(<void*> dst, <void*> (self.buf + start), first)
            memcpy(<void*> (dst + first), <void*> self.buf, total - first)

    def write_line(self, data: bytes):
        """
        Single-message write. Message header (4 bytes uint32 native) + payload,
        but the cursor advances by padded_tot = round_up_to_4(header+payload).
        """
        # cdef locals (must be all declared before executable code)
        cdef unsigned int msg_len
        cdef unsigned int tot
        cdef unsigned int padded_tot
        cdef unsigned int *wptr
        cdef unsigned int *rptr
        cdef const char *dptr
        cdef unsigned int r, w, rseq, wseq, used, free, start
        cdef unsigned int cap
        cdef unsigned int first, payload_start
        cdef uint32_t tmp_len
        cdef int fwres
        cdef unsigned int pad_start, pad_len
        cdef unsigned int new_wseq, new_wval

        # executable code
        msg_len = len(data)
        tot = msg_len + 4
        padded_tot = (tot + 3) & (~0x3u)

        if msg_len:
            dptr = <const char*> data
        else:
            dptr = NULL

        cap = self.capacity
        if tot > cap:
            raise ValueError("message too big for pipe capacity")

        wptr = &self.hdr.writer_cursor
        rptr = &self.hdr.reader_cursor

        while True:
            r = __atomic_load_n(rptr, __ATOMIC_ACQUIRE)
            w = __atomic_load_n(wptr, __ATOMIC_ACQUIRE)

            # EARLY check: if reader closed, fail immediately (BrokenPipe)
            if is_closed(r):
                raise BrokenPipeError("reader closed")

            rseq = seq_of(r)
            wseq = seq_of(w)
            used = (wseq - rseq) & SEQ_MASK
            free = cap - used
            if free >= padded_tot:
                break
            # wait on reader cursor if still equal to observed r
            fwres = futex_wait_checked(rptr, r)
            continue

        start = wseq % cap  # guaranteed 4-byte aligned
        # write header as native uint32_t (fast path; header contiguous because of alignment)
        tmp_len = <uint32_t> msg_len
        (<uint32_t*> (self.buf + start))[0] = tmp_len

        # write payload (may wrap)
        payload_start = start + 4
        if payload_start >= cap:
            payload_start -= cap
        if msg_len:
            if payload_start + msg_len <= cap:
                memcpy(<void*> (self.buf + payload_start), <const void*> dptr, msg_len)
            else:
                first = cap - payload_start
                memcpy(<void*> (self.buf + payload_start), <const void*> dptr, first)
                memcpy(<void*> self.buf, <const void*> (dptr + first), msg_len - first)

        # # zero padding if needed (so content deterministic)
        # if padded_tot > tot:
        #    pad_start = start + tot
        #    if pad_start >= cap:
        #        pad_start -= cap
        #    pad_len = padded_tot - tot
        #    if pad_start + pad_len <= cap:
        #        memset(<void*> (self.buf + pad_start), 0, <size_t>pad_len)
        #    else:
        #        first = cap - pad_start
        #        memset(<void*> (self.buf + pad_start), 0, <size_t>first)
        #        memset(<void*> self.buf, 0, <size_t>(pad_len - first))

        # publish advance using padded_tot
        new_wseq = (wseq + padded_tot) & SEQ_MASK
        new_wval = pack_seq_closed(new_wseq, is_closed(w))
        __atomic_store_n(wptr, new_wval, __ATOMIC_RELEASE)
        
        # TODO: only call this when the buffer starts to look congested
        # will increase latency but enhance throughput
        if used > 512000: # experiment, want a bunch of data first
            futex_wake_checked(wptr, 1)
    
        return

    def read_line(self):
        """
        Read a single message (bytes) and advance reader cursor by padded_tot.
        Returns bytes payload or None on EOF.
        """
        # locals
        cdef unsigned int *wptr = &self.hdr.writer_cursor
        cdef unsigned int *rptr = &self.hdr.reader_cursor
        cdef unsigned int w, r, wseq, rseq, available, free
        cdef unsigned int start, msg_len, tot, padded_tot
        cdef unsigned int payload_start
        cdef unsigned int cap = self.capacity
        cdef unsigned int first
        cdef object payload
        cdef char *dst
        cdef uint32_t hdr32
        cdef int fwres
        cdef unsigned int r_old

        while True:
            w = __atomic_load_n(wptr, __ATOMIC_ACQUIRE)
            r = __atomic_load_n(rptr, __ATOMIC_ACQUIRE)
            wseq = seq_of(w)
            rseq = seq_of(r)
            available = (wseq - rseq) & SEQ_MASK
            if available >= 4:
                break
            if is_closed(w) and available == 0:
                return None
            fwres = futex_wait_checked(wptr, w)
            continue

        start = rseq % cap  # aligned to 4
        # fast header read
        hdr32 = (<uint32_t*> (self.buf + start))[0]
        msg_len = <unsigned int> hdr32
        tot = msg_len + 4
        # ensure full payload available
        while True:
            w = __atomic_load_n(wptr, __ATOMIC_ACQUIRE)
            wseq2 = seq_of(w)
            available2 = (wseq2 - rseq) & SEQ_MASK
            if available2 >= tot:
                break
            if is_closed(w):
                return None
            fwres = futex_wait_checked(wptr, w)
            continue

        # copy payload
        payload_start = start + 4
        if payload_start >= cap:
            payload_start -= cap
        if msg_len == 0:
            payload = PyBytes_FromStringAndSize(NULL, 0)
        else:
            if payload_start + msg_len <= cap:
                # contiguous fast path: let CPython allocate & copy in one call
                payload = PyBytes_FromStringAndSize(<char*>(self.buf + payload_start), msg_len)
            else:
                payload = PyBytes_FromStringAndSize(NULL, msg_len)
                dst = PyBytes_AS_STRING(payload)
                first = cap - payload_start
                memcpy(<void*> dst, <void*> (self.buf + payload_start), first)
                memcpy(<void*> (dst + first), <void*> self.buf, msg_len - first)

        # advance by padded total
        padded_tot = (tot + 3) & (~0x3u)
        r_old = r
        rseq_new = (rseq + padded_tot) & SEQ_MASK
        new_rval = pack_seq_closed(rseq_new, is_closed(r_old))
        __atomic_store_n(rptr, new_rval, __ATOMIC_RELEASE)

        # TODO: only do this if buffer was at more than half full
        free = self.capacity - available
        if free < (self.capacity >> 1):
            futex_wake_checked(rptr, 1)

        return payload

    def read_batch(self, int max_msgs=1024, int max_bytes=0, double min_release_fraction=0.5):
        """
        Read up to a batch of messages and return them as a Python list of bytes.

        Messages are copied into Python bytes. The reader cursor is published once
        at the end (advance by sum of padded_tot per message).
        """
        # cdef locals at top
        cdef unsigned int *wptr = &self.hdr.writer_cursor
        cdef unsigned int *rptr = &self.hdr.reader_cursor
        cdef unsigned int w, r, wseq, rseq, available
        cdef unsigned int start_len, msg_len, tot
        cdef unsigned int consumed = 0
        cdef unsigned int target = 0
        cdef unsigned int payload_start
        cdef unsigned int first
        cdef unsigned int msgs = 0
        cdef unsigned int cap = self.capacity
        cdef object batch_list = []
        cdef object payload
        cdef char *dst
        cdef int fwres
        cdef unsigned int payloads_bytes
        cdef uint32_t hdr32
        cdef unsigned int padded_tot

        # Wait until at least one header is available or EOF
        while True:
            w = __atomic_load_n(wptr, __ATOMIC_ACQUIRE)
            r = __atomic_load_n(rptr, __ATOMIC_ACQUIRE)
            wseq = seq_of(w)
            rseq = seq_of(r)
            available = (wseq - rseq) & SEQ_MASK

            if available < 4:
                if is_closed(w) and available == 0:
                    return batch_list
                fwres = futex_wait_checked(wptr, w)
                continue
            # compute batch target in bytes
            target = <unsigned int> (available * min_release_fraction)
            if target == 0:
                target = available if available <= cap else cap
            if max_bytes and max_bytes > 0 and <unsigned int> max_bytes < target:
                target = <unsigned int> max_bytes
            break

        # Read messages until consumed >= target or msgs >= max_msgs
        while consumed < target and msgs < max_msgs:
            if (available - consumed) < 4:
                break
            start_len = (rseq + consumed) % cap
            # header is contiguous due to alignment
            hdr32 = (<uint32_t*> (self.buf + start_len))[0]
            msg_len = <unsigned int> hdr32
            tot = msg_len + 4
            if (available - consumed) < tot:
                break

            # copy payload
            payload_start = start_len + 4
            if payload_start >= cap:
                payload_start -= cap
            if msg_len == 0:
                payload = PyBytes_FromStringAndSize(NULL, 0)
            else:
                if payload_start + msg_len <= cap:
                    # contiguous fast path: let CPython allocate & copy in one call
                    payload = PyBytes_FromStringAndSize(<char*>(self.buf + payload_start), msg_len)
                else:
                    # wrapped: need two copies into a fresh bytes object
                    payload = PyBytes_FromStringAndSize(NULL, msg_len)
                    dst = PyBytes_AS_STRING(payload)
                    first = cap - payload_start
                    memcpy(<void*> dst, <void*> (self.buf + payload_start), first)
                    memcpy(<void*> (dst + first), <void*> self.buf, msg_len - first)

            batch_list.append(payload)

            # advance consumed by padded size (so next header remains aligned)
            padded_tot = (tot + 3) & (~0x3u)
            consumed += padded_tot
            msgs += 1

            if max_bytes and max_bytes > 0:
                payloads_bytes = consumed - (4 * msgs)
                if payloads_bytes >= <unsigned int> max_bytes:
                    break

        # publish reader cursor once if we consumed anything
        if consumed:
            new_rseq = (rseq + consumed) & SEQ_MASK
            new_rval = pack_seq_closed(new_rseq, is_closed(r))
            __atomic_store_n(rptr, new_rval, __ATOMIC_RELEASE)
            futex_wake_checked(rptr, 1)

        return batch_list

    def read_lines_batched_iter(self, int max_msgs=1024, int max_bytes=0, double min_release_fraction=0.5):
        """
        Python generator that repeatedly yields messages by calling read_batch().
        Stops when read_batch() returns an empty list and writer is closed.
        """
        cdef unsigned int w, r
        cdef unsigned int *wptr = &self.hdr.writer_cursor
        cdef unsigned int *rptr = &self.hdr.reader_cursor

        while True:
            batch = self.read_batch(max_msgs=max_msgs, max_bytes=max_bytes, min_release_fraction=min_release_fraction)
            if not batch:
                w = __atomic_load_n(wptr, __ATOMIC_ACQUIRE)
                r = __atomic_load_n(rptr, __ATOMIC_ACQUIRE)
                if is_closed(w) and seq_of(w) == seq_of(r):
                    return
                continue
            for msg in batch:
                yield msg