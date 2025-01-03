#cython: boundscheck=False, wraparound=False, initializedcheck=False, overflowcheck=False, cdivision=True
###cython: boundscheck=True, wraparound=True, initializedcheck=True, overflowcheck=True, cdivision=False
#!python

#from types cimport *
from types import *
from libc cimport stdlib, stdio
from libc.string cimport strstr

def distribute(str fin_name, list fifo_names, int chunk_size=10000, 
               size_t in_buf_size=2**20, size_t out_buf_size=2**19, 
               header_detect_func=None, header_fifo="", 
               header_broadcast=False, size_t _buffer_size=0, **kw):

    if header_fifo == 0:
        # special mode: use the first fifo name for header data and the other
        # as round-robin outputs as usual
        header_fifo = fifo_names[0]
        fifo_names = fifo_names[1:]

    if _buffer_size > 0:
        in_buf_size = _buffer_size
        out_buf_size = _buffer_size

    cdef size_t i = 0
    cdef size_t j = 0
    cdef size_t n_outs = len(fifo_names)
    cdef size_t n = 0
    cdef str line
    cdef char* buffer
    cdef ssize_t n_read = 0
    # support input distribution to up to 128 fifos in parallel
    cdef char* fifo_buffers[128]
    buffer = <char*>stdlib.malloc(in_buf_size)

    assert n <= 128
    cdef stdio.FILE *fifos[128]

    cdef char* stdin_buf = <char*>stdlib.malloc(in_buf_size)
    cdef stdio.FILE *fin = stdio.fopen(fin_name.encode('utf-8'), 'r')
    cdef stdio.FILE *fheader
    stdio.setvbuf(fin, stdin_buf, stdio._IOFBF, in_buf_size)

    # set up output buffers for the main fifos
    for i in range(n_outs):
        fifo_buffers[i] = <char*>stdlib.malloc(out_buf_size)
        fifos[i] = stdio.fopen(fifo_names[i].encode('utf-8'), 'w')
        stdio.setvbuf(fifos[i], fifo_buffers[i], stdio._IOFBF, out_buf_size)

    if header_detect_func:
        # if the input stream contains some special header lines, allow to detect 
        # these and write them to a separate header-fifo
        if header_fifo:
            fheader = stdio.fopen(header_fifo.encode('utf-8'), 'w')

        while(True):
            n_read = stdio.getline(&buffer, &in_buf_size, fin) 
            if n_read <= 0:
                break
            
            if header_detect_func(bytes(buffer).decode("ascii")):
                if header_fifo:
                    # print("writing to fifo header")
                    stdio.fwrite(buffer, n_read, 1, fheader)
                if header_broadcast:
                    for i in range(n_outs):
                        # print(f"writing header line to fifo {i} (n_read={n_read}) line={str(buffer)}")
                        stdio.fwrite(buffer, n_read, 1, fifos[i])
            else:
                # as soon as we see a non-header line, we break and enter the main loop
                break

        if header_fifo:
            # close the header fifo
            stdio.fclose(fheader)

    # main distribute loop
    while(True):
        if n_read == 0:
            # we may have already read a line in the header-detection above!
            # read a new-line only if n_read has explicitly been zeroed again
            n_read = stdio.getline(&buffer, &in_buf_size, fin) 

        if n_read <= 0:
            break

        j = n // chunk_size
        stdio.fwrite(buffer, n_read, 1, fifos[j % n_outs])
        n += 1
        n_read = 0

    # close down all main fifos and free the output buffers
    for i in range(n_outs):
        stdio.fclose(fifos[i])
        stdlib.free(fifo_buffers[i])

    # close the input and free the input buffer, as well as the line buffer
    stdio.fclose(fin)
    stdlib.free(stdin_buf)
    stdlib.free(buffer)

    return n # number of lines distributed (excluding header)


def distribute_by_substr(str fin_name, list fifo_names, dict sub_lookup,
                         size_t sub_size, bytes sub_lead=b"\tCB:Z:", 
                         size_t in_buf_size=2**20, size_t out_buf_size=2**19, 
                         header_detect_func=None, header_fifo="",
                         header_broadcast=False, _buffer_size=0, **kw):

    if header_fifo == 0:
        # special mode: use the first fifo name for header data and the other
        # as round-robin outputs as usual
        header_fifo = fifo_names[0]
        fifo_names = fifo_names[1:]

    if _buffer_size > 0:
        in_buf_size = _buffer_size
        out_buf_size = _buffer_size

    cdef size_t i = 0
    cdef ssize_t j = 0

    cdef size_t n_outs = len(fifo_names)
    cdef size_t n = 0
    cdef str line
    cdef char* buffer
    cdef char* sub_lead_c = sub_lead
    cdef size_t lead_n = len(sub_lead)
    cdef char* sub_buffer = <char*>stdlib.malloc(sub_size)
    cdef char* sub_match_c = NULL
    cdef bytes sub
    cdef ssize_t n_read = 0
    # support input distribution to up to 128 fifos in parallel
    cdef char* fifo_buffers[128]
    buffer = <char*>stdlib.malloc(in_buf_size)

    assert n_outs <= 128
    cdef stdio.FILE *fifos[128]

    cdef char* stdin_buf = <char*>stdlib.malloc(in_buf_size)
    cdef stdio.FILE *fin = stdio.fopen(fin_name.encode('utf-8'), 'r')
    cdef stdio.FILE *fheader
    stdio.setvbuf(fin, stdin_buf, stdio._IOFBF, in_buf_size)

    # set up output buffers for the main fifos
    for i in range(n_outs):
        fifo_buffers[i] = <char*>stdlib.malloc(out_buf_size)
        fifos[i] = stdio.fopen(fifo_names[i].encode('utf-8'), 'w')
        stdio.setvbuf(fifos[i], fifo_buffers[i], stdio._IOFBF, out_buf_size)

    if header_detect_func:
        # if the input stream contains some special header lines, allow to detect these and write them to a separate header-fifo
        if header_fifo:
            fheader = stdio.fopen(header_fifo.encode('utf-8'), 'w')

        while(True):
            n_read = stdio.getline(&buffer, &in_buf_size, fin) 
            if n_read <= 0:
                break
            
            if header_detect_func(bytes(buffer).decode("ascii")):
                if header_fifo:
                    stdio.fwrite(buffer, n_read, 1, fheader)
                if header_broadcast:
                    for i in range(n_outs):
                        # print(f"writing header line to fifo {i} (n_read={n_read}) line={str(buffer)}")
                        stdio.fwrite(buffer, n_read, 1, fifos[i])
            else:
                # as soon as we see a non-header line, we break and enter the main loop
                break

        if header_fifo:
            # close the header fifo
            stdio.fclose(fheader)

    # main distribute loop
    n = 0
    while(True):
        if n_read == 0:
            # we may have already read a line in the header-detection above!
            # read a new-line only if n_read has explicitly been zeroed again
            n_read = stdio.getline(&buffer, &in_buf_size, fin) 

        if n_read <= 0:
            break

        # extract the \tCB:Z:<cell barcode> value.
        # for more flexibility, we are going to make '\tCB:Z:' variable, 
        # and extract sub_n characters downstream of that

        sub_match_c = strstr(buffer, sub_lead_c)
        if sub_match_c is NULL:
            raise ValueError(f"can not find {sub_lead} as substring in {str(buffer)}")

        sub = (sub_match_c + lead_n)[:sub_size]
        # print(f"found sub {sub}")

        j = sub_lookup.get(sub, -1)
        if j >= 0:
            stdio.fwrite(buffer, n_read, 1, fifos[j % n_outs])
        else:
            print(f"unknown sub: {sub}")

        n += 1
        n_read = 0

    # close down all main fifos and free the output buffers
    for i in range(n_outs):
        stdio.fclose(fifos[i])
        stdlib.free(fifo_buffers[i])

    # close the input and free the input buffer, as well as the line buffer
    stdio.fclose(fin)
    stdlib.free(stdin_buf)
    stdlib.free(buffer)

    return n # number of lines distributed (excluding header)


def collect(list fifo_names, str fout_name, int chunk_size=10000, 
            size_t in_buf_size=2**19, size_t out_buf_size=2**20,
            header_fifo="", _buffer_size=0, custom_header=None, 
            int n_reopen_inputs=1, 
            str log_name="mrfifo.collect",
            size_t log_rate_every_n=0,
            str log_rate_template="processed {n_out} records ({kps:.1f} / second)", **kw):

    import logging
    logger = logging.getLogger(log_name)
    from time import time

    if header_fifo == 0:
        # special mode: use the first fifo name for header data 
        # and the other as round-robin inputs as usual
        header_fifo = fifo_names[0]
        fifo_names = fifo_names[1:]

    if _buffer_size > 0:
        in_buf_size = _buffer_size
        out_buf_size = _buffer_size

    cdef int i = 0
    cdef int j = 0
    cdef int k = 0
    cdef size_t n_ins = len(fifo_names)
    cdef size_t n = 0
    cdef size_t n_out = 0
    cdef str line
    cdef char* buffer
    cdef ssize_t n_read
    
    # support collecting and demuxing from up to 128 fifos
    cdef char* fifo_buffers[128]

    import sys
    # sys.stderr.write(f"collecting from {fifo_names} into {fout_name}")
    assert n <= 128
    cdef stdio.FILE *fifos[128]
    cdef bint[128] fifo_closed
    
    cdef size_t drained_fifos = 0
    cdef int n_loop = 0
    
    # line buffer
    buffer = <char*>stdlib.malloc(out_buf_size)
    # prepare output file and buffer
    cdef char* out_buf = <char*>stdlib.malloc(out_buf_size)
    cdef stdio.FILE *fout = stdio.fopen(fout_name.encode('utf-8'), 'w')
    stdio.setvbuf(fout, out_buf, stdio._IOFBF, out_buf_size)

    # cdef char * line_bytes
    if custom_header is not None:
        # we have a custom header string, write this first!
        header_bytes = custom_header.encode('utf-8')
        stdio.fwrite(<const char*>header_bytes, len(header_bytes), 1, fout)

    cdef stdio.FILE *fheader
    if header_fifo:
        # we have some special, un-multiplexed header data. Read this 
        # stuff first and write it to the output
        fheader = stdio.fopen(header_fifo.encode('utf-8'), 'r')
        while(True):
            n_read = stdio.getline(&buffer, &out_buf_size, fheader) 
            if n_read <= 0:
                break
            
            stdio.fwrite(buffer, n_read, 1, fout)
        
        # all header data has been read! close this fifo's reading end
        stdio.fclose(fheader)
        logger.debug("completely written header")

    T0 = time()
    t0 = T0
    logger.debug("entering collect() main loop")
    for n_loop in range(n_reopen_inputs):
        #print(f"opening the input fifos for the {n_loop}th time: {fifo_names}")
        # now prepare input from all the demuxed fifos
        for i in range(n_ins):
            fifo_buffers[i] = <char*>stdlib.malloc(in_buf_size)
            fifos[i] = stdio.fopen(fifo_names[i].encode('utf-8'), 'r')
            stdio.setvbuf(fifos[i], fifo_buffers[i], stdio._IOFBF, in_buf_size)

        drained_fifos = 0
        for i in range(n_ins):
            fifo_closed[i] = False

        # main mutiplexing loop
        while(drained_fifos < n_ins):
            j = n // chunk_size
            k = j % n_ins # index of fifo
            if not fifo_closed[k]:
                # print(f"collection wants to read from {fifo_names[k]}. blocking")
                n_read = stdio.getline(&buffer, &out_buf_size, fifos[k]) 
                if n_read <= 0:
                    fifo_closed[k] = True
                    drained_fifos += 1
                else:
                    # print(f"fifo {k} ({fifo_names[k]}). {n_read} bytes. writing to {fout_name}")
                    stdio.fwrite(buffer, n_read, 1, fout)
                    n_out += 1
            n += 1
            if (log_rate_every_n > 0) and (n % log_rate_every_n == 0):
                t1 = time()
                dt = t1 - t0
                dT = t1 - T0
                t0 = t1
                kps = 0.001 * log_rate_every_n / dt
                KPS = 0.001 * n_out / dT
                logger.info(log_rate_template.format(
                    n_rate=log_rate_every_n,
                    n_out=n_out,
                    M_out=1e-6*n_out,
                    kps=kps,
                    KPS=KPS,
                    mps=0.001 * kps,
                    MPS=0.001 * KPS,
                    dt=dt,
                    dT=dT)
                )

        for i in range(n_ins):
            stdio.fclose(fifos[i])
            stdlib.free(fifo_buffers[i])

    # free the line buffer, close the output, and free the output buffer
    stdlib.free(buffer)
    stdio.fclose(fout)
    stdlib.free(out_buf)

    return n_out # total number of multiplexed lines (excluding header)
