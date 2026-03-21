# tests/test_pipebuf_futex.py
import os
import time
import uuid
import pytest
from multiprocessing import Process, Pipe

try:
    import pipebuf_futex as pbmod
except Exception:
    pbmod = None

pytestmark = pytest.mark.skipif(pbmod is None, reason="pipebuf_futex extension not available")

def unique_name(prefix="t"):
    return f"{prefix}-{uuid.uuid4().hex}"

# Child helpers -------------------------------------------------------------
def child_reader_close_then_ack(name, conn):
    try:
        pb = pbmod.PipeBufFutex.open(name)
    except Exception as e:
        conn.send(("error", f"open failed: {e}")); conn.close(); return
    try:
        pb.close_for_read()
        conn.send(("ok", 0))
    except Exception as e:
        conn.send(("error", repr(e)))
    finally:
        try: pb.close()
        except: pass
        conn.close()

def child_producer_open_write_and_close(name, count, size, conn):
    """Producer that opens an existing mapping (created by parent), writes and closes."""
    try:
        pb = pbmod.PipeBufFutex.open(name)
    except Exception as e:
        conn.send(("error", f"open failed: {e}")); conn.close(); return
    try:
        payload = b"x" * size
        for _ in range(count):
            pb.write_line(payload)
        pb.close_for_write()
        conn.send(("ok", count))
    except Exception as e:
        conn.send(("error", repr(e)))
    finally:
        try: pb.close()
        except: pass
        conn.close()

def child_consumer_collect(name, conn, collect_msgs=False):
    try:
        pb = pbmod.PipeBufFutex.open(name)
    except Exception as e:
        conn.send(("error", f"open failed: {e}")); conn.close(); return
    msgs = []
    total = 0
    try:
        while True:
            b = pb.read_line()
            if b is None:
                break
            total += len(b)
            msgs.append(b)
        if collect_msgs:
            conn.send(("ok", len(msgs), total, msgs))
        else:
            conn.send(("ok", len(msgs), total))
    except Exception as e:
        conn.send(("error", repr(e)))
    finally:
        try: pb.close()
        except: pass
        conn.close()

def child_consumer_block_once(name, conn):
    try:
        pb = pbmod.PipeBufFutex.open(name)
    except Exception as e:
        conn.send(("error", f"open failed: {e}")); conn.close(); return
    try:
        b = pb.read_line()  # expected to block
        if b is None:
            conn.send(("ok-eof", 0))
        else:
            conn.send(("ok-data", len(b)))
    except Exception as e:
        conn.send(("error", repr(e)))
    finally:
        try: pb.close()
        except: pass
        conn.close()

# Tests ---------------------------------------------------------------------
def test_reader_closes_before_write():
    name = unique_name("rc")
    # Parent creates mapping so children can open safely
    pb = pbmod.PipeBufFutex.create(name, 1024*1024)
    time.sleep(0.01)
    parent_conn, child_conn = Pipe()
    p = Process(target=child_reader_close_then_ack, args=(name, child_conn))
    p.start()
    try:
        assert parent_conn.poll(2.0), "reader child did not reply"
        res = parent_conn.recv()
        assert res[0] == "ok"
        # Now attempt to open and write (writer in parent should see BrokenPipeError)
        with pytest.raises(BrokenPipeError):
            pb.write_line(b"hello")
    finally:
        try: pb.close(); pb.unlink()
        except: pass
        p.join(timeout=1.0)
        if p.is_alive():
            p.terminate(); p.join()

def test_writer_closes_and_reader_reads_all():
    name = unique_name("wc")
    count = 2000
    size = 128
    # Parent creates mapping for both children to open
    pb_parent = pbmod.PipeBufFutex.create(name, 4*1024*1024)
    time.sleep(0.01)
    prod_parent, prod_child = Pipe()
    cons_parent, cons_child = Pipe()
    p_prod = Process(target=child_producer_open_write_and_close, args=(name, count, size, prod_child))
    p_cons = Process(target=child_consumer_collect, args=(name, cons_child, False))
    p_prod.start()
    p_cons.start()
    try:
        assert prod_parent.poll(30.0), "producer child did not report"
        pres = prod_parent.recv()
        assert pres[0] == "ok", f"producer error: {pres}"
        assert cons_parent.poll(30.0), "consumer child did not report"
        cres = cons_parent.recv()
        assert cres[0] == "ok", f"consumer error: {cres}"
        read_count, total_bytes = cres[1], cres[2]
        assert read_count == count
        assert total_bytes == count * size
    finally:
        p_prod.join(timeout=1.0)
        p_cons.join(timeout=1.0)
        try:
            pb_parent.close()
            pb_parent.unlink()
        except Exception:
            pass

def test_reader_blocks_if_writer_never_writes():
    name = unique_name("rb")
    pb = pbmod.PipeBufFutex.create(name, 1024*1024)
    time.sleep(0.01)
    parent_conn, child_conn = Pipe()
    p = Process(target=child_consumer_block_once, args=(name, child_conn))
    p.start()
    try:
        time.sleep(0.2)
        assert p.is_alive(), "consumer returned early; expected it to block"
    finally:
        p.terminate()
        p.join(timeout=1.0)
        try: pb.close(); pb.unlink()
        except: pass

def test_wraparound_small_buffer_content_verification():
    name = unique_name("wrap")
    bufsize = 256
    count = 300
    pattern = [10,20,7,15,12]
    pb = pbmod.PipeBufFutex.create(name, bufsize)
    time.sleep(0.02)
    parent_conn, child_conn = Pipe()
    cproc = Process(target=child_consumer_collect, args=(name, child_conn, True))
    cproc.start()
    try:
        expected = []
        for i in range(count):
            s = (f"m{i}-" + ("X" * pattern[i % len(pattern)])).encode()
            expected.append(s)
            pb.write_line(s)
        pb.close_for_write()
        assert parent_conn.poll(30.0)
        cres = parent_conn.recv()
        assert cres[0] == "ok"
        read_count, total_bytes, msgs = cres[1], cres[2], cres[3]
        assert read_count == count
        assert msgs == expected
    finally:
        cproc.join(timeout=1.0)
        try: pb.close(); pb.unlink()
        except: pass