#!/usr/bin/env python3
# bench_compare_instrumented.py
# Improved benchmark that reports per-process wall/CPU times and decides which side is the bottleneck.

import argparse
import os
import sys
import time
import struct
from multiprocessing import Process, Pipe

try:
    import psutil
except Exception:
    psutil = None

def human(x):
    # bytes -> human MB
    return f"{x/1024/1024:.2f} MB"

# ---------- Producer / Consumer implementations ----------------------

def producer_pipe(count, msg_size, conn):
    """
    Producer using plain pipe (writes header+payload)
    Reports wall_time_ns, cpu_time_s, msgs, bytes
    """
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        # child => consumer
        os.close(w)
        fd = r
        total_bytes = 0
        read = 0
        start_wall = time.perf_counter_ns()
        start_cpu = time.process_time()
        while read < count:
            hdr = os.read(fd, 4)
            while len(hdr) < 4:
                more = os.read(fd, 4 - len(hdr))
                if not more:
                    break
                hdr += more
            if len(hdr) < 4:
                break
            (msg_len,) = struct.unpack(">I", hdr)
            payload = os.read(fd, msg_len)
            while len(payload) < msg_len:
                more = os.read(fd, msg_len - len(payload))
                if not more:
                    break
                payload += more
            total_bytes += msg_len
            read += 1
        end_wall = time.perf_counter_ns()
        end_cpu = time.process_time()
        os._exit(0)
    else:
        # parent => writer (we are the producer here)
        os.close(r)
        fd = w
        payload = b'a' * msg_size
        start_wall = time.perf_counter_ns()
        start_cpu = time.process_time()
        for i in range(count):
            hdr = struct.pack(">I", msg_size)
            os.write(fd, hdr)
            os.write(fd, payload)
        os.close(fd)
        end_wall = time.perf_counter_ns()
        end_cpu = time.process_time()
        _, status = os.waitpid(pid, 0)
        # Report producer stats (writer)
        conn.send(("ok",
                  {"role":"producer",
                   "wall_ns": end_wall - start_wall,
                   "cpu_s": end_cpu - start_cpu,
                   "msgs": count,
                   "bytes": count * msg_size}))
        conn.close()

def producer_pipebuf(count, msg_size, name, bufsize, conn, batched=False):
    """
    Parent process acts as producer (writes then closes).
    Child is spawned as consumer inside this function.
    We report producer's own timing via conn; consumer reports through its own conn to parent child pipe.
    """
    try:
        import pipebuf_futex as pbmod
    except Exception as e:
        conn.send(("error", f"pipebuf import failed: {e}")); conn.close(); return

    # Create mapping in parent so both sides can open
    pb = pbmod.PipeBufFutex.create(name, bufsize)

    # set up a Pipe for child->parent reporting
    parent_conn, child_conn = Pipe()
    pid = os.fork()
    if pid == 0:
        # child => consumer
        try:
            pb2 = pbmod.PipeBufFutex.open(name)
        except Exception as e:
            child_conn.send(("error", f"consumer open failed: {e}"))
            child_conn.close()
            os._exit(2)

        # consumer measures
        start_wall = time.perf_counter_ns()
        start_cpu = time.process_time()
        total_bytes = 0
        read = 0
        if batched:
            for b in pb2.read_lines_batched_iter():
                if b is None:
                    break
                total_bytes += len(b)
                read += 1
                if read >= count and False:
                    pass
        else:
            while True:
                b = pb2.read_line()
                if b is None:
                    break
                total_bytes += len(b)
                read += 1
                if read >= count and False:
                    pass
        end_wall = time.perf_counter_ns()
        end_cpu = time.process_time()
        child_conn.send(("ok", {"role":"consumer",
                                "wall_ns": end_wall - start_wall,
                                "cpu_s": end_cpu - start_cpu,
                                "msgs": read,
                                "bytes": total_bytes}))
        child_conn.close()
        pb2.close()
        os._exit(0)
    else:
        # parent is producer: run writes
        time.sleep(0.01)  # give child time to open
        payload = b'a' * msg_size
        start_wall = time.perf_counter_ns()
        start_cpu = time.process_time()
        for i in range(count):
            pb.write_line(payload)
        pb.close_for_write()
        end_wall = time.perf_counter_ns()
        end_cpu = time.process_time()
        # wait for child report
        ok = parent_conn.poll(30.0)
        consumer_report = None
        if ok:
            consumer_report = parent_conn.recv()
        _, status = os.waitpid(pid, 0)
        # send producer stats back to caller
        conn.send(("ok", {"role":"producer",
                          "wall_ns": end_wall - start_wall,
                          "cpu_s": end_cpu - start_cpu,
                          "msgs": count,
                          "bytes": count * msg_size},
                          consumer_report))
        conn.close()
        pb.close()
        try:
            pb.unlink()
        except Exception:
            pass

# ---------- High-level harness & analysis -----------------------------

def analyze_and_print(results):
    """
    results: list of tuples from child pipes. Each tuple like ("ok", stats) or ("ok", producer_stats, consumer_report)
    For pipebuf we get a triple from producer call; for pipe we get only producer-side.
    We'll normalize to producer_stats and consumer_stats.
    """
    producer = None
    consumer = None

    # Unpack results
    for r in results:
        if r is None:
            continue
        if r[0] != "ok":
            print("ERROR:", r)
            return
        data = r[1]
        if isinstance(data, dict) and data.get("role") == "producer":
            producer = data
            # pipebuf sends consumer nested in r[2]
            if len(r) > 2 and r[2]:
                cres = r[2]
                if cres[0] == "ok":
                    consumer = cres[1]
        elif isinstance(data, dict) and data.get("role") == "consumer":
            consumer = data

    if producer is None and consumer is None:
        print("No valid reports")
        return

    # If one side missing (e.g., pipe mode where parent only reports), estimate using rusage is omitted.
    print("\n--- Results ---")
    if producer:
        pw = producer["wall_ns"] / 1e9
        pc = producer["cpu_s"]
        pm = producer["msgs"]
        pb = producer["bytes"]
        p_msgps = pm / pw if pw > 0 else float("inf")
        p_mb_s = (pb / (1024*1024)) / pw if pw > 0 else float("inf")
        print(f"Producer: msgs={pm}, bytes={pb}, wall={pw:.6f}s, cpu={pc:.6f}s, {p_msgps:.0f} msg/s, {p_mb_s:.2f} MB/s, cpu_util={(pc/pw*100) if pw>0 else 0:.1f}%")
    if consumer:
        cw = consumer["wall_ns"] / 1e9
        cc = consumer["cpu_s"]
        cm = consumer["msgs"]
        cb = consumer["bytes"]
        c_msgps = cm / cw if cw > 0 else float("inf")
        c_mb_s = (cb / (1024*1024)) / cw if cw > 0 else float("inf")
        print(f"Consumer: msgs={cm}, bytes={cb}, wall={cw:.6f}s, cpu={cc:.6f}s, {c_msgps:.0f} msg/s, {c_mb_s:.2f} MB/s, cpu_util={(cc/cw*100) if cw>0 else 0:.1f}%")

    # Decide bottleneck using throughputs
    if producer and consumer:
        if p_msgps < c_msgps * 0.98:
            print("\nBottleneck: PRODUCER (writer) — producer msg/s < consumer msg/s")
        elif c_msgps < p_msgps * 0.98:
            print("\nBottleneck: CONSUMER (reader) — consumer msg/s < producer msg/s")
        else:
            print("\nBalanced: producer and consumer throughputs are similar")
        # Look at cpu util to see if CPU bound
        if producer:
            if (producer["cpu_s"] / (producer["wall_ns"]/1e9)) > 0.9:
                print("Producer appears CPU-bound (high CPU utilization).")
        if consumer:
            if (consumer["cpu_s"] / (consumer["wall_ns"]/1e9)) > 0.9:
                print("Consumer appears CPU-bound (high CPU utilization).")
    else:
        print("\nOnly one side reported; cannot reliably decide bottleneck, but see timings above.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=['pipe', 'pipebuf'], default='pipebuf')
    ap.add_argument("--count", type=int, default=200000)
    ap.add_argument("--msg-size", type=int, default=128)
    ap.add_argument("--name", default="bench1")
    ap.add_argument("--bufsize", type=int, default=4*1024*1024)
    ap.add_argument("--batched", action="store_true")
    args = ap.parse_args()

    print("bench config:", args)

    parent_conn, child_conn = Pipe()
    p = None
    if args.backend == 'pipe':
        # For pipe path, we only measure producer here; consumer is forked inside producer_pipe
        p = Process(target=producer_pipe, args=(args.count, args.msg_size, child_conn))
        p.start()
        # wait for producer report
        ok = parent_conn.poll(60.0)
        res = None
        if ok:
            res = parent_conn.recv()
        p.join(timeout=1.0)
        analyze_and_print([res])
    else:
        # pipebuf path: parent calls producer_pipebuf which forks consumer; it reports both
        p = Process(target=producer_pipebuf, args=(args.count, args.msg_size, args.name, args.bufsize, child_conn, args.batched))
        p.start()
        ok = parent_conn.poll(60.0)
        res = None
        if ok:
            res = parent_conn.recv()
        p.join(timeout=1.0)
        analyze_and_print([res])

if __name__ == "__main__":
    main()