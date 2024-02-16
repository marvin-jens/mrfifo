import multiprocessing as mp
# weirdly needed to avoid tests hanging if run as an entire suite from pytest
# see: https://github.com/pytest-dev/pytest/issues/11174
# mp.set_start_method("spawn", force=True)
# however, it can not pickle my decorated functions.
# luckily, if we close all open file descriptors in child processes, the default method also works and
# there are no hanging tests anymore

import logging
import pytest
import mrfifo as mf

def simple_counter(input):
    i = 0
    for line in input:
        i += 1
    
    return i

def pass_through(input, output):
    i = 0
    for line in input:
        i += 1
        output.write(line)
    
    return i

def add_lines(inputs, output):
    i = 0
    for input in inputs:
        i += pass_through(input, output)
    
    return i

def test_plumbing():
    with mf.plumbing.create_named_pipes(['fq_in', 'w0', 'w1', 'w2', 'w3', 'bam_out']) as pipes:
        from pprint import pprint
        pprint(pipes)

def test_input_output():
    w = (
        mf.Workflow('input_output')
        .gz_reader(inputs=["test_data/simple.txt.gz"])
        # gz_reader, by default, writes to 'input_text'
        .funnel(
            func=pass_through,
            input=mf.FIFO("input_text", "rt"),
            output=open("/dev/stdout", "w"),
        )
        .run()
    )
    print(w.result_dict)
    assert w.result_dict['input_output.funnel0'] == 17

def test_dist():
    w = (
        mf.Workflow('simple_line_counter')
        .gz_reader(inputs=["test_data/simple.txt.gz"])
        # gz_reader, by default, writes to 'input_text'
        .distribute(
            input=mf.FIFO("input_text", 'rt'),
            outputs=mf.FIFO("dist{n}", "wt", n=4),
            chunk_size=1,
        )
        .workers(
            func=simple_counter,
            input=mf.FIFO("dist{n}", "rt"),
            n=4,
        )
        .run()
    )
    assert w.result_dict['simple_line_counter.worker0'] == 5
    assert w.result_dict['simple_line_counter.worker1'] == 4
    assert w.result_dict['simple_line_counter.worker2'] == 4
    assert w.result_dict['simple_line_counter.worker3'] == 4

           
def test_dist_work_collect():
    w = (
        mf.Workflow('simple_line_counter')
        .gz_reader(inputs=["test_data/simple.txt.gz"])
        # gz_reader, by default, writes to 'input_text'
        .distribute(
            input=mf.FIFO("input_text", 'rt'),
            outputs=mf.FIFO("dist{n}", "wt", n=4),
            chunk_size=1,
        )
        .workers(
            func=pass_through,
            input=mf.FIFO("dist{n}", "rt"),
            output=mf.FIFO("out{n}", "wt"),
            n=4,
        )
        .collect(
            inputs=mf.FIFO("out{n}", "rt", n=4),
            output="/dev/stdout",
            chunk_size=1,
        )
        .run()
    )
    assert w.result_dict['simple_line_counter.collect0'] == 17


def test_dist_work_funnel():
    w = (
        mf.Workflow('simple_line_counter')
        .gz_reader(inputs=["test_data/simple.txt.gz"])
        # gz_reader, by default, writes to 'input_text'
        .distribute(
            input=mf.FIFO("input_text", 'rt'),
            outputs=mf.FIFO("dist{n}", "wt", n=4),
            chunk_size=1,
        )
        .workers(
            func=pass_through,
            input=mf.FIFO("dist{n}", "rt"),
            output=mf.FIFO("out{n}", "wt"),
            n=4,
        )
        .funnel(
            inputs=mf.FIFO("out{n}", "rt", n=4),
            output=open("/dev/stdout", "wt"),
            func=add_lines,
        )
        .run()
    )
    assert w.result_dict['simple_line_counter.funnel0'] == 17


def test_dist_work_collect_funnel():
    w = (
        mf.Workflow('simple_line_counter')
        .gz_reader(inputs=["test_data/simple.txt.gz"])
        # gz_reader, by default, writes to 'input_text'
        .distribute(
            input=mf.FIFO("input_text", 'rt'),
            outputs=mf.FIFO("dist{n}", "wt", n=4),
            chunk_size=1,
        )
        .workers(
            func=pass_through,
            input=mf.FIFO("dist{n}", "rt"),
            output=mf.FIFO("out{n}", "wt"),
            n=4,
        )
        .collect(
            inputs=mf.FIFO("out{n}", "rt", n=4),
            output=mf.FIFO("combined", "wt"),
            chunk_size=1,
        )
        .funnel(
            input=mf.FIFO("combined", "rt"),
            func=simple_counter,
        )
        .run()
    )
    assert w.result_dict['simple_line_counter.funnel0'] == 17



def header_checker(input):
    header = []
    n_bam_records = 0
    for line in input:
        if line.startswith('@'):
            header.append(line)
        else:
            n_bam_records += 1
    
    for x in header:
        print(x.rstrip())

    print(f"received {len(header)} header records and {n_bam_records} BAM records")
    return len(header), n_bam_records


def is_header(line):
    return line.startswith('@')

def test_header_broadcast():
    w = (
        mf.Workflow('BAM_header_broadcast')
        .BAM_reader(input="test_data/tiny_test.bam")
        .distribute(
            input=mf.FIFO("input_sam", "rt"),
            outputs=mf.FIFO("dist{n}", "wt", n=4),
            chunk_size=1,
            header_detect_func=is_header,
            header_broadcast=True,
        )
        .workers(
            input=mf.FIFO("dist{n}", "rt"),
            func=header_checker,
            n=4,
        )
    )
    w.run()
    print(w.result_dict)
    assert w.result_dict['BAM_header_broadcast.worker0'] == (5, 9)
    assert w.result_dict['BAM_header_broadcast.worker1'] == (5, 9)
    assert w.result_dict['BAM_header_broadcast.worker2'] == (5, 8)
    assert w.result_dict['BAM_header_broadcast.worker3'] == (5, 8)

def test_header_fifo():
    w = (
        mf.Workflow('BAM_header_fifo')
        .BAM_reader(input="test_data/tiny_test.bam")
        .distribute(
            input=mf.FIFO("input_sam", "rt"),
            outputs=mf.FIFO("dist{n}", "wt", n=4),
            chunk_size=1,
            header_detect_func=is_header,
            header_broadcast=False,
            header_fifo=mf.FIFO("header", "wt"),
        )
        .workers(
            input=mf.FIFO("dist{n}", "rt"),
            func=header_checker,
            n=4,
        )
        .funnel(
            input=mf.FIFO("header", "rt"),
            func=header_checker,
        )
    )
    w.run()
    print(w.result_dict)
    assert w.result_dict['BAM_header_fifo.worker0'] == (0, 9)
    assert w.result_dict['BAM_header_fifo.worker1'] == (0, 9)
    assert w.result_dict['BAM_header_fifo.worker2'] == (0, 8)
    assert w.result_dict['BAM_header_fifo.worker3'] == (0, 8)
    assert w.result_dict['BAM_header_fifo.funnel0'] == (5, 0)
 

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    # test_plumbing()
    # test_input_output()
    # test_dist()
    # test_dist_work_collect()
    # test_dist_work_funnel()
    # test_dist_work_funnel_count()
    # test_dist_work_collect_funnel()
    # test_header_broadcast()
    # test_header_fifo()
