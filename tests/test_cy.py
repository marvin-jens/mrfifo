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

def fancy_counter(input):
    counts = mf.util.CountDict()
    for line in mf.util.timed_loop(input, logging.getLogger("fancy_counter"), chunk_size=1, T=0):
        counts.count(f"obs_{line.strip()}")
    
    return counts


def pass_through(input, output, raise_exception=False, _logger=None, _job_name="job", **kwargs):
    i = 0
    for line in input:
        i += 1
        output.write(line)
        if raise_exception:
            if _logger is not None:
                _logger.warning("about to raise exception!")
            raise ValueError(f"{_job_name} was asked to raise an exception for testing purposes. Here we are...")
    
    return i

def turn_to_SAM(input, output, **kwargs):
    sam_line = "{qname}\t4\t*\t0\t0\t*\t*\t0\t0\t{seq}\t{qual}\t{tags}\n"
    tags = "RG:Z:A"
    for line in input:
        i = int(line)
        qname = f"read_{i}"
        seq = "ACTG" + "ACTG" * i
        qual = "E" * len(seq)
        output.write(sam_line.format(**locals()))

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
        .run()
    )

    print(w.result_dict)
    assert w.result_dict['BAM_header_fifo.worker0'] == (0, 9)
    assert w.result_dict['BAM_header_fifo.worker1'] == (0, 9)
    assert w.result_dict['BAM_header_fifo.worker2'] == (0, 8)
    assert w.result_dict['BAM_header_fifo.worker3'] == (0, 8)
    assert w.result_dict['BAM_header_fifo.funnel0'] == (5, 0)
 
def test_bam_reconstruct(chunk_size=1, n=4):
    w = (
        mf.Workflow('BAM_reconstruct')
        .BAM_reader(input="test_data/tiny_test.bam")
        .distribute(
            input=mf.FIFO("input_sam", "rt"),
            outputs=mf.FIFO("dist{n}", "wt", n=n),
            chunk_size=chunk_size,
            header_detect_func=is_header,
            header_broadcast=False,
            header_fifo=mf.FIFO("header", "wt"),
        )
        .workers(
            input=mf.FIFO("dist{n}", "rt"),
            output=mf.FIFO("out{n}", "wt"),
            func=pass_through,
            n=n,
        )
        .collect(
            inputs=mf.FIFO("out{n}", "rt", n=n),
            header_fifo=mf.FIFO("header", "rt"),
            output=mf.FIFO("out_sam", "wt"),
            chunk_size=chunk_size,
        )
        .funnel(
            input=mf.FIFO("out_sam", "rt"),
            output="test_data/reconstruct.bam",
            _manage_fifos=False,
            func=mf.parts.bam_writer
        )
        .run()
    )
    print(str(w))

    print(w.result_dict)
    from pprint import pprint
    pprint(w._fifo_readers)
    pprint(w._fifo_writers)

    import os
    os.system('samtools view -Sh --no-PG test_data/tiny_test.bam > orig')
    os.system('samtools view -Sh --no-PG test_data/reconstruct.bam > rec')
    os.system('diff orig rec > delta')
    assert len(open('delta').read().strip()) == 0

def test_exception(chunk_size=1, n=4):
    try:
        w = (
            mf.Workflow('exception test')
            .BAM_reader(input="test_data/tiny_test.bam")
            .distribute(
                input=mf.FIFO("input_sam", "rt"),
                outputs=mf.FIFO("dist{n}", "wt", n=n),
                chunk_size=chunk_size,
                header_detect_func=is_header,
                header_broadcast=False,
                header_fifo=mf.FIFO("header", "wt"),
            )
            .workers(
                input=mf.FIFO("dist{n}", "rt"),
                output=mf.FIFO("out{n}", "wt"),
                func=pass_through,
                n=n,
                raise_exception=True,
                pass_internals=True
            )
            .collect(
                inputs=mf.FIFO("out{n}", "rt", n=n),
                header_fifo=mf.FIFO("header", "rt"),
                output=mf.FIFO("out_sam", "wt"),
                chunk_size=chunk_size,
            )
            .funnel(
                input=mf.FIFO("out_sam", "rt"),
                output="test_data/reconstruct.bam",
                _manage_fifos=False,
                func=mf.parts.bam_writer
            )
            .run()
        )
    except mf.WorkflowError:
        pass
    else:
        raise ValueError("expected a WorkflowError in this test!")

def test_fancy_counter():
    w = (
        mf.Workflow('fancy_line_counter')
        .gz_reader(inputs=["test_data/simple.txt.gz"])
        # gz_reader, by default, writes to 'input_text'
        .distribute(
            input=mf.FIFO("input_text", 'rt'),
            outputs=mf.FIFO("dist{n}", "wt", n=4),
            chunk_size=1,
        )
        .workers(
            func=fancy_counter,
            input=mf.FIFO("dist{n}", "rt"),
            n=4,
        )
        .run()
    )
    counts = mf.util.CountDict(w.result_dict.values())
    df = counts.get_stats_df()
    print(df)

def test_BAM_creation(n=2, chunk_size=1):
    w = (
        mf.Workflow('BAM_create')
        .gz_reader(inputs=["test_data/simple.txt.gz"])
        .distribute(
            input=mf.FIFO("input_text", "rt"),
            outputs=mf.FIFO("dist{n}", "wt", n=n),
            chunk_size=chunk_size,
        )
        .workers(
            input=mf.FIFO("dist{n}", "rt"),
            output=mf.FIFO("out{n}", "wt"),
            func=turn_to_SAM,
            n=n,
        )
        .collect(
            inputs=mf.FIFO("out{n}", "rt", n=n),
            custom_header=mf.util.make_SAM_header(),
        #     output="test.sam",
        #     chunk_size=chunk_size,
        # )

            output=mf.FIFO("out_sam", "wt"), #"test.sam",
            chunk_size=chunk_size,
        )
        .funnel(
            input=mf.FIFO("out_sam", "rt"),
            output="test_data/new.bam",
            _manage_fifos=False,
            func=mf.parts.bam_writer,
            fmt="Sbh"
        )
        .run()
    )
    print(str(w))


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)-20s\t%(name)-30s\t%(levelname)s\t%(message)s",)
    # test_plumbing()
    # test_input_output()
    # test_dist()
    # test_dist_work_collect()
    # test_dist_work_funnel()
    # test_dist_work_funnel_count()
    # test_dist_work_collect_funnel()
    # test_header_broadcast()
    # test_header_fifo()
    # test_bam_reconstruct()
    # test_fancy_counter()
    test_BAM_creation()
