import multiprocessing as mp
# weirdly needed to avoid tests hanging if run as an entire suite from pytest
# see: https://github.com/pytest-dev/pytest/issues/11174
# mp.set_start_method("spawn", force=True)

import logging
import pytest
import mrfifo as mf

from setproctitle import setproctitle

def line_counter(inputs=[], outputs=[], name="job"):
    setproctitle(name)
    logger = logging.getLogger(name)
    assert len(inputs) == 1
    assert len(outputs) < 2
    finput = inputs.pop()
    n = 0
    for line in open(finput):
        n += 1
    
    logger.debug(f"counted to {n}")
    if outputs:
        fout = outputs.pop()
        logger.debug(f"writing to output '{fout}'")
        f = open(fout, 'wt')
        f.write(f"results {name} {n}\n")
        f.close()
    
    logger.debug(f"exiting")
    
def add_lines(inputs=[], outputs=[], name="job"):
    setproctitle(name)
    logger = logging.getLogger(name)

    assert len(outputs) == 1
    assert len(inputs) > 0
    N = 0
    for fin in inputs:
        for line in open(fin):
            n = int(line.split(' ')[-1])
            N += n
            print(f"acc at {N} after reading line from {fin}")
    
    fout = outputs.pop()
    open(fout, "w").write(f"total_count {N}\n")


def test_plumbing():
    with mf.plumbing.create_named_pipes(['fq_in', 'w0', 'w1', 'w2', 'w3', 'bam_out']) as pipes:
        from pprint import pprint
        pprint(pipes)

def test_input_output():
    w = (
        mf.Workflow('input_output')
        .reader(
            inputs = ["../spacemake/test_data/reads_chr22_R1.fastq.gz"],
            inputs_are_files=True,
            func=mf.parts.igzip_reader,
            outputs=["input.decompressed"],
        )
        .funnel(
            inputs=["input.decompressed"],
            func=line_counter, #mf.parts.serializer,
            output="/dev/stdout",
            output_is_file=True,
        )
    )
    w.run()

def test_dist():
    input_files = ["../spacemake/test_data/reads_chr22_R1.fastq.gz"]
    # TODO: create_named_pipes dict support

    w = (
        mf.Workflow('simple_fastq_line_counter')
        .reader(
            inputs = ["../spacemake/test_data/reads_chr22_R1.fastq.gz"],
            inputs_are_files=True,
            func=mf.parts.igzip_reader,
            outputs=["input.decompressed"],
        )
        .distribute(
            input="input.decompressed",
            output_pattern="decompressed.dist{n}",
            n=4, chunk_size=1,
        )
        .workers(
            inputs=["decompressed.dist{n}"],
            func=line_counter,
            n=4,
        )
    )
    w.run(dry_run=False)

           
def test_dist_work_collect():
    w = (
        mf.Workflow('simple_fastq_line_counter')
        .reader(
            inputs = ["../spacemake/test_data/reads_chr22_R1.fastq.gz"],
            inputs_are_files=True,
            func=mf.parts.igzip_reader,
            outputs=["input.decompressed"],
        )
        .distribute(
            input="input.decompressed",
            output_pattern="decompressed.dist{n}",
            n=4, chunk_size=1,
        )
        .workers(
            inputs=["decompressed.dist{n}"],
            outputs=["counted{n}"],
            func=line_counter,
            n=4,
        )
        .collect(
            input_pattern="counted{n}",
            output="/dev/stdout",
            output_is_file=True,
            n=4, chunk_size=1,
            func=mf.parts.collector,
        )
    )
    w.run(dry_run=False)


def test_dist_work_funnel():
    input_files = ["../spacemake/test_data/reads_chr22_R1.fastq.gz"]
    # TODO: create_named_pipes dict support

    w = (
        mf.Workflow('simple_fastq_line_counter')
        .reader(
            inputs = ["../spacemake/test_data/reads_chr22_R1.fastq.gz"],
            inputs_are_files=True,
            func=mf.parts.igzip_reader,
            outputs=["input.decompressed"],
        )
        .distribute(
            input="input.decompressed",
            output_pattern="decompressed.dist{n}",
            n=4, chunk_size=1,
        )
        .workers(
            inputs=["decompressed.dist{n}"],
            func=line_counter,
            n=4,
            outputs=["counted{n}"],
        )
        .funnel(
            inputs=["counted0", "counted1", "counted2", "counted3"],
            output="/dev/stdout",
            output_is_file=True,
            func=add_lines,
        )
        .run()
    )


def test_dist_work_funnel_count():
    input_files = ["../spacemake/test_data/reads_chr22_R1.fastq.gz"]
    # TODO: create_named_pipes dict support

    w = (
        mf.Workflow('simple_fastq_line_counter')
        .reader(
            inputs = ["../spacemake/test_data/reads_chr22_R1.fastq.gz"],
            inputs_are_files=True,
            func=mf.parts.igzip_reader,
            outputs=["input.decompressed"],
        )
        .distribute(
            input="input.decompressed",
            output_pattern="decompressed.dist{n}",
            n=4, chunk_size=1,
        )
        .workers(
            inputs=["decompressed.dist{n}"],
            func=line_counter,
            n=4,
            outputs=["counted{n}"],
        )
        .funnel(
            inputs=["counted0", "counted1", "counted2", "counted3"],
            output="merged",
            output_is_file=False,
            func=mf.parts.serializer,
        )
        .funnel(
            inputs=["merged"],
            output="/dev/stdout",
            output_is_file=True,
            func=add_lines,
        )

    )
    w.run(dry_run=False)


def test_dist_work_collect_funnel():
    w = (
        mf.Workflow('simple_fastq_line_counter')
        .reader(
            inputs = ["../spacemake/test_data/reads_chr22_R1.fastq.gz"],
            inputs_are_files=True,
            func=mf.parts.igzip_reader,
            outputs=["input.decompressed"],
        )
        .distribute(
            input="input.decompressed",
            output_pattern="decompressed.dist{n}",
            n=4, chunk_size=1,
        )
        .workers(
            inputs=["decompressed.dist{n}"],
            func=line_counter,
            n=4,
            outputs=["counted{n}"],
        )
        .collect(
            input_pattern="counted{n}",
            output="merged",
            output_is_file=False,
            n=4, chunk_size=1,
            func=mf.parts.collector,
        )
        .funnel(
            inputs=["merged"],
            output="/dev/stdout",
            output_is_file=True,
            func=add_lines,
        )
    )
    w.run(dry_run=False)


def header_checker(inputs=[], outputs=[], name="bla", **kwargs):
    assert len(inputs) == 1
    sam_in = inputs.pop()
    header = []
    n_bam_records = 0
    for line in open(sam_in):
        if line.startswith('@'):
            header.append(line)
        else:
            n_bam_records += 1
    
    for x in header:
        print(x.rstrip())

    print(f"worker {name} received {len(header)} header records and {n_bam_records} BAM records")


def is_header(line):
    return line.startswith('@')

def test_header_broadcast():
    
    w = (
        mf.Workflow('BAM_header_checker')
        .reader(
            inputs = ["test_data/tiny_test.bam"],
            inputs_are_files=True,
            func=mf.parts.bam_reader,
            outputs=["input.SAM"],
        )
        .distribute(
            input="input.SAM",
            output_pattern="SAM.dist{n}",
            n=4, chunk_size=1,
            header_detect_func=is_header,
            header_broadcast=True,
        )
        .workers(
            inputs=["SAM.dist{n}"],
            func=header_checker,
            n=4,
            outputs=[],
        )
    )
    w.run()


def test_header_fifo():
    
    w = (
        mf.Workflow('BAM_header_checker')
        .reader(
            inputs = ["test_data/tiny_test.bam"],
            inputs_are_files=True,
            func=mf.parts.bam_reader,
            outputs=["input.SAM"],
        )
        .distribute(
            input="input.SAM",
            output_pattern="SAM.dist{n}",
            output_header="SAM.header",
            n=4, chunk_size=1,
            header_detect_func=is_header,
        )
        .workers(
            inputs=["SAM.dist{n}"],
            func=header_checker,
            n=4,
            outputs=[],
        )
        .funnel(
            inputs=["SAM.header"],
            func=header_checker
        )
    )
    w.run()

import multiprocessing as mp


def file_reader(fname, out):
    n_lines_read = 0
    for line in open(fname):
        out.write(line)
        n_lines_read += 1
    
    return n_lines_read

def simple_counter(src):
    i = 0
    for line in src:
        i += 1
    
    return i

def pass_through(src, out):
    i = 0
    for line in src:
        i += 1
        out.write(line)
    
    return i

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    print("__main__.simple_counter", simple_counter)
    w = (
        mf.Workflow('test')
        # .reader(func=file_reader, fname="test_data/simple.txt", out=mf.FIFO("text", 'w'))
        .gz_reader(input_files=["test_data/simple.txt.gz"])
        .distribute(src=mf.FIFO("input_text", 'rt'), outputs=mf.FIFO("dist{n}", "wt", n=4), chunk_size=1)
        .workers(func=pass_through, src=mf.FIFO("dist{n}", "rt"), out=mf.FIFO("out{n}", "wt"), n=4)
        .collect(inputs=mf.FIFO("out{n}", "rt", n=4), out="/dev/stderr", chunk_size=1)
        .run(dry_run=False)
    )
    for jobname, res in w.result_dict.items():
        print(f"{jobname}\t{res}")

# if __name__ == "__main__":
    # test_plumbing()
    # test_input_output()
    # test_dist()
    # test_dist_work_collect()
    # test_dist_work_funnel()
    # test_dist_work_funnel_count()
    # test_dist_work_collect_funnel()
    # test_header_broadcast()
    # test_header_fifo()
