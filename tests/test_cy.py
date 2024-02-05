import multiprocessing as mp
# weirdly needed to avoid tests hanging if run as an entire suite from pytest
# see: https://github.com/pytest-dev/pytest/issues/11174
mp.set_start_method("spawn", force=True)

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