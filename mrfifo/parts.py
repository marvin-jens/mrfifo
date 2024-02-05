from .contrib import *
from .parallel import ExceptionLogging
from .plumbing import open_named_pipe

def igzip_reader(input_files, pipe):
    with ExceptionLogging("mrfifo.util.igzip_reader") as el:
        el.logger.info(f"writing to {pipe}")
        from isal import igzip
        out_file = open_named_pipe(pipe, mode='wb')

        try:
            for fname in input_files:
                el.logger.info(f"reading from {fname}")
                in_file = igzip.IGzipFile(fname, 'r')
                while True:
                    block = in_file.read(igzip.READ_BUFFER_SIZE)
                    if block == b"":
                        break

                    out_file.write(block)
                
                in_file.close()
        finally:
            el.logger.info(f"closing down {pipe}")
            out_file.close()


def FASTQ_src(src):
    from more_itertools import grouper

    for name, seq, _, qual in grouper(src, 4):
        yield name.rstrip()[1:], seq.rstrip(), qual.rstrip()


def BAM_src(src):
    import pysam

    bam = quiet_bam_open(src, "rb", check_sq=False)
    for read in bam.fetch(until_eof=True):
        yield read.query_name, read.query_sequence, read.query_qualities


def distributor_main(in_pipe, worker_in_pipes, **kw):
    with ExceptionLogging("mrfifo.util.distributor_main") as el:
        el.logger.info(f"reading from {in_pipe}, writing to {worker_in_pipes} kw={kw}")
        from .fast_loops import distribute
        distribute(in_pipe, worker_in_pipes, **kw)

