from .contrib import *
from .parallel import ExceptionLogging
from .plumbing import open_named_pipe
import logging

def igzip_reader(input_files, out):
    "ensure that the out pipe is opened in binary mode"
    assert 'b' in out.mode
    logger=logging.getLogger('mrfifo.parts.igzip_reader')
    n_bytes = 0
    from isal import igzip
    for fname in input_files:
        logger.info(f"reading from {fname}")
        in_file = igzip.IGzipFile(fname, 'r')
        while True:
            block = in_file.read(igzip.READ_BUFFER_SIZE)
            if block == b"":
                break

            out.write(block)
            n_bytes += len(block)
        
        in_file.close()

    return n_bytes

def bam_reader(bam_name, out, threads=2, name="mrfifo.parts.bam_reader"):
    "ensure that the out FIFO is not managed"
    assert type(out) is str
    import os
    os.system(f'samtools view -Sh --no-PG --threads={threads} {bam_name} > {out}')

def distributor(src, outputs, **kw):
    "ensure that the FIFOs are not managed"
    assert type(src) is str
    logger = logging.getLogger("mrfifo.parts.distributor")
    logger.info(f"reading from {src}, writing to {outputs} kw={kw}")

    from .fast_loops import distribute
    res = distribute(fin_name=src, fifo_names=outputs, **kw)
    logger.info("distribution complete")
    return res


def collector(inputs, out, name="mrfifo.parts.collector", **kw):
    "ensure that the FIFOs are not managed"
    logger = logging.getLogger("mrfifo.parts.collector")
    logger.info(f"collecting from '{inputs}', writing to '{out}' kw={kw}")
    from .fast_loops import collect
    res = collect(fifo_names=inputs, fout_name=out, **kw)
    logger.info("collection complete")
    return res

def serializer(inputs, outputs, name="mrfifo.parts.serializser", **kw):
    assert len(inputs) > 0
    assert len(outputs) == 1
    fout = outputs.pop()

    with ExceptionLogging(name) as el:
        el.logger.debug(f"writing to '{fout}'")
        with open(fout, 'w') as out:
            for fin in inputs:
                el.logger.debug(f"reading from '{fin}'")
                f = open(fin)
                for line in f:
                    # el.logger.debug(f"got line {line}")
                    out.write(line)
                f.close()

