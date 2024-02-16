from .contrib import *
from .parallel import ExceptionLogging
from .plumbing import open_named_pipe
import logging

def igzip_reader(inputs, output):
    "ensure that the out pipe is opened in binary mode"
    assert 'b' in output.mode
    logger=logging.getLogger('mrfifo.parts.igzip_reader')
    n_bytes = 0
    from isal import igzip
    for fname in inputs:
        logger.info(f"reading from {fname}")
        in_file = igzip.IGzipFile(fname, 'r')
        while True:
            block = in_file.read(igzip.READ_BUFFER_SIZE)
            if block == b"":
                break

            output.write(block)
            n_bytes += len(block)
        
        in_file.close()

    return n_bytes


def bam_reader(input, output, threads=2, name="mrfifo.parts.bam_reader"):
    "ensure that the out FIFO is not managed"
    assert type(output) is str
    import os
    return os.system(f'samtools view -Sh --no-PG --threads={threads} {input} > {output}')


def distributor(input, outputs, **kw):
    "ensure that the FIFOs are not managed"
    assert type(input) is str
    logger = logging.getLogger("mrfifo.parts.distributor")
    logger.info(f"reading from {input}, writing to {outputs} kw={kw}")

    from .fast_loops import distribute
    res = distribute(fin_name=input, fifo_names=outputs, **kw)
    logger.info("distribution complete")
    return res


def collector(inputs, output, name="mrfifo.parts.collector", **kw):
    "ensure that the FIFOs are not managed"
    logger = logging.getLogger("mrfifo.parts.collector")
    logger.info(f"collecting from '{inputs}', writing to '{output}' kw={kw}")
    from .fast_loops import collect
    res = collect(fifo_names=inputs, fout_name=output, **kw)
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

