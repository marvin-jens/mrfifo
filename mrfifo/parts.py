from .contrib import *
from .parallel import ExceptionLogging
from .plumbing import open_named_pipe
import logging


def igzip_reader(inputs, output):
    "ensure that the out pipe is opened in binary mode"
    assert "b" in output.mode
    logger = logging.getLogger("mrfifo.parts.igzip_reader")
    n_bytes = 0
    from isal import igzip

    for fname in inputs:
        logger.info(f"reading from {fname}")
        try:
            in_file = igzip.IGzipFile(fname, "r")
            while True:
                block = in_file.read(igzip.READ_BUFFER_SIZE)
                if block == b"":
                    break

                output.write(block)
                n_bytes += len(block)

        except igzip.BadGzipFile:
            logger.warning(f"Not a proper gzip file: '{fname}'. Assuming text file.")
            in_file = open(fname, "rb")
            while True:
                block = in_file.read(igzip.READ_BUFFER_SIZE)
                if block == b"":
                    break

                output.write(block)
                n_bytes += len(block)

        finally:
            in_file.close()

    return n_bytes


def bam_reader(input, output, threads=2, mode="Sh"):
    "ensure that the out FIFO is not managed"
    assert type(output) is str
    import os

    return os.system(
        f"samtools view -{mode} --no-PG --threads={threads} {input} > {output}"
    )


def bam_writer(input, output, threads=4, fmt="Sbh"):
    "ensure that the out FIFOs are not managed"
    assert type(output) is str
    assert type(input) is str
    import os

    return os.system(
        f"samtools view -{fmt} --no-PG --threads={threads} {input} > {output}"
    )


def null_writer(input, output="/dev/null", threads=4, fmt="Sbh"):
    """
    drop-in replacement for bam_writer that just redirects to wherever you like
    """
    import os

    return os.system(f"cat {input} > {output}")


def distributor(input, outputs, **kw):
    "ensure that the FIFOs are not managed"
    assert type(input) is str
    logger = logging.getLogger("mrfifo.parts.distributor")
    logger.debug(f"reading from {input}, writing to {outputs} kw={kw}")

    from .fast_loops import distribute

    res = distribute(fin_name=input, fifo_names=outputs, **kw)
    logger.debug("distribution complete")
    return res


def distribute_by_CB(input, outputs, l_prefix=4, **kw):
    "ensure that the FIFOs are not managed"
    assert type(input) is str
    logger = logging.getLogger("mrfifo.parts.distribute_by_CB")
    logger.debug(f"reading from {input}, writing to {outputs} kw={kw}")

    from .fast_loops import distribute_by_substr
    from mrfifo.util import make_CB_dist_map

    d = make_CB_dist_map(r=l_prefix, n=len(outputs))
    # print(sorted(d.items()))
    res = distribute_by_substr(
        fin_name=input, fifo_names=outputs, sub_lookup=d, sub_size=l_prefix, **kw
    )
    logger.debug("distribution complete")
    return res


def collector(inputs, output, **kw):
    "ensure that the FIFOs are not managed"
    for inp in inputs:
        assert type(inp) is str

    assert type(output) is str

    logger = logging.getLogger("mrfifo.parts.collector")
    logger.debug(f"collecting from '{inputs}', writing to '{output}' kw={kw}")
    from .fast_loops import collect

    res = collect(fifo_names=inputs, fout_name=output, **kw)
    logger.debug("collection complete")
    return res


# def serializer(inputs, outputs, name="mrfifo.parts.serializser", **kw):
#     assert len(inputs) > 0
#     assert len(outputs) == 1
#     fout = outputs.pop()

#     with ExceptionLogging(name) as el:
#         el.logger.debug(f"writing to '{fout}'")
#         with open(fout, 'w') as out:
#             for fin in inputs:
#                 el.logger.debug(f"reading from '{fin}'")
#                 f = open(fin)
#                 for line in f:
#                     # el.logger.debug(f"got line {line}")
#                     out.write(line)
#                 f.close()
