import os
import logging
from .contrib import *

def quiet_bam_open(*argc, **kw):
    """_summary_

    This wrapper around pysam.AlignmentFile() simply silences warnings about missing BAM index etc.
    We don't care about the index and therefore these error messages are just spam.

    Returns:
        pysam.AlignmentFile: the sam/bam object as returned by pysam.
    """
    import pysam

    save = pysam.set_verbosity(0)
    bam = pysam.AlignmentFile(*argc, **kw)
    pysam.set_verbosity(save)
    return bam

def ensure_path(path):
    dirname = os.path.dirname(path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    return path

def timed_loop(
    src,
    logger,
    T=5,
    chunk_size=10000,
    template="processed {i} BAM records in {dT:.1f}sec. ({rate:.3f} k rec/sec)",
    skim=0,
):
    from time import time

    t0 = time()
    t_last = t0
    i = 0
    for i, x in enumerate(src):
        if skim:
            if i % skim == 0:
                yield x
        else:
            yield x

        if i % chunk_size == 0:
            t = time()
            if t - t_last > T:
                dT = t - t0
                rate = i / dT / 1000.0
                logger.info(template.format(**locals()))
                t_last = t

    t = time()
    dT = t - t0
    rate = i / dT / 1000.0
    logger.info("Finished! " + template.format(**locals()))



def read_fq(fname, skim=0):
    import gzip

    logger = logging.getLogger("spacemake.util.read_fq")
    if str(fname) == "None":
        logger.warning("yielding empty data forever")
        while True:
            yield ("no_qname", "no_seq", "no_qual")

    if "*" in fname:
        logger.warning("EXPERIMENTAL: fname contains wildcards")
        from glob import glob

        for match in sorted(glob(fname)):
            for rec in read_fq(match, skim=skim):
                yield rec

    logger.info(f"iterating over reads from '{fname}'")
    if fname.endswith(".gz"):
        src = FASTQ_src(gzip.open(fname, mode="rt"))
    elif fname.endswith(".bam"):
        src = BAM_src(fname)
    elif type(fname) is str:
        src = FASTQ_src(open(fname))
    else:
        src = FASTQ_src(fname)  # assume its a stream or file-like object already

    n = 0
    for record in timed_loop(src, logger, T=15, template="processed {i} reads in {dT:.1f}sec. ({rate:.3f} k rec/sec)", skim=skim):
        yield record
        n += 1

    logger.info(f"processed {n} FASTQ records from '{fname}'")


def make_header(bam, progname):
    import os
    import sys

    header = bam.header.to_dict()
    # if "PG" in header:
    # for pg in header['PG']:
    #     if pg["ID"] == progname:
    #         progname = progname + ".1"

    pg_list = header.get("PG", [])
    pg = {
        "ID": progname,
        "PN": progname,
        "CL": " ".join(sys.argv),
        "VN": __version__,
    }
    if len(pg_list):
        pg["PP"] = pg_list[-1]["ID"]

    header["PG"] = pg_list + [pg]
    return header


def header_dict_to_text(header):
    buf = []
    for group, data in header.items():
        if type(data) is dict:
            data = [data]

        for d in data:
            valstr = "\t".join([f"{key}:{value}" for key, value in d.items()])
            buf.append(f"@{group}\t{valstr}")

    return "\n".join(buf)


COMPLEMENT = {
    "a": "t",
    "t": "a",
    "c": "g",
    "g": "c",
    "k": "m",
    "m": "k",
    "r": "y",
    "y": "r",
    "s": "s",
    "w": "w",
    "b": "v",
    "v": "b",
    "h": "d",
    "d": "h",
    "n": "n",
    "A": "T",
    "T": "A",
    "C": "G",
    "G": "C",
    "K": "M",
    "M": "K",
    "R": "Y",
    "Y": "R",
    "S": "S",
    "W": "W",
    "B": "V",
    "V": "B",
    "H": "D",
    "D": "H",
    "N": "N",
    "-": "-",
    "=": "=",
    "+": "+",
}


def complement(s):
    return "".join([COMPLEMENT[x] for x in s])


def rev_comp(seq):
    return complement(seq)[::-1]


def fasta_chunks(lines, strip=True, fuse=True):
    chunk = ""
    data = []

    for l in lines:
        if l.startswith("#"):
            continue
        if l.startswith(">"):
            if data and chunk:
                # print chunk
                yield chunk, "".join(data)

                if strip:
                    data = []
                else:
                    data = [l]

            chunk = l[1:].strip()
        else:
            if fuse:
                data.append(l.rstrip())
            else:
                data.append(l)

    if data and chunk:
        yield chunk, "".join(data)


# @contextmanager
# def message_aggregation(log_listen="spacemake", print_logger=False, print_success=True):
#     message_buffer = []

#     log = logging.getLogger(log_listen)
#     log.setLevel(logging.INFO)

#     class MessageHandler(logging.NullHandler):
#         def handle(this, record):
#             if record.name == log_listen:
#                 if print_logger:
#                     print(f"{log_listen}: {record.msg}")
#                 else:
#                     print(record.msg)

#     log.addHandler(MessageHandler())

#     try:
#         yield True

#         if print_success:
#             print(f"{LINE_SEPARATOR}SUCCESS!")

#     except SpacemakeError as e:
#         print(e)


def load_yaml(path, mode="rt"):
    if not path:
        return {}
    else:
        import yaml

        return yaml.load(path, mode)


def setup_logging(
    args,
    name="spacemake.main",
    log_file="",
    FORMAT="%(asctime)-20s\t{sample:30s}\t%(name)-50s\t%(levelname)s\t%(message)s",
):
    sample = getattr(args, "sample", "na")
    import setproctitle
    if name != "spacemake.main":
        setproctitle.setproctitle(f"{name} {sample}")

    FORMAT = FORMAT.format(sample=sample)

    log_level = getattr(args, "log_level", "INFO")
    lvl = getattr(logging, log_level)
    logging.basicConfig(level=lvl, format=FORMAT)
    root = logging.getLogger("spacemake")
    root.setLevel(lvl)

    log_file = getattr(args, "log_file", log_file)
    if log_file:
        fh = logging.FileHandler(filename=ensure_path(log_file), mode="a")
        fh.setFormatter(logging.Formatter(FORMAT))
        root.debug(f"adding log-file handler '{log_file}'")
        root.addHandler(fh)

    if hasattr(args, "debug"):
        # cmdline requested debug output for specific domains (comma-separated)
        for logger_name in args.debug.split(","):
            if logger_name:
                root.info(f"setting domain {logger_name} to DEBUG")
                logging.getLogger(logger_name.replace("root", "")).setLevel(
                    logging.DEBUG
                )

    logger = logging.getLogger(name)
    logger.debug("started logging")
    for k, v in sorted(vars(args).items()):
        logger.debug(f"cmdline arg\t{k}={v}")

    return logger

def setup_smk_logging(name="spacemake.smk", **kw):
    import argparse
    args = argparse.Namespace(**kw)
    return setup_logging(args, name=name)

default_log_level = "INFO"


def make_minimal_parser(prog="", usage="", **kw):
    import argparse

    parser = argparse.ArgumentParser(prog=prog, usage=usage, **kw)
    parser.add_argument(
        "--log-file",
        default=f"{prog}.log",
        help=f"place log entries in this file (default={prog}.log)",
    )
    parser.add_argument(
        "--log-level",
        default=default_log_level,
        help=f"change threshold of python logging facility (default={default_log_level})",
    )
    parser.add_argument(
        "--debug",
        default="",
        help=f"comma-separated list of logging-domains for which you want DEBUG output",
    )
    parser.add_argument(
        "--sample", default="sample_NA", help="sample_id (where applicable)"
    )
    return parser



def batched(iterable, n):
    "Batch data into lists of length n. The last batch may be shorter."
    from itertools import islice
    # batched('ABCDEFG', 3) --> ABC DEF G
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch

