import os
import sys
import logging
from .contrib import __version__, __license__, __author__, __email__


# This should go to 'parts' or a few example implementations
class CountDict:
    def __init__(self, others=[]):
        from collections import defaultdict

        self.stats = defaultdict(float)
        for o in others:
            if type(o) is CountDict:
                self.add_other_stats(o)

    def count(self, key, inc=1):
        self.stats[key] += inc

    def add_other_stats(self, other):
        for k, v in other.items():
            self.stats[k] += v

    def get_stats_df(self):
        import pandas as pd

        return pd.DataFrame(
            dict(
                [
                    (
                        k,
                        [
                            v,
                        ],
                    )
                    for k, v in sorted(self.stats.items())
                ]
            )
        )

    def save_stats(self, path, sep="\t", **kw):
        self.get_stats_df().to_csv(path, sep=sep, **kw)

    def items(self):
        return self.stats.items()

    def keys(self):
        return self.stats.keys()

    def values(self):
        return self.stats.values()


def timed_loop(
    src,
    logger,
    T=5,
    chunk_size=10000,
    template="processed {i} records in {dT:.1f}sec. ({rate:.3f} k rec/sec)",
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


header_template = """@HD\tVN:{sam_version}
@RG\tID:{rg_id}\tSM:{rg_name}
@PG\tPN:{prog_name}\tID:{prog_id}\tVN:{prog_version}\tCL:{cmdline}
"""


def make_SAM_header(
    sam_version="1.6",
    rg_id="A",
    rg_name="sample",
    prog_name=sys.argv[0],
    prog_id=sys.argv[0],
    cmdline=" ".join(sys.argv),
    prog_version=__version__,
):

    return header_template.format(**locals())
