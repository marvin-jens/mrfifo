from .contrib import *
# import logging
# TODO: proper logging
from contextlib import contextmanager


@contextmanager
def create_named_pipes(names):
    import os
    import logging
    import tempfile
    path_dict = {}
    path_lookup = {}
    logger = logging.getLogger("mrfifo.plumbing.create_named_pipes")
    with tempfile.TemporaryDirectory() as base:
        paths = [os.path.join(base, name) for name in names]
        for fname in paths:
            if os.path.exists(fname):
                logger.warning(f"can not create fifo because file already exists '{fname}'")
            else:
                os.mkfifo(fname)
        try:
            yield dict(zip(names, paths))
        finally:
            # Clean up the named pipes
            for fname in paths:
                os.remove(fname)

def open_named_pipe(path, mode='rt+', buffer_size=1000000):
    import fcntl
    F_SETPIPE_SZ = 1031  # Linux 2.6.35+
    # F_GETPIPE_SZ = 1032  # Linux 2.6.35+

    fifo_fd = open(path, mode)
    fcntl.fcntl(fifo_fd, F_SETPIPE_SZ, buffer_size)
    return fifo_fd
