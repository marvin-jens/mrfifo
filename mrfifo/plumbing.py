from .contrib import *
# import logging
# TODO: proper logging
from contextlib import contextmanager


@contextmanager
def create_named_pipes(names):
    import os
    import tempfile
    with tempfile.TemporaryDirectory() as base:
        paths = [os.path.join(base, name) for name in names]
        #print(paths)
        # create new fifos (named pipes)
        [os.mkfifo(fname) for fname in paths]

        try:
            yield paths
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
