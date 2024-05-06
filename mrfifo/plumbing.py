from .contrib import *
import logging

# TODO: proper logging
from contextlib import contextmanager


max_buffer_size = 1048576
min_buffer_size = 65536  #
# page size is normally 4096 but on some very high RAM systems it could be 8192
# let's be on the safe side and round up in that case
page_size = 8192


@contextmanager
def create_named_pipes(names, total_pipe_buffer=16777216):
    import os
    import logging
    import tempfile

    path_dict = {}
    path_lookup = {}
    logger = logging.getLogger("mrfifo.plumbing.create_named_pipes")
    buffer_size = min(
        max_buffer_size, int((total_pipe_buffer / page_size // len(names))) * page_size
    )
    logging.debug(f"planning to allocate {buffer_size} bytes of pipe buffer per FIFO")
    if buffer_size < min_buffer_size:
        raise OSError(
            "Not enough pipe buffer assigned to this workflow. Individual pipe buffers would drop below 64kB."
        )

    with tempfile.TemporaryDirectory() as base:
        paths = [os.path.join(base, name) for name in names]
        logger.debug(f"creating the following FIFOs: '{paths}'")
        for fname in paths:
            if os.path.exists(fname):
                logger.warning(
                    f"can not create fifo because file already exists '{fname}'"
                )
            else:
                os.mkfifo(fname)
        try:
            pipe_dict = {"buffer_size": buffer_size, "paths": dict(zip(names, paths))}
            yield pipe_dict
        finally:
            # Clean up the named pipes
            for fname in paths:
                os.remove(fname)


def open_named_pipe(path, mode="rt+", buffer_size=1048576):  # 524288
    import fcntl

    F_SETPIPE_SZ = 1031  # Linux 2.6.35+
    # F_GETPIPE_SZ = 1032  # Linux 2.6.35+

    logger = logging.getLogger("mrfifo.plumbing.open_named_pipes")
    logger.debug(f"opening pipe '{path}' with mode='{mode}'")
    fifo_fd = open(path, mode)
    try:
        fcntl.fcntl(fifo_fd, F_SETPIPE_SZ, buffer_size)
    except OSError as err:
        logger.error(
            f"could not set pipe buffer_size={buffer_size} on '{path}'. "
            "Please reduce the total amount of pipe-buffer available to this workflow."
        )
        raise err

    return fifo_fd


class FIFO:
    def __init__(self, name, mode, n=None):
        self.logger = logging.getLogger(f"mrfifo.FIFO.{name}")
        self.name = name
        self.mode = mode
        self.n = n
        self.file_objects = []

        assert mode[0] in ["r", "w"]

    def _format_name(self, context):
        return FIFO(self.name.format(**context), self.mode, n=self.n)

    def is_collection(self):
        return self.n is not None

    def is_reader(self):
        return self.mode[0] == "r"

    def get_names(self, **kw):
        if self.is_collection():
            return [self.name.format(n=i, **kw) for i in range(self.n)]
        else:
            return [
                self.name,
            ]

    def open(self, pipe_dict, manage_fifos=True, **kw):
        buffer_size = pipe_dict["buffer_size"]
        if manage_fifos:

            for name in self.get_names(**kw):
                pipe_path = pipe_dict["paths"][name]

                self.file_objects.append(
                    open_named_pipe(pipe_path, mode=self.mode, buffer_size=buffer_size)
                )

            if self.is_collection():
                return self.file_objects
            else:
                return self.file_objects[0]
        else:
            # unmanaged, return paths to fifos instead of open files
            paths = [pipe_dict["paths"][name] for name in self.get_names(**kw)]
            if self.is_collection():
                return paths
            else:
                return paths[0]

    def close(self):
        for f in self.file_objects:
            self.logger.debug(f"closing {f}")
            f.flush()
            f.close()
            self.logger.debug(f"done closing {f}")

        self.file_objects = []

    def __repr__(self):
        return f"FIFO({self.mode} names={self.get_names()}) n={self.n}"


def fifo_routed(
    f, pass_internals=False, fifo_name_format={}, _manage_fifos=True, **kwargs
):
    from functools import wraps

    fifo_vars = {}
    kw = {}
    for k, v in kwargs.items():
        # print(f"{k} : {v} {type(v)}")
        if type(v) is FIFO:
            if fifo_name_format:
                v = v._format_name(fifo_name_format)
            fifo_vars[k] = v
        # elif type(v) is ITER:
        #     # replace the ITER object with whatever is returned at the current iteration
        #     # this may raise a StopIteration
        #     kw[k] = v.get()
        else:
            kw[k] = v

    @wraps(f)
    def wrapper(
        result_dict={}, pipe_dict={}, exc_dict={}, job_name="job", args=(), **kwds
    ):
        kwargs = kw.copy()
        kwargs.update(**kwds)

        for target, fifo in fifo_vars.items():
            kwargs[target] = fifo.open(pipe_dict, manage_fifos=_manage_fifos)

        from . import parallel

        with parallel.ExceptionLogging(name=job_name, exc_dict=exc_dict) as el:
            try:
                if pass_internals:
                    kwargs["_job_name"] = job_name
                    kwargs["_logger"] = el.logger
                    kwargs["_buffer_size"] = pipe_dict["buffer_size"]

                res = f(*args, **kwargs)
                result_dict[job_name] = res

            finally:
                for fifo in fifo_vars.values():
                    fifo.close()

    return wrapper, fifo_vars
