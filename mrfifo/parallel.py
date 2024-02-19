import time
from .contrib import __version__, __author__, __license__, __email__

import logging
from contextlib import contextmanager

class ExceptionLogging:
    """
    A context manager that handles otherwise uncaught exceptions by logging
    the event and traceback info, optinally raises a flag.
    Very handy for wrapping the main function in a sub-process!
    """

    def __init__(self, name, exc_flag=None, exc_dict=None, set_proc_title=True):
        self.exc_flag = exc_flag
        self.exc_dict = exc_dict
        self.name = name
        self.logger = logging.getLogger(name)
        self.exception = None
        self.logger.debug(f"ExceptionLogging(name={name}, exc_flag={exc_flag}, exc_dict={exc_dict})")

        if set_proc_title:
            import setproctitle
            setproctitle.setproctitle(name)

    def __enter__(self):
        self.t0 = time.time()
        # print('__enter__ called')
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # print('__exit__ called')
        self.t1 = time.time()
        self.logger.debug(f"CPU time: {self.t1 - self.t0:.3f} seconds.")
        if exc_type and (exc_type != SystemExit):
            import traceback

            lines = "\n".join(
                traceback.format_exception(exc_type, exc_value, exc_traceback)
            ).split("\n")
            self.exception = lines
            self.logger.error(f"an unhandled exception occurred")
            if self.exc_dict is None:
                for l in lines:
                    self.logger.error(l)
            else:
                self.exc_dict[self.name] = lines

            if self.exc_flag is not None:
                self.logger.error(f"raising exception flag {self.exc_flag}")
                self.exc_flag.value = True

