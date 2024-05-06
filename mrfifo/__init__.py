from .contrib import __version__, __license__, __author__, __email__

import logging
from functools import wraps
from collections import defaultdict

from . import plumbing
from . import parts
from . import util

from .plumbing import FIFO, fifo_routed


class WorkflowError(Exception):
    pass


class Job:
    def __init__(self, func, result_dict={}, exc_dict={}, pipe_dict={}, name="job"):
        self.name = name
        self.func = func
        self.result_dict = result_dict
        self.exc_dict = exc_dict
        self.pipe_dict = pipe_dict
        self.p = None

    def create(self):
        import multiprocessing as mp

        return mp.Process(
            target=self.func,
            kwargs=dict(
                pipe_dict=self.pipe_dict,
                result_dict=self.result_dict,
                exc_dict=self.exc_dict,
                job_name=self.name,
            ),
        )

    def start(self):
        assert self.p is None
        self.p = self.create()
        self.p.start()

    def join(self):
        assert self.p is not None
        self.p.join()

    def __str__(self):
        return f"Job({self.name}) func={self.func.__name__}"


class Workflow:
    def __init__(self, name, n=4, total_pipe_buffer_MB=16):
        self.name = name
        self.n = n
        self.total_pipe_buffer = int(total_pipe_buffer_MB * 1048576)
        self.logger = logging.getLogger(f"{self.name}.Workflow")
        self.job_count_by_pattern = defaultdict(int)

        self._jobs = []
        self._subs = []

        self._fifo_readers = defaultdict(list)
        self._fifo_writers = defaultdict(list)
        self._fifo_balance = defaultdict(int)

        import multiprocessing as mp

        self.manager = mp.Manager()
        self.pipe_dict = self.manager.dict()
        self.result_dict = self.manager.dict()
        self.exc_dict = self.manager.dict()

    def register_fifos(
        self, fifos, job_name, n_reopen_inputs=1, n_reopen_outputs=1, **kwargs
    ):
        n_readers = 0
        n_writers = 0
        for f in fifos:
            for name in f.get_names():
                if f.is_reader():
                    self._fifo_balance[name] += n_reopen_inputs
                    self._fifo_readers[name].append(job_name)
                    n_readers += 1
                else:
                    self._fifo_balance[name] -= n_reopen_outputs
                    self._fifo_writers[name].append(job_name)
                    n_writers += 1

        return n_readers, n_writers

    def check(self):
        unbalanced = False
        for name, bal in self._fifo_balance.items():
            if bal > 0:
                self.logger.error(
                    f"fifo {name} has a reader but no writer! (balance={bal})"
                )
                unbalanced = name
            elif bal < 0:
                self.logger.error(
                    f"fifo {name} has a writer but no reader! (balance={bal})"
                )
                unbalanced = name

        if unbalanced:
            raise ValueError(f"workflow '{self.name}' has deadlocking fifo '{name}'")

    def get_pipe_list(self):
        return sorted(self._fifo_balance.keys())

    def render_job_name(self, job_name):
        n = self.job_count_by_pattern[job_name]
        self.job_count_by_pattern[job_name] += 1

        return job_name.format(workflow=self.name, n=n)

    def add_job(
        self,
        *argc,
        func=None,
        job_name="{workflow}.job{n}",
        assert_n_reader_ge=None,
        assert_n_writer_ge=None,
        assert_n_reader_le=None,
        assert_n_writer_le=None,
        **kwargs,
    ):

        assert func is not None
        job_name = self.render_job_name(job_name)

        job_func, fifo_vars = fifo_routed(func, *argc, **kwargs)
        job = Job(
            job_func,
            result_dict=self.result_dict,
            pipe_dict=self.pipe_dict,
            exc_dict=self.exc_dict,
            name=job_name,
        )

        n_readers, n_writers = self.register_fifos(
            fifo_vars.values(), job_name=job_name, **kwargs
        )
        self.logger.debug(
            f"add_job job={job} n_readers={n_readers} n_writers={n_writers} fifo_vars={fifo_vars}"
        )
        self._jobs.append(job)

        if assert_n_reader_ge:
            assert n_readers >= assert_n_reader_ge
        if assert_n_writer_ge:
            assert n_writers >= assert_n_writer_ge
        if assert_n_reader_le:
            assert n_readers <= assert_n_reader_le
        if assert_n_writer_le:
            assert n_writers <= assert_n_writer_le

        return self

    def subworkflow(self, name="subworkflow"):
        sub = Workflow(name)
        # sub-workflows share the plumbing
        sub.pipe_dict = self.pipe_dict
        self._subs.append(sub)
        self._jobs.append(sub)

        return sub

    # presets/short-hands for more readable workflow compositions
    def reader(self, *argc, job_name="{workflow}.reader{n}", **kwargs):
        return self.add_job(*argc, job_name=job_name, assert_n_writer_ge=1, **kwargs)

    def run(self, dry_run=False):
        # shared plumbing between main and sub-workflows
        for sub in self._subs:
            for name, jobnames in sub._fifo_readers.items():
                self._fifo_readers[name].extend(jobnames)

            for name, jobnames in sub._fifo_writers.items():
                self._fifo_writers[name].extend(jobnames)

            for name, bal in sub._fifo_balance.items():
                self._fifo_balance[name] += bal

        # gather all named pipes that are required
        pipe_names = self.get_pipe_list()
        self.logger.debug(f"pipe_names={pipe_names}")
        self.check()
        if not dry_run:
            with plumbing.create_named_pipes(
                pipe_names, total_pipe_buffer=self.total_pipe_buffer
            ) as pipes:
                self.pipe_dict.update(pipes)
                # start all processes in reverse data-flow order
                for job in reversed(self._jobs):
                    if type(job) is Workflow:
                        sub = job
                        # we have a sub-workflow!
                        for job in reversed(sub._jobs):
                            job.start()

                        # join jobs in data-flow order
                        for job in sub._jobs:
                            job.join()
                    else:
                        self.logger.debug(f"starting {job}")
                        job.start()

                # join jobs in data-flow order
                for job in self._jobs:
                    self.logger.debug(f"waiting for {job}")
                    job.join()

        # check for exceptions that occurred in child processes
        caught_exc = False
        for jobname, exc in sorted(self.exc_dict.items()):
            for line in exc:
                self.logger.error(f"exception in {jobname}: {line}")
                caught_exc = True

        if caught_exc:
            raise WorkflowError(
                "detected unhandled exceptions in jobs during workflow-execution"
            )

        return self

    def __str__(self):
        # TODO: make this more comprehensive and beautiful
        buf = [f"Workflow({self.name})"]
        for i, job in self._fifo_readers.items():
            buf.append(f"I:{i} -> J:{job}")

        for o, job in self._fifo_writers.items():
            buf.append(f"J:{job} -> O:{o}")

        return "\n".join(buf)

    def gz_reader(
        self,
        *argc,
        job_name="{workflow}.igzip_text_reader{n}",
        func=parts.igzip_reader,
        inputs=["/dev/stdin"],
        output=FIFO("input_text", "wb"),
        **kwargs,
    ):

        return self.add_job(
            *argc,
            job_name=job_name,
            assert_n_writer_ge=1,
            func=func,
            inputs=inputs,
            output=output,
            **kwargs,
        )

    def BAM_reader(
        self,
        *argc,
        job_name="{workflow}.BAM_reader{n}",
        func=parts.bam_reader,
        input="/dev/stdin",
        output=FIFO("input_sam", "w"),
        threads=2,
        _manage_fifos=False,
        **kwargs,
    ):

        return self.add_job(
            *argc,
            job_name=job_name,
            func=func,
            input=input,
            output=output,
            threads=threads,
            assert_n_writer_ge=1,
            _manage_fifos=_manage_fifos,
            **kwargs,
        )

    def distribute(
        self,
        *argc,
        func=parts.distributor,
        header_fifo="",
        header_detect_func=None,
        header_broadcast=False,
        job_name="{workflow}.dist{n}",
        _manage_fifos=False,
        pass_internals=True,
        **kwargs,
    ):

        return self.add_job(
            *argc,
            job_name=job_name,
            func=func,
            assert_n_writer_ge=1,
            assert_n_reader_ge=1,
            _manage_fifos=_manage_fifos,
            pass_internals=pass_internals,
            header_fifo=header_fifo,
            header_detect_func=header_detect_func,
            header_broadcast=header_broadcast,
            **kwargs,
        )

    def workers(self, *argc, n=4, func=None, job_name="{workflow}.worker{n}", **kwargs):
        for i in range(n):
            self.add_job(
                *argc,
                job_name=job_name,
                func=func,
                assert_n_reader_ge=1,
                fifo_name_format={"n": i},
                **kwargs,
            )

        return self

    def collect(
        self,
        *argc,
        func=parts.collector,
        inputs=[],
        output="/dev/stdout",
        header_fifo="",
        job_name="{workflow}.collect{n}",
        _manage_fifos=False,
        pass_internals=True,
        **kwargs,
    ):

        return self.add_job(
            *argc,
            job_name=job_name,
            func=func,
            inputs=inputs,
            output=output,
            assert_n_reader_ge=1,
            _manage_fifos=_manage_fifos,
            pass_internals=pass_internals,
            header_fifo=header_fifo,
            **kwargs,
        )

    def funnel(self, *argc, job_name="{workflow}.funnel{n}", **kwargs):
        # basically an alias for add_job()
        return self.add_job(*argc, job_name=job_name, **kwargs)

    def start(self, dry_run=False):
        # gather all named pipes that are required
        pipe_names = self.get_pipe_list()
        self.logger.debug(f"pipe_names={pipe_names}")
        self.check()
        if not dry_run:
            with plumbing.create_named_pipes(pipe_names) as pipes:
                self.pipe_dict.update(pipes)
                # start all processes in reverse data-flow order
                for job in reversed(self._jobs):
                    self.logger.debug(f"starting {job}")
                    job.start()

        return self

    def join(self):
        # join jobs in data-flow order
        for job in self._jobs:
            self.logger.debug(f"waiting for {job}")
            job.join()

        # check for exceptions that occurred in child processes
        caught_exc = False
        for jobname, exc in sorted(self.exc_dict.items()):
            for line in exc:
                self.logger.error(f"exception in {jobname}: {line}")
                caught_exc = True

        if caught_exc:
            raise WorkflowError(
                "detected unhandled exceptions in jobs during workflow-execution"
            )

        return self
