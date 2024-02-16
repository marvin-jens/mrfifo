from .contrib import *

import logging
from functools import wraps
from collections import defaultdict

#from .fast_loops import *
from . import parallel
from . import plumbing
from . import parts
from . import util
#from contextlib import contextmanager


class FIFO():
    def __init__(self, name, mode, n=None):
        self.name = name
        self.mode = mode
        self.n = n
        self.file_objects = []

        assert mode[0] in ['r', 'w']
    
    def _format_name(self, context):
        return FIFO(self.name.format(**context), self.mode, n=self.n)

    def is_collection(self):
        return self.n is not None

    def is_reader(self):
        return self.mode[0] == 'r'
    
    def get_names(self, **kw):
        if self.is_collection():
            return [self.name.format(n=i, **kw) for i in range(self.n)]
        else:
            return [self.name,]
    
    def open(self, pipe_dict, manage_fifos=True, **kw):
        from mrfifo.plumbing import open_named_pipe
        if manage_fifos:
            self.file_objects = [
                open_named_pipe(
                    pipe_dict[name], 
                    mode=self.mode
                )
                for name in self.get_names(**kw)
            ]

            if self.is_collection():
                return self.file_objects
            else:
                return self.file_objects[0]
        else:
            # unmanaged, return paths to fifos instead of open files
            paths = [pipe_dict[name] for name in self.get_names(**kw)]
            if self.is_collection():
                return paths
            else:
                return paths[0]


    def close(self):
        for f in self.file_objects:
            f.flush()
            f.close()

    def __repr__(self):
        return f"FIFO({self.mode} names={self.get_names()}) n={self.n}"

from functools import wraps
def Job_decorator(f, pass_internals=False, fifo_name_format={}, _manage_fifos=True, **kwargs):

    # print("JOB DECORATOR", f)
    fifo_vars = {}
    kw = {}
    for k, v in kwargs.items():
        # print(f"{k} : {v} {type(v)}")
        if type(v) is FIFO:
            if fifo_name_format:
                v = v._format_name(fifo_name_format)
            fifo_vars[k] = v
        else:
            kw[k] = v

    @wraps(f)
    def wrapper(result_dict={}, pipe_dict={}, job_name="", args=(), **kwds):
        kwargs = kw.copy()
        kwargs.update(**kwds)
        
        for target, fifo in fifo_vars.items():
            kwargs[target] = fifo.open(pipe_dict, manage_fifos=_manage_fifos)

        try:
            with parallel.ExceptionLogging(job_name) as el:
                if pass_internals:
                    kwargs['_job_name'] = job_name
                    kwargs['_exception_logger'] = el

                res = f(*args, **kwargs)
                result_dict[job_name] = res

        finally:
            for fifo in fifo_vars.values():
                fifo.close()

    
    return wrapper, fifo_vars


class Job():
    def __init__(self, func, result_dict={}, pipe_dict={}, name="job"):
        self.name = name
        self.func = func
        # self.argc = argc
        # self.kwargs = kwargs
        self.result_dict = result_dict
        self.pipe_dict = pipe_dict
        self.p = None

    def create(self):
        kwargs = dict()
        # kwargs['inputs'] = [pipes.get(inp, inp) for inp in self.inputs]
        # kwargs['outputs'] = [pipes.get(outp, outp) for outp in self.outputs]
        kwargs['pipe_dict'] = self.pipe_dict
        kwargs['result_dict'] = self.result_dict
        kwargs['job_name'] = self.name
        # print(f"  Job.create({self.name})")
        # print(f"\tmapped inputs to {kwargs['inputs']}")
        # print(f"\tmapped outputs to {kwargs['outputs']}")
        import multiprocessing as mp
        return mp.Process(target=self.func, kwargs=kwargs)

    def start(self): #, pipes):
        assert self.p is None
        self.p = self.create() #pipes)
        self.p.start()
    
    def join(self):
        assert self.p is not None
        self.p.join()
    
    def __str__(self):
        return f"Job({self.name}) func={self.func.__name__}"
        

class Workflow():
    def __init__(self, name, n=4):
        self.name = name
        self.n = n
        self.logger = logging.getLogger(self.name)
        self.job_count_by_pattern = defaultdict(int)

        self._jobs = []

        self._fifo_readers = defaultdict(list)
        self._fifo_writers = defaultdict(list)
        self._fifo_balance = defaultdict(int)
        
        import multiprocessing as mp
        self.manager = mp.Manager()
        self.result_dict = self.manager.dict()
        self.pipe_dict = self.manager.dict()

    def register_fifos(self, fifos, job_name):
        n_readers = 0
        n_writers = 0
        for f in fifos:
            for name in f.get_names():
                if f.is_reader():
                    self._fifo_balance[name] += 1
                    self._fifo_readers[name].append(job_name)
                    n_readers += 1
                else:
                    self._fifo_balance[name] -= 1
                    self._fifo_writers[name].append(job_name)
                    n_writers += 1
        
        return n_readers, n_writers

    def check(self):
        unbalanced = False
        for name, bal in self._fifo_balance.items():
            if bal > 0:
                self.logger.error(f"fifo {name} has a reader but no writer! (balance={bal})")
                unbalanced = name
            elif bal < 0:
                self.logger.error(f"fifo {name} has a writer but no reader! (balance={bal})")
                unbalanced = name
        
        if unbalanced: 
            raise ValueError(f"workflow '{self.name}' has deadlocking fifo '{name}'")
        
    def get_pipe_list(self):
        return sorted(self._fifo_balance.keys())

    def render_job_name(self, job_name):
        n = self.job_count_by_pattern[job_name]
        self.job_count_by_pattern[job_name] += 1

        return job_name.format(workflow=self.name, n=n)

    def add_job(self, *argc, func=None, job_name="{workflow}.job{n}", assert_n_reader_ge=None, assert_n_writer_ge=None, **kwargs):
        assert func is not None
        job_name = self.render_job_name(job_name)

        job_func, fifo_vars = Job_decorator(func, *argc, **kwargs)
        job = Job(job_func, result_dict = self.result_dict, pipe_dict=self.pipe_dict,
                  name=job_name)

        n_readers, n_writers = self.register_fifos(fifo_vars.values(), job_name=job_name)
        self.logger.debug(f"add_job job={job} n_readers={n_readers} n_writers={n_writers} fifo_vars={fifo_vars}")
        self._jobs.append(job)

        if assert_n_reader_ge:
            assert n_readers >= assert_n_reader_ge
        if assert_n_writer_ge:
            assert_n_writer_ge >= assert_n_writer_ge

        return self

    # presets/short-hands for more readable workflow compositions
    def reader(self, *argc, job_name="{workflow}.reader{n}", **kwargs):

        return self.add_job(*argc, job_name=job_name, 
                            assert_n_writer_ge=1,
                            **kwargs)

    def gz_reader(self, *argc, job_name="{workflow}.igzip_text_reader{n}",
                         func=parts.igzip_reader, input_files=["/dev/stdin"], 
                         out=FIFO("input_text", "wb"), **kwargs):

        return self.add_job(*argc, job_name=job_name, 
                            assert_n_writer_ge=1,
                            func=func,
                            input_files=input_files,
                            out=out,
                            **kwargs)

    def BAM_reader(self, *argc, job_name="{workflow}.BAM_reader{n}",
                   func=parts.bam_reader, bam_name="/dev/stdin", out=FIFO("input_sam", "w"), 
                   threads=2, **kwargs):

        return self.add_job(*argc, job_name=job_name, 
                            func=func,
                            bam_name=bam_name,
                            out=out,
                            threads=threads,
                            assert_n_writer_ge=1, 
                            _manage_fifos=_manage_fifos,
                            **kwargs)

    def distribute(self, *argc, func=parts.distributor, 
                   header_fifo="", header_detect_func=None, header_broadcast=False,
                   job_name="{workflow}.dist{n}", _manage_fifos=False, **kwargs):

        return self.add_job(*argc, job_name=job_name, 
                            func=func, 
                            assert_n_writer_ge=1, 
                            assert_n_reader_ge=1, 
                            _manage_fifos=_manage_fifos,
                            header_fifo=header_fifo, 
                            header_detect_func=header_detect_func,
                            header_broadcast=header_broadcast,
                            **kwargs)

    def workers(self, *argc, n=4, func=None, job_name="{workflow}.worker{n}", **kwargs):
        for i in range(n):
            self.add_job(*argc, job_name=job_name,
                         func=func,
                         assert_n_reader_ge=1, 
                         fifo_name_format={'n' : i},
                         **kwargs)

        return self

    def collect(self, *argc, func=parts.collector, 
                   inputs=[], out="/dev/stdout", header_fifo="",
                   job_name="{workflow}.dist{n}", _manage_fifos=False, **kwargs):

        return self.add_job(*argc, job_name=job_name, 
                            func=func,
                            inputs=inputs,
                            out=out, 
                            assert_n_writer_ge=1, 
                            assert_n_reader_ge=1, 
                            _manage_fifos=_manage_fifos,
                            header_fifo=header_fifo, 
                            **kwargs)


    # def collect(self, *argc, input_pattern="", output="", output_is_file=False, func=None, job_name="{workflow}.collect{n}", n=4, **kwargs):
    #     job = Job(func, [input_pattern.format(n=i) for i in range(n)], [output], argc=argc, kwargs=kwargs,
    #               name=self.render_job_name(job_name))

    #     self.register_job_inputs(job)
    #     if not output_is_file:
    #         self.register_job_outputs(job)

    #     self._jobs.append(job)

    #     return self
    
    # def funnel(self, *argc, inputs="", output="", func=None, job_name="{workflow}.funnel{n}", output_is_file=True, **kwargs):
    #     job = Job(func, inputs, [output], argc=argc, kwargs=kwargs, name=self.render_job_name(job_name))
    #     self.register_job_inputs(job)

    #     if not output_is_file:
    #         self.register_job_outputs(job)

    #     self._jobs.append(job)
    #     return self

    def run(self, dry_run=False):
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

                # join jobs in data-flow order
                for job in self._jobs:
                    self.logger.debug(f"waiting for {job}")
                    job.join()

        return self

    def __str__(self):
        # TODO: make this more comprehensive and beautiful
        buf = [f"Workflow({self.name})"]
        for i, job in self._fifo_readers.items():
            buf.append(f"I:{i} -> J:{job}")
        
        for o, job in self._fifo_writers.items():
            buf.append(f"J:{job} -> O:{o}")
        
        return "\n".join(buf)
    
