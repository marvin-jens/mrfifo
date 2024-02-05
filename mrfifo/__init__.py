from .contrib import *
from .fast_loops import *
from . import parallel
from . import plumbing
from . import parts
from . import util

from collections import defaultdict

class Job():
    def __init__(self, func, inputs=[], outputs=[], argc=(), kwargs={}, name="job"):
        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.func = func
        self.argc = argc
        self.kwargs = kwargs
        self.p = None

    def create(self, pipes):
        kwargs = dict(self.kwargs)
        kwargs['inputs'] = [pipes.get(inp, inp) for inp in self.inputs]
        kwargs['outputs'] = [pipes.get(outp, outp) for outp in self.outputs]
        kwargs['name'] = self.name
        # print(f"  Job.create({self.name})")
        # print(f"\tmapped inputs to {kwargs['inputs']}")
        # print(f"\tmapped outputs to {kwargs['outputs']}")
        import multiprocessing as mp
        return mp.Process(target=self.func, args=self.argc, kwargs=kwargs)

    def start(self, pipes):
        self.p = self.create(pipes)
        self.p.start()
    
    def join(self):
        self.p.join()
    
    def __str__(self):
        return f"Job({self.name}) I:{self.inputs} O:{self.outputs} func={self.func.__name__} argc={self.argc} kwargs={self.kwargs}"
        


class Workflow():
    def __init__(self, name):
        self.name = name
        import logging
        self.logger = logging.getLogger(self.name)
        self.job_count_by_pattern = defaultdict(int)

        self._jobs = []

        self._as_inputs = defaultdict(list)
        self._as_outputs = defaultdict(list)
        self._pipe_counter = defaultdict(int)

    def register_job_inputs(self, job):
        for i in job.inputs:
            self._as_inputs[i].append(job.name)
            self._pipe_counter[i] += 1

    def register_job_outputs(self, job):
        for o in job.outputs:
            self._as_outputs[o].append(job.name)
            self._pipe_counter[o] -= 1

    def get_pipe_list(self):
        return sorted(self._pipe_counter.keys())

    def render_job_name(self, job_name):
        n = self.job_count_by_pattern[job_name]
        self.job_count_by_pattern[job_name] += 1

        return job_name.format(workflow=self.name, n=n)

    def reader(self, *argc, inputs=[], func=None, outputs=[], job_name="{workflow}.reader{n}", inputs_are_files=True, **kwargs):
        job = Job(func, inputs, outputs, argc=argc, kwargs=kwargs, 
                  name=self.render_job_name(job_name))

        if not inputs_are_files:
            self.register_job_inputs(job)

        self.register_job_outputs(job)
        self._jobs.append(job)

        return self

    def distribute(self, *argc, input=None, output_pattern=None, n=4, func=parts.distributor, job_name="{workflow}.dist{n}", **kwargs):
        job = Job(func, [input], [output_pattern.format(n=i) for i in range(n) ], argc=argc, kwargs=kwargs,
                  name=self.render_job_name(job_name))

        self.register_job_inputs(job)
        self.register_job_outputs(job)
        self._jobs.append(job)

        return self

    def workers(self, *argc, inputs=[], outputs=[], n=4, func=None, job_name="{workflow}.worker{n}", **kwargs):
        for i in range(n):
            job = Job(func, 
                      [inp.format(n=i) for inp in inputs] if inputs else [],
                      [outp.format(n=i) for outp in outputs] if outputs else [],
                      argc, kwargs, name=self.render_job_name(job_name))

            self.register_job_inputs(job)
            self.register_job_outputs(job)
            self._jobs.append(job)

        return self
        
    def collect(self, *argc, input_pattern="", output="", output_is_file=False, func=None, job_name="{workflow}.collect{n}", n=4, **kwargs):
        job = Job(func, [input_pattern.format(n=i) for i in range(n)], [output], argc=argc, kwargs=kwargs,
                  name=self.render_job_name(job_name))

        self.register_job_inputs(job)
        if not output_is_file:
            self.register_job_outputs(job)

        self._jobs.append(job)

        return self
    
    def funnel(self, *argc, inputs="", output="", func=None, job_name="{workflow}.funnel{n}", output_is_file=True, **kwargs):
        job = Job(func, inputs, [output], argc=argc, kwargs=kwargs, name=self.render_job_name(job_name))
        self.register_job_inputs(job)

        if not output_is_file:
            self.register_job_outputs(job)

        self._jobs.append(job)
        return self

    def run(self, dry_run=False):
        # gather all named pipes that are required
        pipe_names = self.get_pipe_list()
        self.logger.debug(f"pipe_names={pipe_names}")

        if not dry_run:
            with plumbing.create_named_pipes(pipe_names) as pipes:
                # start all processes in reverse data-flow order 
                for job in reversed(self._jobs):
                    self.logger.debug(f"starting {job}")
                    job.start(pipes)

                # join jobs in data-flow order
                for job in self._jobs:
                    self.logger.debug(f"waiting for {job}")
                    job.join()

    def __str__(self):
        # TODO: make this more comprehensive and beautiful
        buf = [f"Workflow({self.name})"]
        for i, job in self._as_inputs.items():
            buf.append(f"I:{i} -> J:{job}")
        
        for o, job in self._as_outputs.items():
            buf.append(f"J:{job} -> O:{o}")
        
        return "\n".join(buf)
