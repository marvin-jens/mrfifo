import mrfifo
from contextlib import contextmanager
from functools import wraps
from mrfifo.parallel import ExceptionLogging


@contextmanager
def input_fifo(path):
    from mrfifo.plumbing import open_named_pipe
    p = open_named_pipe(path, mode="rt+")
    try:
        yield p
    finally:
        p.close()

@contextmanager
def output_fifo(path):
    from mrfifo.plumbing import open_named_pipe
    p = open_named_pipe(path, mode="wt+")
    try:
        yield p
    finally:
        p.close()


class FIFO():
    def __init__(self, name, mode, n=None):
        self.name = name
        self.mode = mode
        self.n = n
        self.file_objects = []

        assert mode in ['r', 'w']
    
    def is_collection(self):
        return "{" in self.name

    def get_names(self, **kw):
        if self.is_collection():
            return [self.name.format(n=i, **kw) for i in range(self.n)]
        else:
            return [self.name,]
    
    def open(self, pipe_dict, **kw):
        from mrfifo.plumbing import open_named_pipe
        self.file_objects = [
            open_named_pipe(
                pipe_dict[name], 
                mode=f"{self.mode}t"
            )
            for name in self.get_names(**kw)
        ]

        if self.is_collection():
            return self.file_objects
        else:
            return self.file_objects[0]

    def close(self):
        for f in self.file_objects:
            f.flush()
            f.close()


from functools import wraps
def Job_decorator(f, pass_internals=False, **kwargs):

    fifo_vars = {}
    kw = {}
    for k, v in kwargs.items():
        if type(v) is FIFO:
            fifo_vars[k] = v
        else:
            kw[k] = v

    @wraps(f)
    def wrapper(result_dict={}, pipe_dict={}, job_name="", args=(), **kwds):
        kwargs = kw.copy()
        kwargs.update(**kwds)
        
        for target, fifo in fifo_vars.items():
            kwargs[target] = fifo.open(pipe_dict)

        try:
            with ExceptionLogging(job_name) as el:
                if pass_internals:
                    kwargs['_job_name'] = job_name
                    kwargs['_exception_logger'] = el

                res = f(*args, **kwargs)
                result_dict[job_name] = res

        finally:
            for fifo in fifo_vars.values():
                fifo.close()

    
    return wrapper


def file_reader(fname, out):
    for line in open(fname):
        out.write(line)

def simple_counter(src):
    i = 0
    for line in src:
        i += 1
    
    return i

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)


    import os
    try:
        os.mkfifo('text_pipe')

        job = Job_decorator(file_reader, fname="test_data/simple.txt", out=FIFO("text", 'w'))
        jobd = Job_decorator(parts.distributor, input=FIFO("text", "r"), outputs=FIFO("text_d{n}", 'w', n=4), output_header=FIFO("header", 'w'))
        job2 = Job_decorator(simple_counter, src=FIFO("text", 'r'))

        import multiprocessing as mp
        manager = mp.Manager()
        result_dict = manager.dict()
        pipe_dict={'text' : 'text_pipe'}

        p1 = mp.Process(target=job, kwargs=dict(result_dict=result_dict, pipe_dict=pipe_dict, job_name="dryrun.reader"))
        p1.start()

        p2 = mp.Process(target=job2, kwargs=dict(result_dict=result_dict, pipe_dict=pipe_dict, job_name="dryrun.counter"))
        p2.start()

        p1.join()
        p2.join()

        for k, v in result_dict.items():
            print(f"{k}\t{v}")

    finally:    
        os.remove('text_pipe')


# def reader(self, func, **kwargs):
#     self.jobs.append(Job_decorator(func, **kwargs))

# def distribute(self, func, **kwargs):
#     self.jobs.append(Job_decorator(func, **kwargs))







# w = (
#     mrfifo.Workflow('dist_counter')
#     .reader(
#         func=mrfifo.parts.igzip_reader, 
#         infiles=['../test_data/simple.txt.gz',], 
#         out=FIFO('uncompressed', 'w'))
#     .distribute(
#         func=mrfifo.parts.distributor,
#         input=FIFO('uncompressed', 'r'),
#         outputs=FIFO('dist{n}', 'w'), n=4)
#     .workers(
#         func=simple_counter,
#         src=FIFO('dist{n}', 'r'), n=4)
#     .run()
#     )




# def test_func(stream, ofs=0, **kw):
#     n = 0
#     for x in stream:
#         n += 1
#     return n + ofs



# _inputs = {'stream' : (open, "test_data/simple.txt")}
# _outputs = {}


# import multiprocessing as mp

# manager = mp.Manager()
# result_dict = manager.dict()


# def r_fifo(name):


# class Fifo():
#     def __init__(self, **kwargs):
#         pass


# def plug(func, **kwargs):




# w = (
#     mf.Workflow('bla')
#     .reader(
#         plug(igzip_reader, out=w_fifo("decomp")), 
#         input_files=["test_data/simple.txt.gz"]
#     )
#     .distribute(input=r_fifo("decomp"), output=w_fifos("dist{i}"), chunk_size=1)
#     .workers(
#         plug(my_worker, infile=r_fifo("dist{i}", out=w_fifo("w_out{i}")))
#     )
#     .collect(
#         input=r_fifos("dist{i}"), output="/dev/stdout", chunk_size=1
#     )
#     .run(n=4)
# )


# p = mp.Process(target=Job_decorator(test_func, _inputs=_inputs, result_dict=result_dict, name="wrapped_test_func"))
# p.start()
# p.join()

# print(result_dict.items())
