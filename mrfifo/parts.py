from .contrib import *
from .parallel import ExceptionLogging
from .plumbing import open_named_pipe

def igzip_reader(inputs, outputs, name='mrfifo.igzip_reader'):
    assert len(outputs) == 1
    pipe = outputs.pop()

    with ExceptionLogging(name) as el:
        el.logger.info(f"writing to {pipe}")
        from isal import igzip
        out_file = open_named_pipe(pipe, mode='wb')

        try:
            for fname in inputs:
                el.logger.info(f"reading from {fname}")
                in_file = igzip.IGzipFile(fname, 'r')
                while True:
                    block = in_file.read(igzip.READ_BUFFER_SIZE)
                    if block == b"":
                        break

                    out_file.write(block)
                
                in_file.close()
        finally:
            el.logger.info(f"closing down {pipe}")
            out_file.close()

def bam_reader(inputs, outputs, threads=2, name="mrfifo.parts.bam_reader"):
    assert len(outputs) == 1
    pipe = outputs.pop()
    assert len(inputs) == 1
    input_file = inputs.pop()
    
    import os
    with ExceptionLogging(name) as el:
        os.system(f'samtools view -Sh --no-PG --threads={threads} {input_file} > {pipe}')

def distributor(inputs, outputs, name="mrfifo.parts.distributor", **kw):
    assert len(inputs) == 1
    in_pipe = inputs.pop()
    with ExceptionLogging(name) as el:
        el.logger.info(f"reading from {in_pipe}, writing to {outputs} kw={kw}")
        from .fast_loops import distribute
        distribute(in_pipe, outputs, **kw)

def collector(inputs, outputs, name="mrfifo.parts.collector", **kw):
    assert len(inputs) > 0
    assert len(outputs) == 1
    out = outputs.pop()

    with ExceptionLogging(name) as el:
        el.logger.info(f"collecting from '{inputs}', writing to '{out}' kw={kw}")
        from .fast_loops import collect
        collect(inputs, out, **kw)
        el.logger.info("collection complete")

def serializer(inputs, outputs, name="mrfifo.parts.serializser", **kw):
    assert len(inputs) > 0
    assert len(outputs) == 1
    fout = outputs.pop()

    with ExceptionLogging(name) as el:
        el.logger.debug(f"writing to '{fout}'")
        with open(fout, 'w') as out:
            for fin in inputs:
                el.logger.debug(f"reading from '{fin}'")
                f = open(fin)
                for line in f:
                    # el.logger.debug(f"got line {line}")
                    out.write(line)
                f.close()

