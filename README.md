# Mr. FIFO

[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)
[![PyPI](https://badge.fury.io/py/mrfifo.svg)](https://badge.fury.io/py/mrfifo)
![PyPI - License](https://img.shields.io/pypi/l/mrfifo)
[![Coverage Status](https://coveralls.io/repos/github/marvin-jens/mrfifo/badge.svg?branch=main)](https://coveralls.io/github/marvin-jens/mrfifo?branch=main)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Mr. FIFO is a python/cython package that builds on top of the `multiprocessing` module, providing high throughput and very low-overhead, directional, point-to-point interprocess communication. It essentially provides _Map-Reduce parallelism over FIFOs_ (aka named pipes).

# Abstract

Some problems can very nicely be implemented in python and are easily parallelizable, but require high throughput. The python multiprocessing module primitives (Queue, Pipe, etc.) however suffer from significant overhead (pickling/un-pickling etc.) and can become a severe bottleneck.
Mr. FIFO provides few very low overhead function primitives (implemented in Cython) that exploit the time-tested and extremely optimized inter-process communication framework known as *named pipes* or FIFOs (first-in first-out). The development is heavily geared towards bioinformatics tasks (processing large BAM files for instance), but is in no way restricted to that.

# Install

```
   pip install mrfifo
```

should do the trick. 

# Example workflow

Here's an instructive example that shows off several of the main features. The following code decompresses a `fastq.gz` file, distributes the FASTQ lines to four separate worker processes running in parallel. These workers execute `line_counter` which does nothing but count the lines, and report the count by *writing it to an output* file. Finally, the outputs are `collected` and passed on to `add_counts`, a second bit of user-defined code that adds up the counts to arrive at a grand total.

```python

    def pass_through(input, output):
        i = 0
        for line in input:
            i += 1
            output.write(line)

        return i

    def is_header(line):
        return line.startswith("@")

    def test_bam_reconstruct(chunk_size=1, n=4):
        import mrfifo as mf
        w = (
            mf.Workflow("BAM_reconstruct")
            .BAM_reader(input="test_data/tiny_test.bam")
            .distribute(
                input=mf.FIFO("input_sam", "rt"),
                outputs=mf.FIFO("dist{n}", "wt", n=n),
                chunk_size=chunk_size,
                header_detect_func=is_header,
                header_broadcast=False,
                header_fifo=mf.FIFO("header", "wt"),
            )
            .workers(
                input=mf.FIFO("dist{n}", "rt"),
                output=mf.FIFO("out{n}", "wt"),
                func=pass_through,
                n=n,
            )
            .collect(
                inputs=mf.FIFO("out{n}", "rt", n=n),
                header_fifo=mf.FIFO("header", "rt"),
                output=mf.FIFO("out_sam", "wt"),
                chunk_size=chunk_size,
            )
            .funnel(
                input=mf.FIFO("out_sam", "rt"),
                output="test_data/reconstruct.bam",
                _manage_fifos=False,
                func=mf.parts.bam_writer,
            )
            .run()
        )
```

Please note that we assign the arguments of `pass_through` to `mf.FIFO` instances. This tells mrfifo that input and output are considered *internal plumbing* between the various parts executed in parallel. As such, Mr.FIFO will actually create named pipes in a temporary directory for each connection between parts. Unless explicitly turned of with `manage_pipes=False`, Mr. FIFO will even open and close the named pipes to avoid deadlocks in case of any exceptions.

The example above actually creates a number of sub-processes:

 * `BAM_reconstruct.bam_reader0`: reads a BAM file from `test_data/tiny_test.bam` and decompresses to the named pipe `input_sam`
 * `BAM_reconstruct.dist0`: reads line-by-line from `input_sam` and distributes in a round-robin fashion to
 four named pipes `['dist0', 'dist1', 'dist2', 'dist3']`. The BAM header is separately sent to its own FIFO named `header`.
 * `BAM_reconstruct.worker{i}`: four processes, each reading from `dist{i}` and writing to `out{i}` where `i` goes from 0 to 3.
 * `BAM_reconstruct.collect0`: reads line-by-line, round-robin from `['counted0', ..., 'counted3']` and writes the line to `out_sam`. But not before passing on the contents of the `header`!
 * `BAM_reconstruct.funnel0`: sends the content of `out_sam` to a `samtools view` process which re-creates a compressed BAM file with the identical original context.

 # API

 Under the hood, the workflow creates `mrfifo.Job` instances for each process. These are thin wrappers around `mutliprocessing.Process`, which allow to keep track of the inputs and outputs, collect return values and unhandled exceptions. `func` is a callable that must not especially be prepared for mrfifo. In general, it should probably have an argument that can be iterated over (line-wise) so that we can connect it to a FIFO. The API to compose the workflow consists of useful shorthands to create meaningfully different jobs. But there may be different ways to combine code with the offered primitives. The currently proposed logic is as follows:

 ## .reader

 Readers sequentially process one or more input files and write to a single output. In the above example we use a as input, but we could also have provided `/dev/stdin`. Typically, the entry-point to your workflow will be defined by one or more `.reader()` calls.

 ## .distribute

A distributor splits an input into multiple outputs by cycling through the outputs as it iterates over chunks of input. The default distributor is implemented in cython, tries to allocate large pipe buffers and has low overhead.
For more complex input streams, it offers detection and separate treatment of a header. The header can either be broadcast to each of the outputs, or sent to a dedicated output that is exclusively used for the header (see the documentation). The code also supports very basic pattern matching and an output lookup mechanism which allows, for example, to split BAM records by the first few bases of the Unique Molecular Identifier (UMI) or Cell Barcode (CB). This is very useful if the downstream workers, *running in parallel* need to ensure that records with the same UMI or CB are always sent to the same worker.

## .workers

This is the only API function that creates multiple jobs from one call. inputs and outputs are expanded for each worker using the `n` variable in the inputs and outputs argument. Note that the input FIFO contains a wildcard that will be replaced with the appropriate number for each worker. This allows to route multiple inputs (from multiple distributors) into workers while keeping the input streams in sync.

## .collect

Collectors do the opposite of distributors. They read round-robin from multiple inputs and write to a single output. This output can be a file and may represent the output of your workflow. It can of course also be a FIFO.

## .funnel

A funnel is just a job with a single output, but possibly multiple inputs. Any kind of processing may fit in here. Here, for example, we re-compress already collected output.

## w.results_dict

Note that after a workflow is complete, you can find the return values of the functions assigned to subprocesses in the dictionary `w.results_dict[<job-name>]`. Similarly, exception tracebacks can be retrieved from `w.exc_dict`.






