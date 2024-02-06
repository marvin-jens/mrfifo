# Mr. FIFO
Map-Reduce parallelism over FIFOs (named pipes)

# Abstract

Some problems in bioinformatics can very nicely be implemented in python, are easily parallelizable, but require high throughput. The python multiprocessing module primitives (Queue, Pipe, etc.) however suffer from significant overhead and can become a severe bottleneck.
Mr. FIFO provides few very low overhead function primitives (implemented in Cython) that exploit the time-tested and extremely optimized inter-process communication framework known as *named pipes* or FIFOs (first-in first-out).

# Install

This will eventually be pushed to PyPI, so

```
   pip install mrfifo
```

should do the trick. For the meantime, please check-out the git repo and run

```
   pip install .
```

# Example workflow

Here's an instructive example that shows off several of the main features. The following code decompresses a `fastq.gz` file, distributes the FASTQ lines to four separate worker processes running in parallel. These workers execute `line_counter` which does nothing but count the lines, and report the count by *writing it to an output* file. Finally, the outputs are `collected` and passed on to `add_counts`, a second bit of user-defined code that adds up the counts to arrive at a grand total.

```python

    def line_counter(inputs=[], outputs=[], name="counter"):
        assert len(inputs) == 1
        assert len(outputs) == 1
        finput = inputs.pop()
        fout = outputs.pop()

        n = 0
        for line in open(finput):
            n += 1
    
        open(fout, 'wt').write(f"results {name} {n}\n")

    def add_counts(inputs=[], outputs=[], name="adder"):
        assert len(outputs) == 1
        assert len(inputs) == 1
        finput = inputs.pop()
        fout = outputs.pop()

        N = 0
        for line in open(finput):
            _, name, n = line.split(' ')
            N += int(n)
        
        open(fout, "w").write(f"total_count {N}\n")

    import mrfifo as mf
    w = (
        mf.Workflow('fastq.gz.line_counter')
        .reader(
            inputs = ["/dev/stdin"],
            inputs_are_files=True,
            func=mf.parts.igzip_reader,
            outputs=["decompressed"],
        )
        .distribute(
            input="decompressed",
            output_pattern="dist{n}",
            n=4, chunk_size=1,
        )
        .workers(
            inputs=["dist{n}"],
            outputs=["counted{n}"],
            func=line_counter,
            n=4,
        )
        .collect(
            input_pattern="counted{n}",
            output="merged",
            output_is_file=False,
            n=4, chunk_size=1,
            func=mf.parts.collector,
        )
        .funnel(
            inputs=["merged"],
            output="/dev/stdout",
            output_is_file=True,
            func=add_counts,
        )
        .run()
    )
```

Please note that by default all inputs and outputs are considered *internal plumbing* between the various parts executed in parallel. As such, Mr.FIFO will actually create named pipes in a temporary directory for each connection between parts, unless the link is explicitly marked as a file by the use of `output_is_file=True` (for `.collect` or `.funnel`) or `inputs_are_files=True` (for `.reader`).

The example above actually creates a number of sub-processes:

 * `fastq.gz.line_counter.reader0`: reads fastq.gz from `/dev/stdin` and decompresses to the named pipe `decompressed`
 * `fastq.gz.line_counter.dist0`: reads line-by-line from `decompressed` and distributes in a round-robin fashion to
 four named pipes `['dist0', 'dist1', 'dist2', 'dist3']`. This is obviously not the most useful way to distribute FASTQ records, which each consist of four consecutive lines. But here we only want to count lines (hint: set the `chunk_size` argument to any multiple of four and you're good).
 * `fastq.gz.line_counter.worker{i}`: four processes, each reading from `dist{i}` and writing to `counted{i}` where `i` goes from 0 to 3.
 * `fastq.gz.line_counter.collect0`: reads line-by-line, round-robin from `['counted0', ..., 'counted3']` and writes the line to `merged` (again, use `chunk_size` to preserve the integrity of multi-line records).
 * `fastq.gz.funnel0`: reads line-by-line from `merged` and adds up the counts. Finally, the end-result is written to `/dev/stdout`.

 # API

 Under the hood, the workflow creates `mrfifo.Job` instances for each process. These are thin wrappers around `mutliprocessing.Process`, which allow to keep track of the inputs and outputs. `func` is a callable that must at least accept the following named arguments `inputs:Sequence, outputs:Sequence` and `name:str`. Everything else is passed via `argc` and `kwargs`. The API to compose the workflow consists of useful shorthands to create meaningfully different jobs. But there may be different ways to combine code with the offered primitives. The currently proposrf logic is as follows:

 ## .reader

 Readers sequentially process one or more input files and write to a single output. In the above example we use `sys.stdin` as input, but we could also have provided a list of fastq.gz files, say from `sys.argv[1:]`. Typically, the entry-point to your workflow will be defined by one or more `.reader()` calls.

 ## .distribute

A distributor splits an input into multiple outputs by cycling through the outputs as it iterates over chunks of input. The default distributor is implemented in cython, tries to allocate large pipe buffers and has low overhead.
For more complex input streams, it offers detection and separate treatment of a header. The header can either be broadcast to each of the outputs, or sent to a dedicated output that is exclusively used for the header (see the documentation). The code also supports very basic pattern matching and an output lookup mechanism which allows, for example, to split BAM records by the first few bases of the Unique Molecular Identifier (UMI) or Cell Barcode (CB). This is very useful if the downstream workers, *running in parallel* need to ensure that records with the same UMI or CB are always sent to the same worker.

## .workers

This is the only API function that creates multiple jobs from one call. inputs and outputs are expanded for each worker using the `n` variable in the inputs and outputs argument. If you provide a list of inputs, this list will be replaced with the appropriate list for each worker. For example `inputs=['reads1.dist{n}', 'reads2.dist{n}']` will be expanded to `['reads1.dist0', 'reads2.dist0]` for `worker0` *etc.*. This allows to route multiple inputs into workers but keeping the inputs in sync.

## .collect

Collectors do the opposite of distributors. They read round-robin from multiple inputs and write to a single output. This output can be a file and may represent the output of your workflow. It can also be a pipe though.

## .funnel

A funnel is simpler than a collector in the sense that if you call it with multiple inputs, it is expected to sequentially read them from beginning to end, analogous to how .reader can read multiple files - one after the other. A funnel may be very useful with just a single input, for example for re-compression of already collected output.






