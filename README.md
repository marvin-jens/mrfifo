# MR.FIFO
Map-Reduce parallelism over FIFOs (named pipes)

# Abstract

Some problems in bioinformatics are easily parallelizable but require high throughput. The multiprocessing module primitives (Queue, Pipe, etc.) however suffer from significant overhead and can become a bottleneck.
MRFIFO provides a few very low overhead function primitives (implemented in Cython) that exploit the time-tested and extremely optimized inter-process communication framework known as `named pipes` or FIFOs (first-in first-out).

