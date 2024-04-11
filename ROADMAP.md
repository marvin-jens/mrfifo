# Roadmap

## v1.0

Must include everything that is needed for SPACEMAKE 2.0 (aka drop-dropseq), but also come with 
sufficient examples & docs to allow others to re-use in other scenarios.

  * expanded documentation (split-by UMI or CB tag)
  * one round of adding doc-strings and auto-generating reference docs (sphinx?)

## v1.x

Minor improvements in usability and documentation.

  * built-in performance/throughput sensors and statistics output to identify bottlenecks
  * better workflow representation (as str but also as graph)
  * improve documentation

## v2.x (or never)

Major new features that may never be needed. Just collecting ideas here.
  * support binary I/O in addition to line-based. Might make sense for BAM blocks but the format is pretty
    hard to break up. Would certainly require hacking HTSlib in ways that make me woozy.
  * allow sockets in addition to named pipes and files, to scale beyond a single node 
  * deeper integration with snakemake (similar concepts in many places)


