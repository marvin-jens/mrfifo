import pytest
import mrfifo as mf

import multiprocessing as mp

def line_counter(finput):
    n = 0
    for line in open(finput):
        n += 1

    return n


def test_dist():
    input_files = ["../spacemake/test_data/reads_chr22_R1.fastq.gz"]
    # TODO: create_named_pipes dict support
    with mf.plumbing.create_named_pipes(["fq_in", "w0", "w1", "w2", "w3", "w4"]) as pipe_paths:
        igzip_pipe = pipe_paths[0]
        worker_inputs = pipe_paths[1:]

        p_gzip = mp.Process(target=mf.parts.igzip_reader, args=(input_files, igzip_pipe,))
        p_dist = mp.Process(target=mf.parts.distributor_main, args=(igzip_pipe, worker_inputs), kwargs=dict(chunk_size=1))
        p_workers = [mp.Process(target=line_counter, args=(wi,)) for wi in worker_inputs]

        for w in p_workers:
            w.start()
        
        p_dist.start()
        p_gzip.start()

        # close down in reverse order
        p_gzip.join()
        p_dist.join()

        for w in p_workers:
            w.join()


if __name__ == "__main__":
    test_dist()