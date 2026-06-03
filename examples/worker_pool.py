"""Worker pool — a fixed set of goroutines draining a job channel.

The bread-and-butter concurrency pattern: bounded parallelism over a
stream of work.  N workers each `for job in jobs` (which ranges until
the channel is closed); the main goroutine feeds jobs, closes the
channel to signal "no more", and collects results.

Run:
    python3 examples/worker_pool.py
"""

import runloom

NUM_WORKERS = 4
NUM_JOBS = 20

def worker(wid, jobs, results):
    for job in jobs:                   # stops when `jobs` is closed
        results.send((wid, job, job * job))

def main():
    jobs = runloom.Chan(NUM_JOBS)
    results = runloom.Chan(NUM_JOBS)

    for wid in range(NUM_WORKERS):
        runloom.go(worker, wid, jobs, results)

    for n in range(1, NUM_JOBS + 1):
        jobs.send(n)
    jobs.close()                       # workers' for-loops will end

    for _ in range(NUM_JOBS):
        wid, job, square = results.recv()[0]
        print("worker {0}: {1}^2 = {2}".format(wid, job, square))

if __name__ == "__main__":
    runloom.run(main)
