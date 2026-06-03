"""WaitGroup — wait for a batch of goroutines to finish.

Go's sync.WaitGroup in ~10 lines on top of a channel: add(n) records
how many goroutines you launched, each calls done() when it finishes,
and wait() blocks until that many have reported in.  A buffered channel
is all the bookkeeping you need.

Run:
    python3 examples/waitgroup.py
"""

import runloom

class WaitGroup(object):
    """Minimal sync.WaitGroup built on a channel."""

    def __init__(self):
        self.pending = runloom.Chan(1024)
        self.total = 0

    def add(self, n):
        self.total += n

    def done(self):
        self.pending.send(None)

    def wait(self):
        for _ in range(self.total):
            self.pending.recv()

def task(wg, tid):
    try:
        runloom.sleep(0.01 * (tid + 1))
        print("task {0} finished".format(tid))
    finally:
        wg.done()                         # always report, even on error

def main():
    wg = WaitGroup()
    num_tasks = 5
    wg.add(num_tasks)
    for tid in range(num_tasks):
        runloom.go(task, wg, tid)
    wg.wait()
    print("all {0} tasks done".format(num_tasks))

if __name__ == "__main__":
    runloom.run(main)
