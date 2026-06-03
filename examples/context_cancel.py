"""Context — cancellation that fans out to every goroutine.

pygo.context mirrors Go's context.Context.  WithCancel returns a
context plus a cancel() function; calling cancel() closes ctx.done,
which wakes *every* goroutine select-ing on it at once (a closed
channel never blocks a receive).  WithTimeout / WithDeadline cancel
automatically, and cancellation is transitive to child contexts.

Run:
    python3 examples/context_cancel.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo
import pygo.context
import pygo_core


def worker(ctx, work, wid):
    while True:
        idx, payload = pygo_core.select([
            ("recv", ctx.done),           # case 0: cancelled
            ("recv", work),               # case 1: a job to do
        ])
        if idx == 0:
            print("worker {0} stopping: {1}".format(wid, ctx.err()))
            return
        print("worker {0} did job {1}".format(wid, payload[0]))


def main():
    ctx, cancel = pygo.context.WithCancel(pygo.context.Background())
    work = pygo_core.Chan()               # unbuffered

    for wid in range(2):
        pygo.go(worker, ctx, work, wid)

    for job in range(4):
        work.send(job)                    # rendezvous with a free worker

    print("main: cancelling")
    cancel()                              # closes ctx.done -> wakes both workers
    pygo.sleep(0.02)                      # let them observe the cancellation


if __name__ == "__main__":
    pygo.run(main)
