"""Context — cancellation that fans out to every goroutine.

runloom.context mirrors Go's context.Context.  WithCancel returns a
context plus a cancel() function; calling cancel() closes ctx.done,
which wakes *every* goroutine select-ing on it at once (a closed
channel never blocks a receive).  WithTimeout / WithDeadline cancel
automatically, and cancellation is transitive to child contexts.

Run:
    python3 examples/context_cancel.py
"""

import os

import runloom

# Free-threaded build: fan goroutines across all cores (M:N scheduler).
HUBS = os.cpu_count() or 4

def worker(ctx, work, wid):
    while True:
        idx, payload = runloom.select([
            ("recv", ctx.done),           # case 0: cancelled
            ("recv", work),               # case 1: a job to do
        ])
        if idx == 0:
            print("worker {0} stopping: {1}".format(wid, ctx.err()))
            return
        print("worker {0} did job {1}".format(wid, payload[0]))

def main():
    ctx, cancel = runloom.context.WithCancel(runloom.context.Background())
    work = runloom.Chan()               # unbuffered

    for wid in range(2):
        runloom.go(worker, ctx, work, wid)

    for job in range(4):
        work.send(job)                    # rendezvous with a free worker

    print("main: cancelling")
    cancel()                              # closes ctx.done -> wakes both workers
    runloom.sleep(0.02)                      # let them observe the cancellation

if __name__ == "__main__":
    runloom.run(HUBS, main)
