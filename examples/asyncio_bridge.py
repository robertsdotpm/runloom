"""asyncio bridge — run existing async/await code on runloom's scheduler.

Already have async def code?  runloom.aio.run(coro) is a drop-in for
asyncio.run that drives each Task on a runloom goroutine.  Standard
asyncio building blocks — gather, sleep, wait_for, Queue, Lock — work
unchanged; runloom just provides the event loop underneath.

Run:
    python3 examples/asyncio_bridge.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import runloom.aio as paio


async def fetch(name, delay):
    await asyncio.sleep(delay)            # cooperative; other tasks run
    return "{0} done after {1}s".format(name, delay)


async def worker(name, queue, results):
    while True:
        item = await queue.get()
        if item is None:                 # poison pill -> shut down
            queue.task_done()
            return
        await asyncio.sleep(0.01)
        results.append("{0} handled {1}".format(name, item))
        queue.task_done()


async def main():
    # 1) Fan out with gather and race a slow one against a timeout.
    results = await asyncio.gather(
        fetch("a", 0.05),
        fetch("b", 0.02),
        fetch("c", 0.03),
    )
    for line in results:
        print(line)

    try:
        await asyncio.wait_for(fetch("slow", 0.5), timeout=0.1)
    except asyncio.TimeoutError:
        print("slow fetch timed out (as expected)")

    # 2) A queue + a couple of consumer tasks.
    queue = asyncio.Queue()
    collected = []
    consumers = [asyncio.create_task(worker("w{0}".format(i), queue, collected))
                 for i in range(2)]
    for item in range(6):
        await queue.put(item)
    await queue.join()
    for _ in consumers:
        await queue.put(None)
    await asyncio.gather(*consumers)
    print("queue results:", sorted(collected))


if __name__ == "__main__":
    paio.run(main())
