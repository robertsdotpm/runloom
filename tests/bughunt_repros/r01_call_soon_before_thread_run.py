"""Contract: BaseEventLoop.call_soon()/create_task() on a NOT-running loop is
legal from any thread; the work runs when the loop starts (on whatever thread).
Classic pattern: main thread builds loop + schedules initial work, then a
worker thread runs run_forever().
"""
import sys, threading, time, asyncio
import runloom.aio as aio

loop = aio.RunloomEventLoop()
results = []

async def initial_task():
    results.append("task")

loop.call_soon(lambda: results.append("cb"))
t_obj = loop.create_task(initial_task())

t = threading.Thread(target=loop.run_forever, daemon=True)
t.start()
time.sleep(1.0)
loop.call_soon_threadsafe(lambda: None)  # nudge
time.sleep(0.5)
loop.stop()
t.join(timeout=5)
print("results:", results, "task done:", t_obj.done())
if results != ["cb", "task"]:
    print("BUG: scheduled-before-run work was lost (stock asyncio runs both)")
    sys.exit(1)
print("OK")
