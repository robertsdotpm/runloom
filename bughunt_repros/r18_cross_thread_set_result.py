"""run_until_complete(fut) where fut is resolved DIRECTLY from a foreign
thread (works under stock asyncio when the loop is otherwise active).
Suspect: _stop_on_done is _runloom_fire_sync and runs on the FOREIGN thread ->
runloom_c.sched_stop() targets the foreign thread's scheduler -> the loop
thread never stops if background fibers keep the scheduler non-empty."""
import sys, threading, time, asyncio
import runloom.aio as aio

loop = aio.RunloomEventLoop()
asyncio.set_event_loop(loop)

async def bg():
    while True:
        await asyncio.sleep(0.05)

fut = loop.create_future()

def foreign():
    time.sleep(0.3)
    fut.set_result("done")     # direct cross-thread completion

t = threading.Thread(target=foreign, daemon=True)

async def main():
    loop.create_task(bg())     # keeps the scheduler busy forever
    t.start()
    return await fut

watchdog_fired = []
def watchdog():
    time.sleep(8)
    watchdog_fired.append(1)
    print("BUG: run_until_complete never returned after cross-thread "
          "set_result (stock asyncio returns)")
    import os
    os._exit(1)
threading.Thread(target=watchdog, daemon=True).start()

r = loop.run_until_complete(main())
print("returned:", r)
print("OK")
