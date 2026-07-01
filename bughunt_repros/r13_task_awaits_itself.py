"""Stock asyncio: a task awaiting itself raises RuntimeError('Task cannot
await on itself'). Runloom: suspected silent deadlock."""
import sys, asyncio
import runloom.aio as aio

async def main():
    holder = {}
    async def selfwait():
        return await holder["t"]
    holder["t"] = asyncio.get_event_loop().create_task(selfwait())
    try:
        await asyncio.wait_for(asyncio.shield(holder["t"]), 2)
        return "completed"
    except asyncio.TimeoutError:
        holder["t"].cancel()
        return "deadlock"
    except RuntimeError as e:
        return "runtimeerror: %s" % e

r = aio.run(main())
print(r)
if r == "deadlock":
    print("BUG: task awaiting itself deadlocks silently (stock raises RuntimeError)")
    sys.exit(1)
print("OK-ish")
