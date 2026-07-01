"""Split r13: (a) awaiting self deadlocks instead of RuntimeError;
(b) cancel() of a self-awaiting task recurses infinitely."""
import sys, asyncio
import runloom.aio as aio

which = sys.argv[1] if len(sys.argv) > 1 else "a"

async def main():
    holder = {}
    async def selfwait():
        return await holder["t"]
    holder["t"] = asyncio.get_event_loop().create_task(selfwait())
    try:
        await asyncio.wait_for(asyncio.shield(holder["t"]), 1)
        return "completed"
    except asyncio.TimeoutError:
        if which == "b":
            try:
                holder["t"].cancel()      # suspected infinite recursion
                return "deadlock+cancel-ok"
            except RecursionError:
                return "deadlock+cancel-RECURSIONERROR"
        return "deadlock"
    except RuntimeError as e:
        return "runtimeerror: %s" % e

print(aio.run(main()))
