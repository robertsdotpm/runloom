"""Contract: BaseEventLoop.close() while the loop is running raises
RuntimeError("Cannot close a running event loop")."""
import sys, asyncio
import runloom.aio as aio

loop = aio.RunloomEventLoop()
asyncio.set_event_loop(loop)
out = {}

async def main():
    try:
        loop.close()
        out["closed"] = True
    except RuntimeError as e:
        out["raised"] = str(e)
    return 1

try:
    r = loop.run_until_complete(main())
    print("run returned", r, out)
except BaseException as e:
    print("run raised %r" % (e,), out)

if "raised" not in out:
    print("BUG: close() of a running loop did not raise RuntimeError "
          "(stock asyncio raises 'Cannot close a running event loop')")
    sys.exit(1)
print("OK")
