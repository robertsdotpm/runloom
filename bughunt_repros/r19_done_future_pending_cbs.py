"""run_until_complete(already-done future): stock asyncio still runs the loop
one iteration, so previously-scheduled call_soon callbacks execute."""
import sys, asyncio
import runloom.aio as aio

loop = aio.RunloomEventLoop()
asyncio.set_event_loop(loop)
ran = []
loop.call_soon(ran.append, 1)
fut = loop.create_future()
fut.set_result(7)
r = loop.run_until_complete(fut)
print("result:", r, "callbacks ran:", ran)
if not ran:
    print("DIVERGENCE: pending call_soon callbacks not run when the awaited "
          "future was already done (stock runs them)")
    sys.exit(1)
print("OK")
