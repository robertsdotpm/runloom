import asyncio
import runloom.aio as aio
loop = aio.RunloomEventLoop(); asyncio.set_event_loop(loop)
events = []
async def sibling():
    try:
        await asyncio.sleep(0.2)
        events.append("sibling done")
    except BaseException as e:
        events.append("sibling exc: %r" % (e,))
        raise
async def main():
    t = loop.create_task(sibling())
    await asyncio.sleep(0.05)
    loop.close()          # stock asyncio: RuntimeError here
    events.append("closed mid-run")
    try:
        await asyncio.sleep(0.3)   # give sibling time, if it survived
    except BaseException as e:
        events.append("main exc: %r" % (e,))
    events.append("sibling state: done=%s cancelled=%s" % (t.done(), t.cancelled()))
    return "ok"
try:
    r = loop.run_until_complete(main())
    print("run returned:", r)
except BaseException as e:
    print("run raised: %r" % (e,))
print(events)
# after the run, loop is closed:
try:
    loop.call_soon(lambda: None)
    print("call_soon ok")
except RuntimeError as e:
    print("call_soon raised:", e)
