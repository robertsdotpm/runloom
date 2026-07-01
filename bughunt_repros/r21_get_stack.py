"""RunloomTask never runs asyncio.Task.__init__, so the C Task's _coro field
stays NULL. Inherited C methods that read it (get_stack/print_stack) may
crash or misbehave."""
import sys, asyncio
import runloom.aio as aio

async def main():
    async def sub():
        await asyncio.sleep(0.2)
    t = asyncio.get_event_loop().create_task(sub())
    await asyncio.sleep(0.05)
    try:
        st = t.get_stack()
        print("get_stack:", st)
    except BaseException as e:
        print("get_stack raised: %r" % (e,))
    try:
        import io
        buf = io.StringIO()
        t.print_stack(file=buf)
        print("print_stack ok, len", len(buf.getvalue()))
    except BaseException as e:
        print("print_stack raised: %r" % (e,))
    await t

aio.run(main())
print("survived")
