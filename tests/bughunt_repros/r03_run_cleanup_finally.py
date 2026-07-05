"""aio.run() vs asyncio.run(): asyncio.run cancels remaining tasks AND runs
them to completion, so their `finally:` cleanup executes. Does aio.run?"""
import sys, asyncio
import runloom.aio as aio

flag = []

async def bg():
    try:
        await asyncio.sleep(30)
    finally:
        flag.append("cleanup")

async def main():
    asyncio.get_event_loop().create_task(bg())
    await asyncio.sleep(0.05)

aio.run(main())
print("flag:", flag)
if flag != ["cleanup"]:
    print("BUG: background task's finally never ran on aio.run() teardown "
          "(asyncio.run runs cancelled tasks to completion)")
    sys.exit(1)
print("OK")
