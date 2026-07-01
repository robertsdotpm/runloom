"""Same as r03 but through aio.install() + stdlib asyncio.run (whose Runner
cancels remaining tasks and RUNS them to completion)."""
import sys, asyncio
import runloom.aio as aio

aio.install()
flag = []

async def bg():
    try:
        await asyncio.sleep(30)
    finally:
        flag.append("cleanup")

async def main():
    asyncio.get_event_loop().create_task(bg())
    await asyncio.sleep(0.05)

asyncio.run(main())
print("flag via stdlib asyncio.run:", flag)
