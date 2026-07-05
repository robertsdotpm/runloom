import asyncio
import runloom.aio as aio
flag=[]
async def bg():
    try: await asyncio.sleep(30)
    finally: flag.append('cleanup')
async def main():
    asyncio.get_event_loop().create_task(bg())
    await asyncio.sleep(0.05)
aio.run(main())
print(flag)  # stock asyncio.run: ['cleanup']
