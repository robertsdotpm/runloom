import asyncio
import runloom.aio as aio
loop = aio.RunloomEventLoop(); asyncio.set_event_loop(loop)
out={}
async def main():
    try:
        loop.close(); out['closed']=True
    except RuntimeError as e: out['raised']=str(e)
    return 1
print(loop.run_until_complete(main()), out)
