import asyncio
loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
out={}
async def main():
    try:
        loop.close(); out['closed']=True
    except RuntimeError as e: out['raised']=str(e)
    return 1
print(loop.run_until_complete(main()), out)
