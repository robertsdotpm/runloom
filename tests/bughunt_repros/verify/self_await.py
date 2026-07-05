import sys, asyncio
import runloom.aio as aio
async def main():
    holder={}
    async def selfwait(): return await holder['t']
    holder['t']=asyncio.get_event_loop().create_task(selfwait())
    try:
        await asyncio.wait_for(asyncio.shield(holder['t']),1)
        return 'completed'
    except asyncio.TimeoutError:
        try:
            holder['t'].cancel(); return 'deadlock+cancel-ok'
        except RecursionError: return 'deadlock+cancel-RECURSIONERROR'
    except RuntimeError as e: return 'runtimeerror: %s'%e
print(aio.run(main()))
