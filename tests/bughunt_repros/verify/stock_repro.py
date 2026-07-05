import asyncio
loop=asyncio.new_event_loop(); asyncio.set_event_loop(loop)
ran=[]
loop.call_soon(ran.append, 1)
fut=loop.create_future(); fut.set_result(7)
print('result:', loop.run_until_complete(fut), 'callbacks ran:', ran)
