import time, socket, asyncio
socket.getnameinfo = lambda sa, fl: (time.sleep(2.0), ('h','p'))[1]
async def main():
    loop=asyncio.get_event_loop(); ticks=[]
    async def ticker():
        while True: ticks.append(loop.time()); await asyncio.sleep(0.05)
    t=loop.create_task(ticker()); await asyncio.sleep(0.2)
    await loop.getnameinfo(('127.0.0.1',80)); await asyncio.sleep(0.2); t.cancel()
    return max(b-a for a,b in zip(ticks,ticks[1:]))
print('max ticker stall %.2fs' % asyncio.run(main()))
