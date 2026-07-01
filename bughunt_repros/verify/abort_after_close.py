import asyncio, socket, threading, time
import runloom.aio as aio
def slow_server(srv, stop):
    conn,_=srv.accept()
    while not stop.is_set(): time.sleep(0.05)
srv=socket.socket(); srv.bind(('127.0.0.1',0)); srv.listen(1)
stop=threading.Event(); threading.Thread(target=slow_server,args=(srv,stop),daemon=True).start()
class P(asyncio.Protocol):
    def __init__(self): self.lost=asyncio.get_event_loop().create_future()
    def connection_lost(self, exc):
        if not self.lost.done(): self.lost.set_result(exc)
async def main():
    loop=asyncio.get_event_loop()
    tr,proto=await loop.create_connection(P,*srv.getsockname())
    tr.write(b'x'*(8*1024*1024)); assert tr.get_write_buffer_size()>0
    tr.close(); tr.abort()
    try:
        print('lost:', await asyncio.wait_for(asyncio.shield(proto.lost),5)); return True
    except asyncio.TimeoutError:
        print('connection_lost NEVER delivered'); return False
ok=aio.run(main()); stop.set(); assert ok
