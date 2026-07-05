import socket, asyncio
import runloom.aio as aio
real=socket.socket; calls=[]
class FailFirst(socket.socket):
    def __init__(self, family=-1, type=-1, proto=-1, fileno=None):
        calls.append(family)
        if len(calls)==1: raise OSError(97,'Address family not supported')
        super().__init__(family,type,proto,fileno)
async def main():
    loop=asyncio.get_event_loop()
    srv=real(); srv.bind(('127.0.0.1',0)); srv.listen(1)
    host,port=srv.getsockname()
    import runloom.aio.loop_net as ln
    orig=ln._resolve
    ln._resolve=lambda *a: orig(*a)*2   # ensure a fallback entry exists
    socket.socket=FailFirst
    try:
        tr,p=await loop.create_connection(asyncio.Protocol,host,port)
        tr.close(); return 'connected'
    finally:
        socket.socket=real; ln._resolve=orig; srv.close()
print(aio.run(main()))
