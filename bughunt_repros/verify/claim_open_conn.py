import socket
import runloom.aio as aio
real=socket.socket; calls=[]
class FailFirst(socket.socket):
    def __init__(self, family=-1, type=-1, proto=-1, fileno=None):
        calls.append(family)
        if len(calls)==1: raise OSError(97,'Address family not supported')
        super().__init__(family,type,proto,fileno)
async def main():
    srv=real(); srv.bind(('127.0.0.1',0)); srv.listen(1)
    host,port=srv.getsockname()
    import runloom.aio.streams_api as sa
    orig=sa._resolve
    sa._resolve=lambda *a: list(orig(*a))*2
    socket.socket=FailFirst
    try:
        r,w=await aio.open_connection(host,port)
        w.close(); return 'connected'
    except BaseException as e:
        return 'FAILED: %r' % (e,)
    finally:
        socket.socket=real; sa._resolve=orig; srv.close()
print(aio.run(main()))
