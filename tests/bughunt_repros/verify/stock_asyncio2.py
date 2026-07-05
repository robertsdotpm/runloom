import socket, asyncio
real=socket.socket; calls=[]
class FailFirst(socket.socket):
    def __init__(self, family=-1, type=-1, proto=-1, fileno=None):
        calls.append((family,type))
        if len(calls)==1: raise OSError(97,'Address family not supported')
        super().__init__(family,type,proto,fileno)
async def main():
    loop=asyncio.get_event_loop()
    srv=real(); srv.bind(('127.0.0.1',0)); srv.listen(1)
    host,port=srv.getsockname()
    orig=loop.getaddrinfo
    async def gai(*a,**k):
        r=await orig(*a,**k)
        r=list(r)*2
        print("infos:", r)
        return r
    loop.getaddrinfo=gai
    socket.socket=FailFirst
    try:
        tr,p=await loop.create_connection(asyncio.Protocol,host,port)
        tr.close(); return 'connected'
    except BaseException as e:
        return 'FAILED: %r calls=%r' % (e, calls)
    finally:
        socket.socket=real; srv.close()
print(asyncio.run(main()))
