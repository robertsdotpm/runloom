import asyncio, socket
import runloom.aio as aio
async def main():
    loop=asyncio.get_event_loop(); loop.set_exception_handler(lambda l,c: None)
    state={'n':0}
    class P(asyncio.Protocol):
        def __init__(self):
            state['n']+=1
            if state['n']==1: raise ValueError('transient')
        def connection_made(self,tr): tr.write(b'hello'); tr.close()
    server=await loop.create_server(P,'127.0.0.1',0)
    port=server.sockets[0].getsockname()[1]
    def try_conn():
        s=socket.socket(); s.settimeout(2)
        try:
            s.connect(('127.0.0.1',port))
            try: data=s.recv(100)
            except OSError: data=b''
            s.close(); return data
        except OSError: return None
    r1=await loop.run_in_executor(None,try_conn)
    await asyncio.sleep(0.2)
    r2=await loop.run_in_executor(None,try_conn)
    server.close(); return r1,r2
print(aio.run(main()))
