import asyncio, socket, threading, time
import runloom.aio as aio
received=[]; done=threading.Event()
def server(srv):
    conn,_=srv.accept(); time.sleep(0.5); total=0
    while True:
        b=conn.recv(65536)
        if not b: break
        total+=len(b); time.sleep(0.001)
    received.append(total); conn.close(); done.set()
srv=socket.socket(); srv.bind(('127.0.0.1',0)); srv.listen(1)
addr=srv.getsockname(); threading.Thread(target=server,args=(srv,),daemon=True).start()
N=4*1024*1024
async def main():
    r,w=await aio.open_connection(*addr)
    w.write(b'x'*N); w.close(); await w.wait_closed()
aio.run(main()); done.wait(20)
print('sent',N,'received',received)
