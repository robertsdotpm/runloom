"""_StreamTransport: abort() after a graceful close() that still has queued
data must still deliver connection_lost (asyncio does). Suspected: abort()
clears the buffer, close() early-returns on _closed, _close_when_drained
never fires -> connection_lost never delivered."""
import sys, asyncio, socket, threading, time
import runloom.aio as aio

def slow_server(srv_sock, stop):
    conn, _ = srv_sock.accept()
    while not stop.is_set():
        time.sleep(0.05)     # never reads -> client's send buffer fills
    conn.close()

srv = socket.socket()
srv.bind(("127.0.0.1", 0))
srv.listen(1)
addr = srv.getsockname()
stop = threading.Event()
threading.Thread(target=slow_server, args=(srv, stop), daemon=True).start()

class Proto(asyncio.Protocol):
    def __init__(self):
        self.lost = asyncio.get_event_loop().create_future()
    def connection_lost(self, exc):
        if not self.lost.done():
            self.lost.set_result(exc)

async def main():
    loop = asyncio.get_event_loop()
    tr, proto = await loop.create_connection(Proto, *addr)
    # Fill the kernel buffer + transport buffer.
    tr.write(b"x" * (8 * 1024 * 1024))
    assert tr.get_write_buffer_size() > 0, "need queued data for the repro"
    tr.close()      # graceful: defers teardown until drained
    tr.abort()      # asyncio: forceful close -> connection_lost soon
    try:
        exc = await asyncio.wait_for(asyncio.shield(proto.lost), 5)
        print("connection_lost delivered:", exc)
        return True
    except asyncio.TimeoutError:
        print("BUG: connection_lost never delivered after close()+abort() "
              "with queued data (hangs forever)")
        return False

ok = aio.run(main())
stop.set()
sys.exit(0 if ok else 1)
