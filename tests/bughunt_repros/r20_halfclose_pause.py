"""(a) eof_received() -> True keeps transport writable (half-close);
(b) pause_reading/resume_reading actually gates data_received."""
import sys, asyncio, socket, threading, time
import runloom.aio as aio

async def main():
    loop = asyncio.get_event_loop()
    results = {}

    # ---- (a) half-close ----
    class HC(asyncio.Protocol):
        def __init__(self):
            self.buf = b""
            self.got_eof = loop.create_future()
        def connection_made(self, tr):
            self.tr = tr
        def data_received(self, data):
            self.buf += data
        def eof_received(self):
            if not self.got_eof.done():
                self.got_eof.set_result(None)
            return True          # keep transport open for writing
    protos = []
    def factory():
        p = HC(); protos.append(p); return p
    server = await loop.create_server(factory, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    def client():
        s = socket.socket(); s.connect(("127.0.0.1", port))
        s.sendall(b"req"); s.shutdown(socket.SHUT_WR)
        s.settimeout(5)
        data = b""
        try:
            while True:
                b_ = s.recv(4096)
                if not b_: break
                data += b_
        except OSError:
            pass
        s.close()
        return data
    cf = loop.run_in_executor(None, client)
    p = None
    for _ in range(100):
        await asyncio.sleep(0.02)
        if protos: p = protos[0]; break
    await asyncio.wait_for(p.got_eof, 5)
    p.tr.write(b"resp:" + p.buf)      # write AFTER read-side EOF
    p.tr.close()
    results["halfclose"] = await asyncio.wait_for(cf, 5)
    server.close()

    # ---- (b) pause/resume reading ----
    class PR(asyncio.Protocol):
        def __init__(self):
            self.chunks = []
        def connection_made(self, tr):
            self.tr = tr
            tr.pause_reading()
        def data_received(self, data):
            self.chunks.append((loop.time(), len(data)))
    protos2 = []
    def factory2():
        p = PR(); protos2.append(p); return p
    server2 = await loop.create_server(factory2, "127.0.0.1", 0)
    port2 = server2.sockets[0].getsockname()[1]
    c = socket.socket(); c.connect(("127.0.0.1", port2)); c.sendall(b"x" * 1000)
    await asyncio.sleep(0.3)
    p2 = protos2[0]
    before = list(p2.chunks)
    p2.tr.resume_reading()
    await asyncio.sleep(0.3)
    after = list(p2.chunks)
    results["paused_before"] = len(before)
    results["delivered_after"] = sum(n for _, n in after)
    c.close(); server2.close()
    return results

r = aio.run(main())
print(r)
ok = (r["halfclose"] == b"resp:req" and r["paused_before"] == 0
      and r["delivered_after"] == 1000)
if not ok:
    print("BUG: half-close or pause/resume broken")
    sys.exit(1)
print("OK")
