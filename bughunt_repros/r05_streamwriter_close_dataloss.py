"""aio.open_connection StreamWriter: write() buffers residue when the socket
is full; close() must flush buffered data before FIN (asyncio flushes the
transport buffer on close). Does runloom's bridge writer drop it?"""
import sys, asyncio, socket, threading, time
import runloom.aio as aio

received = []
done = threading.Event()

def server(srv_sock):
    conn, _ = srv_sock.accept()
    time.sleep(0.5)          # let the client's send buffer fill up
    total = 0
    while True:
        b = conn.recv(65536)
        if not b:
            break
        total += len(b)
        time.sleep(0.001)    # slow consumer
    received.append(total)
    conn.close()
    done.set()

srv = socket.socket()
srv.bind(("127.0.0.1", 0))
srv.listen(1)
addr = srv.getsockname()
threading.Thread(target=server, args=(srv,), daemon=True).start()

N = 4 * 1024 * 1024

async def main():
    reader, writer = await aio.open_connection(*addr)
    writer.write(b"x" * N)   # way beyond the socket send buffer
    writer.close()           # asyncio: flushes remaining buffer before FIN
    await writer.wait_closed()

aio.run(main())
done.wait(20)
print("sent:", N, "received:", received)
if not received or received[0] != N:
    print("BUG: StreamWriter.close() dropped %d buffered bytes" %
          (N - (received[0] if received else 0)))
    sys.exit(1)
print("OK")
