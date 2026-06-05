"""big_100 / 84 -- chat server.

A line-protocol chat server with rooms and private messages.  Clients JOIN a
room with a nick, then send MSG (broadcast to the room) and PM (to a nick), and
periodically disconnect and reconnect.  Each server connection has a reader
goroutine and a writer goroutine draining a bounded outbound queue (so a slow
client only backs up itself); the writer is joined before the socket closes.

Stresses: broadcast fan-out, private routing, long-lived sockets, churn from
disconnect/reconnect, per-connection backpressure.
"""
import socket
import threading

import harness
import netutil
import runloom

NROOMS = 64


def setup(H):
    srv = netutil.listen_tcp()
    rooms = [set() for _ in range(NROOMS)]    # each: set of outbound Chan
    nicks = {}                                # nick -> outbound Chan
    lock = threading.Lock()
    H.state = {"port": srv.getsockname()[1]}

    def serve(conn):
        outbound = runloom.Chan(128)
        writer_done = runloom.Chan(1)
        nick = None
        room = None
        started = False
        buf = bytearray()
        try:
            first = netutil.recv_until(conn, b"\n").rstrip(b"\n").split(b" ")
            if len(first) < 3 or first[0] != b"JOIN":
                return
            roomid = int(first[1]) % NROOMS
            nick = first[2]
            room = rooms[roomid]
            with lock:
                room.add(outbound)
                nicks[nick] = outbound

            def writer():
                try:
                    while True:
                        val, ok = outbound.recv()
                        if not ok:
                            break
                        try:
                            conn.sendall(val + b"\n")
                        except OSError:
                            break
                finally:
                    writer_done.send(1)

            H.go(writer)
            started = True
            while True:
                if b"\n" not in buf:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                    continue
                nl = buf.index(b"\n")
                line = bytes(buf[:nl])
                del buf[:nl + 1]
                parts = line.split(b" ", 2)
                if parts[0] == b"MSG" and len(parts) >= 2:
                    msg = b"FROM " + nick + b" " + (parts[1] if len(parts) == 2
                                                    else parts[1] + b" " + parts[2])
                    with lock:
                        members = list(room)
                    for ch in members:
                        if ch is not outbound:
                            ch.try_send(msg)
                elif parts[0] == b"PM" and len(parts) == 3:
                    with lock:
                        target = nicks.get(parts[1])
                    if target is not None:
                        target.try_send(b"PM " + nick + b" " + parts[2])
        except (OSError, ValueError):
            pass
        finally:
            with lock:
                if room is not None:
                    room.discard(outbound)
                if nick is not None and nicks.get(nick) is outbound:
                    del nicks[nick]
            try:
                outbound.close()
            except Exception:
                pass
            if started:
                writer_done.recv()
            netutil.close_quiet(conn)

    H.go(netutil.serve_forever, H, srv,
         lambda conn, addr: H.go(serve, conn))


def client(H, wid, rng, state):
    port = state["port"]
    room = wid % NROOMS
    nick = "u{0}".format(wid).encode()
    H.sleep(rng.random() * 1.0)
    while H.running():
        sock = None
        buf = bytearray()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            sock.sendall(b"JOIN " + str(room).encode() + b" " + nick + b"\n")
            for _ in range(rng.randint(4, 30)):
                if not H.running():
                    break
                if rng.random() < 0.85:
                    sock.sendall(b"MSG hello-" + str(rng.randint(0, 1 << 20))
                                 .encode() + b"\n")
                else:
                    tgt = "u{0}".format(rng.randrange(state.get("n", 4000)))
                    sock.sendall(b"PM " + tgt.encode() + b" hi\n")
                H.op(wid)
                # Drain any delivered messages without blocking long.
                while True:
                    line = netutil.recv_line_timeout(sock, 10, buf)
                    if line is netutil.TIMEOUT:
                        break
                    H.op(wid)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
            H.sleep(0.01)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.state["n"] = H.funcs
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p84_chat_server", body, setup=setup, default_funcs=4000,
                 describe="chat rooms + PMs + reconnect churn; broadcast fan-out")
