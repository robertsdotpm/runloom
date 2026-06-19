"""big_100 / 90 -- IRC-like server.

A small IRC server: NICK, JOIN, PRIVMSG and PING/PONG.  The server also PINGs
clients periodically and expects a PONG.  A swarm of bots join channels, chat,
respond to PINGs, and reconnect.  Per connection a reader and a writer goroutine
(bounded outbound queue) run; the writer is joined before close.

Stresses: fan-out, PING/PONG liveness, state cleanup, network churn.
"""
import socket
import threading

import harness
import netutil
import runloom

NCHAN = 64


def setup(H):
    srv = netutil.listen_tcp()
    chans = [set() for _ in range(NCHAN)]
    lock = threading.Lock()
    H.state = {"port": srv.getsockname()[1], "host": srv.getsockname()[0],
               "missed_pong": [0]}

    def serve(conn):
        outbound = runloom.Chan(128)
        writer_done = runloom.Chan(1)
        nick = None
        chan = None
        started = False
        buf = bytearray()
        try:
            def writer():
                try:
                    while True:
                        val, ok = outbound.recv()
                        if not ok:
                            break
                        try:
                            conn.sendall(val + b"\r\n")
                        except OSError:
                            break
                finally:
                    writer_done.send(1)

            H.fiber(writer)
            started = True
            while True:
                if b"\r\n" not in buf:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                    continue
                nl = buf.index(b"\r\n")
                line = bytes(buf[:nl])
                del buf[:nl + 2]
                parts = line.split(b" ", 2)
                cmd = parts[0].upper()
                if cmd == b"NICK" and len(parts) >= 2:
                    nick = parts[1]
                    outbound.try_send(b"001 " + nick + b" welcome")
                elif cmd == b"JOIN" and len(parts) >= 2:
                    cid = int(parts[1].lstrip(b"#")) % NCHAN
                    chan = chans[cid]
                    with lock:
                        chan.add(outbound)
                    outbound.try_send(b"JOINOK " + parts[1])
                elif cmd == b"PRIVMSG" and len(parts) == 3 and chan is not None:
                    msg = b":" + (nick or b"?") + b" PRIVMSG " + parts[2]
                    with lock:
                        members = list(chan)
                    for ch in members:
                        if ch is not outbound:
                            ch.try_send(msg)
                elif cmd == b"PING":
                    outbound.try_send(b"PONG " + (parts[1] if len(parts) > 1
                                                  else b""))
                elif cmd == b"PONG":
                    pass
        except (OSError, ValueError):
            pass
        finally:
            with lock:
                if chan is not None:
                    chan.discard(outbound)
            try:
                outbound.close()
            except Exception:
                pass
            if started:
                writer_done.recv()
            netutil.close_quiet(conn)

    H.fiber(netutil.serve_forever, H, srv,
         lambda conn, addr: H.fiber(serve, conn))


def bot(H, wid, rng, state):
    port = state["port"]
    host = state["host"]
    chan = "#{0}".format(wid % NCHAN).encode()
    nick = "bot{0}".format(wid).encode()
    H.sleep(rng.random() * 1.0)
    while H.running():
        sock = None
        buf = bytearray()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            sock.sendall(b"NICK " + nick + b"\r\n")
            sock.sendall(b"JOIN " + chan + b"\r\n")
            for _ in range(rng.randint(4, 25)):
                if not H.running():
                    break
                if rng.random() < 0.5:
                    sock.sendall(b"PRIVMSG " + chan + b" :hi-"
                                 + str(rng.randint(0, 1 << 20)).encode()
                                 + b"\r\n")
                else:
                    sock.sendall(b"PING tok\r\n")
                H.op(wid)
                # Drain inbound; reply PONG to any server PING.
                while True:
                    line = netutil.recv_line_timeout(sock, 10, buf)
                    if line is netutil.TIMEOUT:
                        break
                    line = line.rstrip(b"\r")
                    if line.startswith(b"PING"):
                        sock.sendall(b"PONG " + line[5:] + b"\r\n")
                    H.op(wid)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
            H.sleep(0.01)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, bot, H.state)


if __name__ == "__main__":
    harness.main("p90_irc_server", body, setup=setup, default_funcs=4000,
                 describe="IRC NICK/JOIN/PRIVMSG/PING bot swarm")
