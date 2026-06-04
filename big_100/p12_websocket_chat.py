"""big_100 / 12 -- WebSocket chat room.

A local WebSocket server with a fixed set of rooms.  Thousands of clients join
a room (first frame = room name), then send messages that the server broadcasts
to everyone else in the room.  Each client counts the broadcasts it receives;
the fan-out has to keep flowing without any one slow reader wedging a room.

Stresses: long-lived connections, broadcast fan-out, per-connection
backpressure (bounded outbound channel, drop on overflow), cleanup on leave.

Per server connection: one READER goroutine (recv frames -> broadcast) and one
WRITER goroutine (drain a bounded outbound channel -> send frames), so a slow
client only backs up its own channel.
"""
import threading

import harness
import netutil
import runloom
import wsutil

NROOMS = 64


class Room(object):
    __slots__ = ("lock", "members")

    def __init__(self):
        self.lock = threading.Lock()        # cooperative under monkey
        self.members = []                   # list of outbound Chan

    def join(self, ch):
        with self.lock:
            self.members.append(ch)

    def leave(self, ch):
        with self.lock:
            try:
                self.members.remove(ch)
            except ValueError:
                pass

    def broadcast(self, msg, sender):
        with self.lock:
            for ch in self.members:
                if ch is sender:
                    continue
                ch.try_send(msg)            # non-blocking -> drop if full


def setup(H):
    srv = netutil.listen_tcp()
    H.state = {"port": srv.getsockname()[1],
               "rooms": [Room() for _ in range(NROOMS)]}
    H.register_close(srv)

    def serve(sock):
        outbound = runloom.Chan(64)
        writer_done = runloom.Chan(1)
        room = None
        writer_started = False
        try:
            wsutil.server_handshake(sock)
            room_name = wsutil.recv_text(sock)
            if room_name is None:
                return
            room = H.state["rooms"][hash(room_name) % NROOMS]
            room.join(outbound)

            def writer():
                try:
                    while True:
                        val, ok = outbound.recv()
                        if not ok:
                            break
                        try:
                            wsutil.send_text(sock, val)
                        except OSError:
                            break
                finally:
                    writer_done.send(1)

            H.go(writer)
            writer_started = True
            # Reader: parks on the client socket; the client's FIN (a network
            # event) wakes it, so we never need a cross-goroutine close here.
            while True:
                msg = wsutil.recv_text(sock)
                if msg is None:
                    break
                room.broadcast(msg, outbound)
        except OSError:
            pass
        finally:
            if room is not None:
                room.leave(outbound)
            # Close outbound to wake a writer parked on recv; the client FIN
            # breaks a writer parked on send.  JOIN the writer before closing
            # the fd so nothing is parked on it at close time.
            try:
                outbound.close()
            except Exception:
                pass
            if writer_started:
                writer_done.recv()
            netutil.close_quiet(sock)

    def accept_loop():
        while H.running():
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            H.go(serve, conn)

    H.go(accept_loop)


def client(H, wid, rng, state):
    import socket
    port = state["port"]
    room = "room{0}".format(wid % NROOMS)
    H.sleep(rng.random() * 1.0)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            wsutil.client_handshake(sock)
            wsutil.send_text(sock, room, mask=True)
            # Single goroutine: send a message, then briefly drain any
            # broadcasts.  No second goroutine ever parks on this fd, so the
            # close at the bottom is always safe.
            for _ in range(rng.randint(5, 40)):
                if not H.running():
                    break
                wsutil.send_text(
                    sock, "m{0}-{1}".format(wid, rng.randint(0, 1 << 30)),
                    mask=True)
                H.op(wid)
                while True:
                    frame = wsutil.recv_text_timeout(sock, 15)
                    if frame is wsutil.TIMEOUT:
                        break
                    if frame is None:
                        raise OSError("server closed")
                    H.op(wid)               # a received broadcast
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
            H.sleep(0.01)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p12_websocket_chat", body, setup=setup, default_funcs=6000,
                 describe="WebSocket rooms + broadcast fan-out with backpressure")
