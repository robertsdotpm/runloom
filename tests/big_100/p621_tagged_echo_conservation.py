"""big_100 / 621 -- tagged-frame echo with digest + bytes conservation.

Like the p01 echo clients, but every message carries a (conn_id, seq) frame and
each worker folds a CRC digest + byte count over the frames it fully round-trips.
The echo returns each frame verbatim, so a correct run conserves both:
  * content digest: summed-across-workers XOR(crc(sent)) == XOR(crc(recv)), and
  * bytes: Sum(bytes_sent) == Sum(bytes_recv).
Each returned frame's (conn_id, seq) is also checked -- a frame coming back with
a DIFFERENT conn_id/seq is cross-goroutine mis-delivery / torn or interleaved
recv, which the untagged got==payload check in p01 cannot see.

Only COMPLETED round-trips (send + read both succeeded) are counted, so a
connection drop at load/teardown leaves no unmatched send -> no false positive.

Stresses: the M:N socket data path, with a data-integrity oracle rather than a
per-op self-referential equality.
"""
import socket

import harness
import netutil


def echo_handler(H, conn):
    try:
        while True:
            data = conn.recv(65536)
            if not data:
                break
            conn.sendall(data)
    except OSError:
        pass
    finally:
        netutil.close_quiet(conn)


def setup(H):
    servers = []
    for ip in H.net_ips:
        srv = netutil.listen_tcp(host=ip)
        H.register_close(srv)
        H.fiber(netutil.serve_forever, H, srv,
                lambda conn, addr: H.fiber(echo_handler, H, conn))
        servers.append((ip, srv.getsockname()[1]))
    n = H.funcs
    H.state = {
        "servers": servers,
        "send_dig": [0] * n,        # worker i only
        "recv_dig": [0] * n,        # worker i only
        "send_bytes": [0] * n,      # worker i only
        "recv_bytes": [0] * n,      # worker i only
        "misdeliver": [0] * n,      # worker i only
    }


def client(H, wid, rng, state):
    servers = state["servers"]
    H.sleep(rng.random() * 0.5)
    conn_id = wid + 1                       # unique nonzero tag per goroutine
    seq = 0
    sd = rd = sb = rb = mis = 0
    for _ in H.round_range():
        sock = None
        did = 0
        host, port = servers[rng.randrange(len(servers))]
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            n = rng.randint(1, 8)
            for _ in range(n):
                if not H.running():
                    break
                payload = rng.randbytes(rng.randint(1, 512))
                frame = netutil.frame_msg(conn_id, seq, payload)
                sock.sendall(frame)
                # read_frame may raise on a drop -> this send is NOT counted, so
                # the conservation totals only ever see complete round-trips.
                cid, s, pl, raw = netutil.read_frame(sock)
                if cid != conn_id or s != seq or pl != payload:
                    mis += 1
                sd ^= netutil.frame_crc(frame)
                sb += len(frame)
                rd ^= netutil.frame_crc(raw)
                rb += len(raw)
                seq += 1
                H.op(wid)
                did += 1
            if did:
                H.task_done(wid)
        except OSError:
            if not H.running():
                break
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
    state["send_dig"][wid] = sd
    state["recv_dig"][wid] = rd
    state["send_bytes"][wid] = sb
    state["recv_bytes"][wid] = rb
    state["misdeliver"][wid] = mis


def body(H):
    H.run_pool(H.funcs, client, H.state)


def post(H):
    st = H.state
    sd = rd = 0
    for x in st["send_dig"]:
        sd ^= x
    for x in st["recv_dig"]:
        rd ^= x
    mis = sum(st["misdeliver"])
    sb = sum(st["send_bytes"])
    rb = sum(st["recv_bytes"])
    H.check(mis == 0,
            "tag mis-delivery: {0} frame(s) returned with wrong (conn_id, seq)"
            .format(mis))
    H.check(sd == rd,
            "content digest mismatch: send=0x{0:08x} recv=0x{1:08x}".format(sd, rd))
    H.check(sb == rb,
            "byte conservation violated: sent={0} recv={1}".format(sb, rb))
    H.require_no_fd_leak()          # tens of thousands of short-lived sockets: no fd leak
    H.log("sent_bytes={0} recv_bytes={1} digest_send=0x{2:08x} "
          "digest_recv=0x{3:08x} misdeliver={4}".format(sb, rb, sd, rd, mis))


if __name__ == "__main__":
    harness.main("p621_tagged_echo_conservation", body, setup=setup, post=post,
                 default_funcs=10000,
                 describe="tagged-frame echo clients with per-worker CRC digest + "
                          "bytes conservation (send==recv); catches cross-goroutine "
                          "mis-delivery an untagged echo check hides")
