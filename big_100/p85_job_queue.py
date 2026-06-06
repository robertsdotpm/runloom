"""big_100 / 85 -- job queue server.

A TCP server exposes SUBMIT / CLAIM / ACK.  A claimed-but-unacked job is
re-queued by a reaper after a lease timeout.  Submitter clients enqueue jobs;
worker clients claim them, sometimes "fail" (claim without acking, to force a
retry), and otherwise ack.  Every job must be acked EXACTLY once -- a double
ack or a lost job is a bug.

Stresses: durable in-memory state, locks, lease/retry fairness, contention.
"""
import socket
import threading
import time

import harness
import netutil

LEASE = 0.5         # seconds before an unacked claim is retried


def setup(H):
    srv = netutil.listen_tcp()
    state = {
        "port": srv.getsockname()[1],
        "host": srv.getsockname()[0],
        "lock": threading.Lock(),
        "ready": [],            # list of job ids ready to claim
        "leased": {},           # id -> lease-deadline
        "acked": set(),         # ids acked (must be unique)
        "next_id": [0],
        "submitted": [0],
        "double_ack": [0],
    }
    H.state = state

    def handle(conn):
        try:
            while True:
                line = netutil.recv_until(conn, b"\n").rstrip(b"\n")
                parts = line.split(b" ", 1)
                cmd = parts[0]
                if cmd == b"SUBMIT":
                    with state["lock"]:
                        jid = state["next_id"][0]
                        state["next_id"][0] += 1
                        state["ready"].append(jid)
                        state["submitted"][0] += 1
                    conn.sendall(b"ID " + str(jid).encode() + b"\n")
                elif cmd == b"CLAIM":
                    with state["lock"]:
                        if state["ready"]:
                            jid = state["ready"].pop()
                            state["leased"][jid] = time.monotonic() + LEASE
                            conn.sendall(b"JOB " + str(jid).encode() + b"\n")
                        else:
                            conn.sendall(b"NONE\n")
                elif cmd == b"ACK" and len(parts) == 2:
                    jid = int(parts[1])
                    with state["lock"]:
                        if jid in state["acked"]:
                            state["double_ack"][0] += 1
                        else:
                            state["acked"].add(jid)
                        state["leased"].pop(jid, None)
                    conn.sendall(b"OK\n")
                else:
                    conn.sendall(b"ERR\n")
        except (OSError, ValueError):
            pass
        finally:
            netutil.close_quiet(conn)

    H.go(netutil.serve_forever, H, srv,
         lambda conn, addr: H.go(handle, conn))

    def reaper():
        # Re-queue expired leases so a failed worker's job gets retried.
        while H.running():
            now = time.monotonic()
            with state["lock"]:
                expired = [j for j, dl in state["leased"].items()
                           if dl < now and j not in state["acked"]]
                for j in expired:
                    del state["leased"][j]
                    state["ready"].append(j)
            H.sleep(0.1)

    H.go(reaper)


def rpc(sock, line):
    sock.sendall(line + b"\n")
    return netutil.recv_until(sock, b"\n").rstrip(b"\n")


def worker(H, wid, rng, state):
    port = state["port"]
    host = state["host"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            for _ in range(rng.randint(2, 8)):
                if not H.running():
                    break
                if rng.random() < 0.5:
                    r = rpc(sock, b"SUBMIT task")
                    if not H.check(r.startswith(b"ID "),
                                   "SUBMIT bad reply: {0!r}".format(r)):
                        return
                else:
                    r = rpc(sock, b"CLAIM")
                    if r == b"NONE":
                        pass
                    elif r.startswith(b"JOB "):
                        jid = r.split(b" ")[1]
                        # Sometimes "fail" (don't ack) to exercise retry.
                        if rng.random() < 0.2:
                            pass
                        else:
                            ack = rpc(sock, b"ACK " + jid)
                            if not H.check(ack == b"OK\n".rstrip(),
                                           "ACK bad reply: {0!r}".format(ack)):
                                return
                    else:
                        H.fail("CLAIM bad reply: {0!r}".format(r))
                        return
                H.op(wid)
            H.task_done(wid)
        except (OSError, ValueError):
            if not H.running():
                break
            H.sleep(0.005)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    s = H.state
    H.check(s["double_ack"][0] == 0,
            "{0} jobs were acked more than once".format(s["double_ack"][0]))
    H.check(len(s["acked"]) <= s["submitted"][0],
            "acked {0} > submitted {1}".format(len(s["acked"]),
                                               s["submitted"][0]))
    H.log("submitted={0} acked={1} ready_left={2} leased_left={3} "
          "double_acks={4}".format(s["submitted"][0], len(s["acked"]),
                                   len(s["ready"]), len(s["leased"]),
                                   s["double_ack"][0]))


if __name__ == "__main__":
    harness.main("p85_job_queue", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="job queue submit/claim/ack/retry; each job acked once")
