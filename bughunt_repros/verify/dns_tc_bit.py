"""Verify: runloom's monkey DNS resolver ignores the TC (truncation) bit and
never falls back to TCP.

Fake nameserver on 127.0.0.1:15353:
  - UDP: answers every A query with TC=1 and either 0 answers (the common
    "truncate to empty" server behavior, e.g. BIND minimal-truncation) or a
    partial answer set (2 of 30 A records), depending on the queried name.
  - TCP: serves the FULL 30-record answer.  glibc's stub resolver and Go's
    netgo resolver both retry over TCP when TC is set, so they would land
    here and return 30 addresses.

Expected (correct/libc/Go): 30 addresses, TCP listener contacted.
Claimed bug: runloom returns EAI_NONAME (zero-answer case) or a 2-address
partial set, TCP listener never contacted, and the wrong result is cached.
"""
import socket, struct, threading, sys, time

sys.path.insert(0, "/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo/src")

PORT = 15353
FULL_COUNT = 30

tcp_contacted = threading.Event()


def build_response(query, tc, nanswers):
    txn = query[:2]
    # QR=1, RD=1, RA=1, TC per arg
    flags = 0x8180 | (0x0200 if tc else 0)
    # question section: name + 4 bytes qtype/qclass
    off = 12
    while query[off] != 0:
        off += 1 + query[off]
    qend = off + 1 + 4
    question = query[12:qend]
    hdr = struct.pack("!HHHHHH", struct.unpack("!H", txn)[0], flags, 1,
                      nanswers, 0, 0)
    body = b""
    for i in range(nanswers):
        # compressed name ptr to offset 12, type A, class IN, ttl 60, rdlen 4
        body += b"\xc0\x0c" + struct.pack("!HHIH", 1, 1, 60, 4)
        body += bytes([10, 0, 0, i + 1])
    return hdr + question + body


def udp_server(sock):
    while True:
        try:
            data, peer = sock.recvfrom(4096)
        except OSError:
            return
        qtype = struct.unpack("!H", data[-4:-2])[0]
        # extract qname first label to pick behavior
        ln = data[12]
        first = data[13:13 + ln].decode()
        if qtype != 1:                     # AAAA etc: NOERROR, 0 answers, no TC
            sock.sendto(build_response(data, tc=False, nanswers=0), peer)
        elif first == "trunczero":
            sock.sendto(build_response(data, tc=True, nanswers=0), peer)
        else:  # truncpartial
            sock.sendto(build_response(data, tc=True, nanswers=2), peer)


def tcp_server(lsock):
    while True:
        try:
            c, _ = lsock.accept()
        except OSError:
            return
        tcp_contacted.set()
        blen = c.recv(2)
        q = c.recv(struct.unpack("!H", blen)[0])
        resp = build_response(q, tc=False, nanswers=FULL_COUNT)
        c.sendall(struct.pack("!H", len(resp)) + resp)
        c.close()


usock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
usock.bind(("127.0.0.1", PORT))
tsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
tsock.bind(("127.0.0.1", PORT))
tsock.listen(4)
threading.Thread(target=udp_server, args=(usock,), daemon=True).start()
threading.Thread(target=tcp_server, args=(tsock,), daemon=True).start()

import runloom, runloom_c
import runloom.monkey
runloom.monkey.patch()
import runloom.monkey.dns as D
D._resolvers_cache = ["127.0.0.1"]
D._DNS_PORT = PORT
D._dns_result_cache.clear()

out = {}


def main():
    try:
        infos = socket.getaddrinfo("trunczero.example", 80,
                                   family=socket.AF_INET,
                                   type=socket.SOCK_STREAM)
        out["zero"] = [i[4][0] for i in infos]
    except socket.gaierror as e:
        out["zero"] = "gaierror: %s" % (e,)
    try:
        infos = socket.getaddrinfo("truncpartial.example", 80,
                                   family=socket.AF_INET,
                                   type=socket.SOCK_STREAM)
        out["partial"] = [i[4][0] for i in infos]
    except socket.gaierror as e:
        out["partial"] = "gaierror: %s" % (e,)
    # cached wrong result?
    try:
        infos = socket.getaddrinfo("truncpartial.example", 80,
                                   family=socket.AF_INET,
                                   type=socket.SOCK_STREAM)
        out["partial_cached"] = [i[4][0] for i in infos]
    except socket.gaierror as e:
        out["partial_cached"] = "gaierror: %s" % (e,)


runloom_c.fiber(main)
runloom_c.run()

print("TC=1, 0 answers  ->", out.get("zero"))
print("TC=1, 2/%d answers ->" % FULL_COUNT, out.get("partial"))
print("repeat (cache)   ->", out.get("partial_cached"))
print("TCP fallback attempted:", tcp_contacted.is_set())

bug = (not tcp_contacted.is_set()
       and isinstance(out.get("zero"), str)
       and isinstance(out.get("partial"), list)
       and len(out["partial"]) == 2)
print("BUG CONFIRMED" if bug else "NOT CONFIRMED")
