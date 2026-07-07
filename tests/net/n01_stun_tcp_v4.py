"""n01 -- STUN Binding Request over TCP (IPv4) through the real netpoll path.

Sends a 20-byte RFC 5389 Binding Request carrying a 96-bit transaction id WE
chose to score-ordered public STUN/TCP servers, frames the length-prefixed
reply, and asserts the response echoes OUR txid + carries an XOR-MAPPED-ADDRESS
that xor-decodes to a plausible PUBLIC IPv4:port.  Tying the oracle to the txid
we picked makes a flaky/hostile server unable to fake a finding: a server that
doesn't echo our txid is ENV (skip), only a txid-MATCHED but structurally
corrupt reply implicates TCPConn/netpoll framing.  Opt-in (RUNLOOM_NET_TESTS=1),
never in any gate.
"""
import os
import socket
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import netrun      # noqa: E402
import netlist     # noqa: E402
import protocols   # noqa: E402
from netrun import EnvSkip, Finding   # noqa: E402

FAMILY = socket.AF_INET


def stun_tcp_probe(io, srv, timeout_ms):
    txid = os.urandom(12)
    s = io.tcp_connect(srv["ip"], srv["port"], FAMILY, timeout_ms)
    try:
        io.send_all(s, protocols.stun_binding_request(txid), timeout_ms)
        hdr = io.recv_n(s, 20, timeout_ms)
        mlen = struct.unpack(">H", hdr[2:4])[0]
        body = io.recv_n(s, mlen, timeout_ms) if mlen else b""
        try:
            fam, ip, port = protocols.stun_parse_response(hdr + body, txid)
        except protocols.StunParseError as e:
            msg, txid_ok = e.args[0], e.args[1]
            if txid_ok:
                raise Finding("STUN txid matched but reply corrupt: " + msg)
            raise EnvSkip("STUN: " + msg)
        if not protocols.ip_is_plausible_public(fam, ip):
            raise Finding("txid matched but mapped addr not public: %s" % ip)
        return ("pass", "public %s:%d" % (ip, port))
    finally:
        try:
            s.close()
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(netrun.main("n01_stun_tcp_v4",
                         [("stun-tcp-v4", netlist.CAT_STUN, "IPv4", "TCP",
                           stun_tcp_probe)]))
