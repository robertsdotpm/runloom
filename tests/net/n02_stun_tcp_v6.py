"""n02 -- STUN Binding Request over TCP (IPv6) through the real netpoll path.

Identical txid-echo + XOR-MAPPED-ADDRESS integrity oracle as n01, but dialing
literal IPv6 STUN/TCP servers so the whole dual-stack path (v6 connect, v6
netpoll arm, TCP recv/send over AF_INET6) is smoke-tested.  A box with no working
IPv6 (connect => ENETUNREACH/EAFNOSUPPORT, no v6 route) SKIPs cleanly -- absence
of IPv6 is never a finding.  Opt-in, never in any gate.
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

FAMILY = socket.AF_INET6


def stun_tcp6_probe(io, srv, timeout_ms):
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
                raise Finding("STUN/v6 txid matched but reply corrupt: " + msg)
            raise EnvSkip("STUN/v6: " + msg)
        if not protocols.ip_is_plausible_public(fam, ip):
            raise Finding("txid matched but mapped v6 addr not public: %s" % ip)
        return ("pass", "public [%s]:%d" % (ip, port))
    finally:
        try:
            s.close()
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(netrun.main("n02_stun_tcp_v6",
                         [("stun-tcp-v6", netlist.CAT_STUN, "IPv6", "TCP",
                           stun_tcp6_probe)]))
