"""n05 -- STUN Binding Request over UDP through the real netpoll path.

Same txid-echo + XOR-MAPPED-ADDRESS integrity oracle as n01, but a single UDP
round-trip (netpoll UDP readiness/park path) against score-ordered STUN servers.
A plain Binding Request elicits a Binding Success from a STUN server; the reply
must parse as STUN, echo OUR txid, and carry a plausible public IPv4:port.  A
txid-matched but corrupt datagram is a UDP-recv-path finding; a lost-wake on the
parked receive is a HANG (caught by the watchdog).  Opt-in, never in any gate.
"""
import os
import socket
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import netrun      # noqa: E402
import netlist     # noqa: E402
import protocols   # noqa: E402
from netrun import EnvSkip, Finding   # noqa: E402

FAMILY = socket.AF_INET


def stun_udp_probe(io, srv, timeout_ms):
    txid = os.urandom(12)
    data = io.udp_roundtrip(srv["ip"], srv["port"], FAMILY,
                            protocols.stun_binding_request(txid), 1024, timeout_ms)
    try:
        fam, ip, port = protocols.stun_parse_response(data, txid)
    except protocols.StunParseError as e:
        msg, txid_ok = e.args[0], e.args[1]
        if txid_ok:
            raise Finding("STUN/UDP txid matched but datagram corrupt: " + msg)
        raise EnvSkip("STUN/UDP: " + msg)
    if not protocols.ip_is_plausible_public(fam, ip):
        raise Finding("txid matched but mapped addr not public: %s" % ip)
    return ("pass", "public %s:%d" % (ip, port))


if __name__ == "__main__":
    sys.exit(netrun.main("n05_stun_udp_testnat",
                         [("stun-udp", netlist.CAT_STUN, "IPv4", "UDP",
                           stun_udp_probe)]))
