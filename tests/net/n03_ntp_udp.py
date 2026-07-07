"""n03 -- NTP client request over UDP (IPv4 + IPv6) through the real netpoll path.

Sends a 48-byte NTP client packet with a known 64-bit token planted in the
Transmit Timestamp to score-ordered public NTP servers; a compliant server
copies that value into the reply's Originate Timestamp, so the echo proves the
datagram round-tripped through OUR socket.  Runs the SAME oracle over AF_INET
and AF_INET6 (the v6 leg SKIPs cleanly if the box has no IPv6).  A datagram whose
originate == our transmit (our exchange) but with inconsistent mode/version is a
UDP-recv-path finding; anything else is ENV.  Opt-in, never in any gate.
"""
import os
import socket
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import netrun      # noqa: E402
import netlist     # noqa: E402
import protocols   # noqa: E402
from netrun import EnvSkip, Finding   # noqa: E402


def _ntp_probe(family):
    def probe(io, srv, timeout_ms):
        token = os.urandom(8)
        data = io.udp_roundtrip(srv["ip"], srv["port"], family,
                                protocols.ntp_client_packet(token), 128, timeout_ms)
        try:
            stratum = protocols.ntp_parse_response(data, token)
        except protocols.NtpParseError as e:
            msg, tok_ok = e.args[0], e.args[1]
            if tok_ok:
                raise Finding("NTP originate matched but reply corrupt: " + msg)
            raise EnvSkip("NTP: " + msg)
        return ("pass", "stratum %d" % stratum)
    return probe


if __name__ == "__main__":
    sys.exit(netrun.main("n03_ntp_udp", [
        ("ntp-udp-v4", netlist.CAT_NTP, "IPv4", "UDP", _ntp_probe(socket.AF_INET)),
        ("ntp-udp-v6", netlist.CAT_NTP, "IPv6", "UDP", _ntp_probe(socket.AF_INET6)),
    ]))
