"""Stateless DNS wire-protocol helpers: IP-literal test, query build,
name-skip, answer parse."""
from ._base import *  # noqa: F401,F403  (shared foundation)

import struct as _struct
import random as _rand

_DNS_PORT      = 53
_QTYPE_A       = 1
_QTYPE_AAAA    = 28

def _is_ip_literal(host):
    """Return AF if host is a numeric address, else None."""
    if isinstance(host, (bytes, bytearray)):
        # Stock getaddrinfo accepts bytes hosts; decode for the str-only
        # inet_pton / split below.  Non-ASCII bytes can't be an IP literal.
        try:
            host = host.decode("ascii")
        except UnicodeDecodeError:
            return None
    h = host.split("%", 1)[0]    # strip v6 zone-id
    try:
        socket.inet_pton(socket.AF_INET, h)
        return socket.AF_INET
    except (OSError, ValueError):
        pass
    try:
        socket.inet_pton(socket.AF_INET6, h)
        return socket.AF_INET6
    except (OSError, ValueError):
        pass
    return None


def _encode_query_name(name):
    """Encode a DNS query name to its ASCII wire form.

    Mirrors stock getaddrinfo: bytes hosts are used verbatim, and non-ASCII
    str hostnames are IDNA (punycode) encoded rather than raising an encode
    error.  (The ``idna`` codec is pre-warmed in runtime._prewarm.)"""
    if isinstance(name, (bytes, bytearray)):
        return bytes(name)
    try:
        return name.encode("ascii")
    except UnicodeEncodeError:
        return name.encode("idna")


def _build_query(name, qtype):
    txn = _rand.randint(0, 0xFFFF)
    flags = 0x0100   # standard query + recursion desired
    # ARCOUNT=1 for the EDNS0 OPT pseudo-RR appended below.
    hdr = _struct.pack("!HHHHHH", txn, flags, 1, 0, 0, 1)
    qname = b""
    for lbl in _encode_query_name(name).split(b"."):
        if not lbl:
            continue
        if len(lbl) > 63:
            raise OSError("DNS label too long")
        qname += bytes([len(lbl)]) + lbl
    qname += b"\x00"
    qpart = qname + _struct.pack("!HH", qtype, 1)   # IN class
    # EDNS0 OPT record (RFC 6891): root name, TYPE=41, CLASS=UDP payload
    # size (advertise 4096 so servers send larger answers instead of
    # setting TC), TTL=0 (ext-rcode/version/flags), RDLEN=0.
    opt = b"\x00" + _struct.pack("!HHIH", 41, 4096, 0, 0)
    return txn, hdr + qpart + opt


def _skip_dns_name(data, off):
    while True:
        if off >= len(data):
            raise OSError("DNS name overruns packet")
        ln = data[off]
        if ln == 0:
            return off + 1
        if (ln & 0xC0) == 0xC0:
            return off + 2
        off += 1 + ln


def _parse_dns_answer(data, expected_txn):
    if len(data) < 12:
        raise OSError("DNS response too short")
    txn, flags, qd, an, _ns, _ar = _struct.unpack("!HHHHHH", data[:12])
    if txn != expected_txn:
        raise OSError("DNS txn mismatch")
    if flags & 0x0200:   # TC (truncation) bit -- the answer set is incomplete
        # We speak UDP only; parsing what fit would silently drop records (and
        # cache the partial/empty set).  Raise so _resolve_qtype falls through
        # to the platform resolver, which retries over TCP and returns the
        # full answer.
        raise OSError("DNS response truncated (TC set)")
    rcode = flags & 0xF
    if rcode == 3:       # NXDOMAIN
        return []
    if rcode != 0:
        raise OSError("DNS server rcode=%d" % rcode)
    off = 12
    for _ in range(qd):
        off = _skip_dns_name(data, off)
        off += 4
    addrs = []
    for _ in range(an):
        off = _skip_dns_name(data, off)
        if off + 10 > len(data):
            break
        rtype, _rclass, _ttl, rdlen = _struct.unpack("!HHIH", data[off:off+10])
        off += 10
        rdata = data[off:off+rdlen]
        off += rdlen
        if rtype == _QTYPE_A and len(rdata) == 4:
            addrs.append(socket.inet_ntoa(rdata))
        elif rtype == _QTYPE_AAAA and len(rdata) == 16:
            addrs.append(socket.inet_ntop(socket.AF_INET6, rdata))
    return addrs
