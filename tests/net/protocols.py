"""Pure protocol codecs + integrity oracles for the remote-internet suite.

NO I/O lives here -- every function builds or parses bytes and is deterministic,
so it is unit-testable offline (run this module directly for a round-trip self
test).  The networked programs (n01..n05) call these to (a) build a request that
plants a transaction TOKEN we choose, and (b) parse a response and assert it
echoes THAT token.  Tying every finding to a token we picked is what makes a
flaky/hostile server unable to manufacture a false positive: a server that
fails to echo our token is ENV (skip), never a finding -- only a response that
IS our transaction (token matches) yet is internally inconsistent implicates
pygo's transport (see tests/net/README semantics).

Protocols:
  * STUN  (RFC 5389): 20-byte Binding Request, txid echo + XOR-MAPPED-ADDRESS.
  * NTP   (RFC 5905): 48-byte client packet, Originate == our Transmit echo.
  * MQTT  (3.1.1):    CONNECT -> CONNACK return-code.
"""
import struct

# ---------------------------------------------------------------- STUN -------
STUN_MAGIC_COOKIE = 0x2112A442
STUN_BINDING_REQUEST = 0x0001
STUN_BINDING_SUCCESS = 0x0101
ATTR_MAPPED_ADDRESS = 0x0001
ATTR_XOR_MAPPED_ADDRESS = 0x0020


def stun_binding_request(txid):
    """20-byte STUN Binding Request carrying `txid` (12 bytes we choose)."""
    if len(txid) != 12:
        raise ValueError("txid must be 12 bytes")
    return struct.pack(">HHI12s", STUN_BINDING_REQUEST, 0, STUN_MAGIC_COOKIE, txid)


class StunParseError(Exception):
    """The bytes did not parse as a well-formed STUN message we can trust."""


def stun_parse_response(data, expect_txid):
    """Parse a STUN response.  Returns (family, ip_str, port).

    Raises StunParseError on ANY inconsistency.  The caller decides ENV vs BUG:
    a txid MISMATCH is ENV (not our transaction / wrong server), but a txid
    MATCH with a malformed body is a transport BUG.  So we surface the txid
    check as a distinct signal: `txid_matches` on the exception.
    """
    if len(data) < 20:
        raise StunParseError("short header (%d < 20)" % len(data), False)
    mtype, mlen, cookie, txid = struct.unpack(">HHI12s", data[:20])
    txid_ok = (txid == expect_txid)
    if cookie != STUN_MAGIC_COOKIE:
        raise StunParseError("bad magic cookie 0x%08x" % cookie, txid_ok)
    if not txid_ok:
        # Not our transaction -> ENV (a stale/other reply). Never a finding.
        raise StunParseError("txid mismatch (not our transaction)", False)
    # From here the message IS our transaction (cookie + txid ours).  A well-
    # formed non-success type is the SERVER'S choice (error/policy/rate-limit),
    # NOT byte corruption on our path -> ENV, never a finding.  Only STRUCTURAL
    # inconsistency below (length lies, attr overrun, undecodable/impossible
    # address) implicates the transport.
    if mtype != STUN_BINDING_SUCCESS:
        raise StunParseError("type 0x%04x != Binding Success (server policy)"
                             % mtype, False)
    body = data[20:]
    if len(body) < mlen:
        raise StunParseError(
            "Message-Length %d exceeds framed body %d" % (mlen, len(body)), True)
    body = body[:mlen]
    off = 0
    mapped = None
    xor_mapped = None
    while off + 4 <= len(body):
        atype, alen = struct.unpack(">HH", body[off:off + 4])
        off += 4
        if off + alen > len(body):
            raise StunParseError("attribute overruns body", True)
        aval = body[off:off + alen]
        off += alen + ((4 - (alen % 4)) % 4)   # 32-bit padding
        if atype == ATTR_XOR_MAPPED_ADDRESS:
            xor_mapped = _decode_mapped(aval, txid, xor=True)
        elif atype == ATTR_MAPPED_ADDRESS:
            mapped = _decode_mapped(aval, txid, xor=False)
    addr = xor_mapped or mapped
    if addr is None:
        raise StunParseError("no (XOR-)MAPPED-ADDRESS attribute", True)
    return addr


def _decode_mapped(aval, txid, xor):
    if len(aval) < 4:
        raise StunParseError("mapped-address attr too short", True)
    family = aval[1]
    port = struct.unpack(">H", aval[2:4])[0]
    if xor:
        port ^= (STUN_MAGIC_COOKIE >> 16)
    if family == 0x01:               # IPv4
        if len(aval) < 8:
            raise StunParseError("IPv4 mapped-address too short", True)
        raw = bytearray(aval[4:8])
        if xor:
            ck = struct.pack(">I", STUN_MAGIC_COOKIE)
            raw = bytes(b ^ c for b, c in zip(raw, ck))
        ip = ".".join(str(b) for b in raw)
        return (4, ip, port)
    if family == 0x02:               # IPv6
        if len(aval) < 20:
            raise StunParseError("IPv6 mapped-address too short", True)
        raw = bytearray(aval[4:20])
        if xor:
            key = struct.pack(">I", STUN_MAGIC_COOKIE) + txid
            raw = bytes(b ^ c for b, c in zip(raw, key))
        parts = [raw[i:i + 2].hex() for i in range(0, 16, 2)]
        return (6, ":".join(parts), port)
    raise StunParseError("unknown address family 0x%02x" % family, True)


def ip_is_plausible_public(family, ip):
    """True if `ip` looks like a routable public address (not loopback / RFC1918
    / link-local / unspecified).  A public reflexive address is what a STUN
    server MUST return; anything else means we did not really reach the wire."""
    if family == 4:
        octs = [int(x) for x in ip.split(".")]
        if len(octs) != 4:
            return False
        a, b = octs[0], octs[1]
        if a == 0 or a == 127:
            return False
        if a == 10:
            return False
        if a == 172 and 16 <= b <= 31:
            return False
        if a == 192 and b == 168:
            return False
        if a == 169 and b == 254:
            return False
        if a >= 224:                 # multicast / reserved
            return False
        return True
    # IPv6: reject ::, ::1, fe80::/10, fc00::/7 (ULA)
    low = ip.lower().replace("::", ":")
    if ip in ("::", "0000:0000:0000:0000:0000:0000:0000:0000"):
        return False
    first = ip.split(":")[0]
    try:
        w = int(first or "0", 16)
    except ValueError:
        return False
    if w == 0:                       # ::1 loopback / unspecified block
        return False
    if 0xfe80 <= w <= 0xfebf:        # link-local
        return False
    if 0xfc00 <= w <= 0xfdff:        # unique-local
        return False
    return True


# ----------------------------------------------------------------- NTP -------
# 48-byte NTP packet.  byte0 = LI(2) | VN(3) | Mode(3).  Client: LI=0, VN=4,
# Mode=3 -> 0x23.  Transmit Timestamp is bytes 40..47; the server copies it into
# the Originate Timestamp (bytes 24..31) of its reply, which is our echo token.
NTP_CLIENT_B0 = 0x23


def ntp_client_packet(token8):
    """48-byte NTP client request with `token8` (8 bytes) in Transmit Timestamp."""
    if len(token8) != 8:
        raise ValueError("token8 must be 8 bytes")
    pkt = bytearray(48)
    pkt[0] = NTP_CLIENT_B0
    pkt[40:48] = token8              # Transmit Timestamp
    return bytes(pkt)


class NtpParseError(Exception):
    pass


def ntp_parse_response(data, expect_token8):
    """Validate a 48-byte NTP reply.  Returns stratum on success.

    ENV (not a finding): short reply, or Originate != our Transmit (not our
    exchange / kiss-o'-death rewrite).  BUG: originate matches (our exchange)
    but mode/version fields are inconsistent.
    """
    if len(data) < 48:
        raise NtpParseError("short reply (%d < 48)" % len(data), False)
    b0 = data[0]
    mode = b0 & 0x07
    version = (b0 >> 3) & 0x07
    stratum = data[1]
    originate = bytes(data[24:32])
    if originate != expect_token8:
        # Not our exchange (or a server that rewrote it) -> ENV.
        raise NtpParseError("originate != our transmit (not our exchange)", False)
    # It IS our exchange: field inconsistencies are a transport BUG.
    if mode != 4:
        raise NtpParseError("mode %d != 4 (server)" % mode, True)
    if version not in (3, 4):
        raise NtpParseError("implausible version %d" % version, True)
    if stratum == 0 or stratum > 15:
        # stratum 0 = kiss-o'-death, >15 = unsynchronized: ENV, not our transport.
        raise NtpParseError("stratum %d (kiss/unsynced)" % stratum, False)
    return stratum


# ---------------------------------------------------------------- MQTT -------
# MQTT 3.1.1 CONNECT -> CONNACK.  (No transaction token in the protocol; the
# oracle is CONNACK framing + return code.)
def mqtt_connect_packet(client_id):
    """MQTT 3.1.1 CONNECT with the given ASCII client_id."""
    cid = client_id.encode("ascii")
    var = b"\x00\x04MQTT" + b"\x04" + b"\x02" + struct.pack(">H", 60)  # level4, clean, keepalive60
    payload = struct.pack(">H", len(cid)) + cid
    body = var + payload
    return b"\x10" + _mqtt_remaining_length(len(body)) + body


def _mqtt_remaining_length(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


class MqttParseError(Exception):
    pass


def mqtt_parse_connack(data):
    """Validate a CONNACK.  Returns the connect return code (0 == accepted).

    Return codes 1..5 (bad version / id rejected / unavailable / bad auth / not
    authorized) are ENV refusals, not transport bugs.  A byte0/length that
    disagrees with the framed bytes IS a transport bug.
    """
    if len(data) < 4:
        raise MqttParseError("short CONNACK (%d < 4)" % len(data), True)
    if data[0] != 0x20:
        raise MqttParseError("byte0 0x%02x != 0x20 (CONNACK)" % data[0], True)
    if data[1] != 0x02:
        raise MqttParseError("remaining-length %d != 2" % data[1], True)
    return data[3]                   # connect return code


# ------------------------------------------------------------- self test -----
def _selftest():
    import os
    # STUN v4 round trip: build a response with a known public IP and decode it.
    txid = os.urandom(12)
    req = stun_binding_request(txid)
    assert len(req) == 20 and req[:2] == b"\x00\x01"
    pub_ip, pub_port = "203.0.113.7", 54321
    ipb = bytes(int(x) for x in pub_ip.split("."))
    xip = bytes(b ^ c for b, c in zip(ipb, struct.pack(">I", STUN_MAGIC_COOKIE)))
    xport = pub_port ^ (STUN_MAGIC_COOKIE >> 16)
    attr_val = b"\x00\x01" + struct.pack(">H", xport) + xip
    attr = struct.pack(">HH", ATTR_XOR_MAPPED_ADDRESS, len(attr_val)) + attr_val
    resp = struct.pack(">HHI12s", STUN_BINDING_SUCCESS, len(attr),
                       STUN_MAGIC_COOKIE, txid) + attr
    fam, ip, port = stun_parse_response(resp, txid)
    assert (fam, ip, port) == (4, pub_ip, pub_port), (fam, ip, port)
    assert ip_is_plausible_public(4, pub_ip)
    assert not ip_is_plausible_public(4, "192.168.1.5")
    assert not ip_is_plausible_public(4, "127.0.0.1")
    # STUN v6 round trip.
    pub6 = "2001:0db8:0000:0000:0000:0000:0000:0001"
    r6 = bytearray(16)
    for i, part in enumerate(pub6.split(":")):
        r6[i * 2:i * 2 + 2] = int(part, 16).to_bytes(2, "big")
    key = struct.pack(">I", STUN_MAGIC_COOKIE) + txid
    x6 = bytes(b ^ c for b, c in zip(r6, key))
    v = b"\x00\x02" + struct.pack(">H", (12345 ^ (STUN_MAGIC_COOKIE >> 16))) + x6
    a6 = struct.pack(">HH", ATTR_XOR_MAPPED_ADDRESS, len(v)) + v
    resp6 = struct.pack(">HHI12s", STUN_BINDING_SUCCESS, len(a6),
                        STUN_MAGIC_COOKIE, txid) + a6
    fam6, ip6, port6 = stun_parse_response(resp6, txid)
    assert fam6 == 6 and port6 == 12345, (fam6, ip6, port6)
    assert ip_is_plausible_public(6, ip6)
    # txid mismatch -> not-our-transaction (txid_matches False)
    try:
        stun_parse_response(resp, os.urandom(12))
        assert False, "expected txid mismatch"
    except StunParseError as e:
        assert e.args[1] is False
    # NTP round trip.
    tok = os.urandom(8)
    npkt = ntp_client_packet(tok)
    assert len(npkt) == 48 and npkt[0] == 0x23 and npkt[40:48] == tok
    reply = bytearray(48)
    reply[0] = (0 << 6) | (4 << 3) | 4          # LI0 VN4 mode4
    reply[1] = 2                                 # stratum 2
    reply[24:32] = tok                           # originate = our transmit
    assert ntp_parse_response(bytes(reply), tok) == 2
    reply[24] ^= 0xFF                            # break originate -> ENV
    try:
        ntp_parse_response(bytes(reply), tok)
        assert False
    except NtpParseError as e:
        assert e.args[1] is False
    # MQTT round trip.
    cpkt = mqtt_connect_packet("pygo-test-1234")
    assert cpkt[0] == 0x10
    connack = b"\x20\x02\x00\x00"
    assert mqtt_parse_connack(connack) == 0
    try:
        mqtt_parse_connack(b"\x30\x02\x00\x00")
        assert False
    except MqttParseError as e:
        assert e.args[1] is True                # wrong type = transport bug
    print("protocols.py self-test: OK")


if __name__ == "__main__":
    _selftest()
