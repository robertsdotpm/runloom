"""Fake DNS server (stock python, no runloom).
Answers A 1.2.3.4 for mysvc.corp.example.; NOERROR/0-answers for its AAAA;
NXDOMAIN for everything else. Logs every query to the file in argv[1].
"""
import socket, struct, sys

LOG = open(sys.argv[1], "a", buffering=1)
TARGET = "mysvc.corp.example"

def parse_qname(data):
    off = 12
    labels = []
    while data[off] != 0:
        n = data[off]
        labels.append(data[off+1:off+1+n].decode())
        off += 1 + n
    off += 1
    qtype = struct.unpack("!H", data[off:off+2])[0]
    return ".".join(labels), qtype, off + 4  # end of question section

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", 53))
print("dnsserver ready", flush=True)
while True:
    data, peer = s.recvfrom(4096)
    try:
        name, qtype, qend = parse_qname(data)
    except Exception:
        continue
    LOG.write("QUERY name=%r qtype=%d\n" % (name, qtype))
    txn = data[:2]
    question = data[12:qend]
    if name.lower() == TARGET and qtype == 1:      # A -> answer
        hdr = txn + struct.pack("!HHHHH", 0x8180, 1, 1, 0, 0)
        ans = b"\xc0\x0c" + struct.pack("!HHIH", 1, 1, 60, 4) + socket.inet_aton("1.2.3.4")
        s.sendto(hdr + question + ans, peer)
    elif name.lower() == TARGET:                    # AAAA etc -> NOERROR, 0 ans
        hdr = txn + struct.pack("!HHHHH", 0x8180, 1, 0, 0, 0)
        s.sendto(hdr + question, peer)
    else:                                           # NXDOMAIN
        hdr = txn + struct.pack("!HHHHH", 0x8183, 1, 0, 0, 0)
        s.sendto(hdr + question, peer)
