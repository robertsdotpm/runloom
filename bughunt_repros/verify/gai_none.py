import socket

# Stock behavior first
stock = socket.getaddrinfo('127.0.0.1', None, socket.AF_INET, socket.SOCK_STREAM)
print("stock:  ", stock[0])

import runloom.monkey as monkey
monkey.patch(dns=True)

patched = socket.getaddrinfo('127.0.0.1', None, socket.AF_INET, socket.SOCK_STREAM)
print("patched:", patched[0])

# Try the documented connect-to-sockaddr pattern
fam, st, proto, canon, sa = patched[0]
s = socket.socket(fam, st, proto)
try:
    s.connect(sa)
    print("connect: ok ->", s.getpeername())
except Exception as e:
    print("connect: %s: %s" % (type(e).__name__, e))
finally:
    s.close()

# also check type=0 expansion under stock
stock0 = socket.getaddrinfo('127.0.0.1', 80, socket.AF_INET, 0)
print("stock type=0 rows would be (unpatched shown below after unpatch):")
monkey  # keep reference
