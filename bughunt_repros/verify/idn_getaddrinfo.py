import socket, traceback

HOST = "bücher.de"

# 1) Stock behavior BEFORE patching
try:
    r = socket.getaddrinfo(HOST, 80, socket.AF_INET, socket.SOCK_STREAM)
    print("STOCK: resolved ->", [ai[4] for ai in r])
except Exception as e:
    print("STOCK: raised %s: %s" % (type(e).__name__, e))

# 2) Patched behavior
import runloom.monkey as monkey
monkey.patch()

try:
    r = socket.getaddrinfo(HOST, 80, socket.AF_INET, socket.SOCK_STREAM)
    print("PATCHED: resolved ->", [ai[4] for ai in r])
except Exception as e:
    print("PATCHED: raised %s: %s" % (type(e).__name__, e))
    print("PATCHED: is OSError subclass:", isinstance(e, OSError))
    traceback.print_exc()

# 3) Also check the dual-family (family=0) path
try:
    r = socket.getaddrinfo(HOST, 80)
    print("PATCHED family=0: resolved ->", [ai[4] for ai in r])
except Exception as e:
    print("PATCHED family=0: raised %s: %s" % (type(e).__name__, e))
