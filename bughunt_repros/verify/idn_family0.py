import socket
import runloom.monkey as monkey
monkey.patch()
try:
    r = socket.getaddrinfo("bücher.de", 80)
    print("family=0: resolved ->", [ai[4] for ai in r])
except Exception as e:
    print("family=0: raised %s: %s" % (type(e).__name__, e))
