import socket
import runloom.monkey as monkey
monkey.patch()
try:
    r = socket.getaddrinfo("example.com", 80)
    print("ascii family=0: resolved ->", [ai[4] for ai in r][:2])
except Exception as e:
    print("ascii family=0: raised %s: %s" % (type(e).__name__, e))
