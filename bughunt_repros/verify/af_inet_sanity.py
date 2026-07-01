import socket
import runloom.monkey as monkey
monkey.patch()
try:
    r = socket.getaddrinfo("example.com", 80, socket.AF_INET, socket.SOCK_STREAM)
    print("ascii AF_INET: resolved ->", [ai[4] for ai in r][:2])
except Exception as e:
    print("ascii AF_INET: raised %s: %s" % (type(e).__name__, e))
