import socket
stock = socket.getaddrinfo('127.0.0.1', 80, socket.AF_INET, 0)
print("stock rows:", len(stock))
for r in stock: print("  ", r)
import runloom.monkey as monkey
monkey.patch(dns=True)
patched = socket.getaddrinfo('127.0.0.1', 80, socket.AF_INET, 0)
print("patched rows:", len(patched))
for r in patched: print("  ", r)
