import socket, traceback

# Stock behavior first (before patching)
print("--- stock ---")
try:
    r = socket.getaddrinfo("ip6-loopback", 80, socket.AF_INET)
    print("stock getaddrinfo:", r)
except Exception as e:
    print("stock getaddrinfo raised:", type(e).__name__, e)
try:
    r = socket.gethostbyname("ip6-loopback")
    print("stock gethostbyname:", r)
except Exception as e:
    print("stock gethostbyname raised:", type(e).__name__, e)

import runloom.monkey as monkey
monkey.patch()

print("--- patched ---")
try:
    r = socket.getaddrinfo("ip6-loopback", 80, socket.AF_INET)
    print("patched getaddrinfo:", r)
except Exception as e:
    print("patched getaddrinfo raised:", type(e).__name__, e)
try:
    r = socket.gethostbyname("ip6-loopback")
    print("patched gethostbyname:", r)
except Exception as e:
    print("patched gethostbyname raised:", type(e).__name__)
    traceback.print_exc()
