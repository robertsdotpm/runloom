import socket
try:
    res = socket.getaddrinfo("mysvc", 80, socket.AF_INET, socket.SOCK_STREAM)
    print("STOCK OK:", res[0][4][0])
except socket.gaierror as e:
    print("STOCK FAIL:", e)
