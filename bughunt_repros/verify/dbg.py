import socket
srv = socket.socket(); srv.bind(("localhost", 0)); srv.listen(1)
print("srv bound:", srv.getsockname())
c = socket.socket()
c.connect(("localhost", srv.getsockname()[1]))
print("plain python connect OK, peer:", c.getpeername())
print("hosts:", open("/etc/hosts").read())
