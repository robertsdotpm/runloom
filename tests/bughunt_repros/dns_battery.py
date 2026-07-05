import socket, sys

def t(tag, fn):
    try:
        r = fn()
        print("%-28s -> %r" % (tag, r), flush=True)
    except Exception as e:
        print("%-28s -> %s: %s" % (tag, type(e).__name__, e), flush=True)

def battery(prefix):
    print("=== %s ===" % prefix, flush=True)
    t("bytes literal host", lambda: socket.getaddrinfo(b"127.0.0.1", 80, socket.AF_INET, socket.SOCK_STREAM))
    t("bytes name host", lambda: socket.getaddrinfo(b"localhost", 80, socket.AF_INET, socket.SOCK_STREAM))
    t("port=None", lambda: socket.getaddrinfo("127.0.0.1", None, socket.AF_INET, socket.SOCK_STREAM))
    t("hosts v6-only AF_INET", lambda: socket.getaddrinfo("ip6-loopback", 80, socket.AF_INET, socket.SOCK_STREAM))
    t("gethostbyname v6-only", lambda: socket.gethostbyname("ip6-loopback"))
    t("IDN name", lambda: socket.getaddrinfo("bücher.de", 80, socket.AF_INET, socket.SOCK_STREAM))
    t("localhost AF_INET", lambda: socket.getaddrinfo("localhost", 80, socket.AF_INET, socket.SOCK_STREAM))

if sys.argv[1] == "stock":
    battery("stock")
else:
    import runloom
    def main():
        runloom.fiber(lambda: battery("patched-fiber"))
    runloom.monkey.patch()
    runloom.run(2, main)
