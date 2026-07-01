"""create_connection error path: if socket.socket(fam,...) itself raises
OSError on the FIRST getaddrinfo entry (e.g. EAFNOSUPPORT for AF_INET6 on an
IPv6-disabled host), the `except OSError:` handler does `s.close()` with `s`
unbound -> NameError swallows the fallback to the next address family."""
import sys, socket, asyncio
import runloom.aio as aio

real_socket = socket.socket
calls = []

class FailFirstSocket(socket.socket):
    def __init__(self, family=-1, type=-1, proto=-1, fileno=None):
        calls.append(family)
        if len(calls) == 1:
            raise OSError(97, "Address family not supported by protocol")
        super().__init__(family, type, proto, fileno)

async def main():
    loop = asyncio.get_event_loop()
    # Point at a listening socket so the fallback (2nd family/addr) succeeds.
    srv = real_socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    host, port = srv.getsockname()
    socket.socket = FailFirstSocket
    try:
        # Force two addrinfo entries for the same target so there is a
        # fallback candidate after the first failure.
        import runloom.aio.loop_net as ln
        orig_resolve = ln._resolve
        def fake_resolve(h, p, fam, typ, proto, flags):
            infos = orig_resolve(h, p, fam, typ, proto, flags)
            return infos + infos      # duplicate: first fails, second works
        ln._resolve = fake_resolve
        try:
            tr, proto = await loop.create_connection(asyncio.Protocol, host, port)
            tr.close()
            return "connected"
        finally:
            ln._resolve = orig_resolve
    finally:
        socket.socket = real_socket
        srv.close()

try:
    r = aio.run(main())
    print(r, "OK")
except NameError as e:
    print("BUG: NameError instead of address-family fallback: %r" % (e,))
    sys.exit(1)
except OSError as e:
    print("acceptable OSError (fallback still failed):", e)
