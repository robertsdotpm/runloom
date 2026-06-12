"""_tls_wrap_client: client-side TLS wrap helper."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .tls_bio import _MemoryBIOTLS  # noqa: F401

def _tls_wrap_client(raw, ssl_arg, server_hostname, host, handshake_timeout=None):
    """Wrap a freshly-connected client socket in cooperative TLS and finish
    the handshake.  ``ssl_arg`` is True (default context) or an SSLContext."""
    context = _ssl.create_default_context() if ssl_arg is True else ssl_arg
    if server_hostname is None and isinstance(host, str) and host:
        server_hostname = host
    tls = _MemoryBIOTLS(raw, context, server_side=False,
                   server_hostname=server_hostname)
    tls.do_handshake(handshake_timeout)
    return tls


# Python's per-thread C recursion counter is shared across all
# fibers on the OS thread.  Phase B saves/restores it per-g, but
# the absolute limit is still global -- spawning thousands of tasks
# can hit RecursionError just from the depth of asyncio's frame chain
# (Task.__step -> coro.send -> awaitable.__await__ -> Future.__await__).
# Runloom's __init__.py bumps the limit when imported; runloom.aio is often
# imported standalone so we do the same here.
if sys.getrecursionlimit() < 1_000_000:
    sys.setrecursionlimit(1_000_000)
