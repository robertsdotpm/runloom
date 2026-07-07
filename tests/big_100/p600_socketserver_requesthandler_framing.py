"""big_100 / 600 -- socketserver StreamRequestHandler request/response framing
carried across a mid-response cooperative park, single-owner AF_UNIX loopback,
under M:N.

socketserver is the stdlib framework behind TCPServer / UnixStreamServer and the
Base/StreamRequestHandler request-dispatch machinery.  A ``UnixStreamServer`` binds
+ listens on a filesystem socket; ``BaseServer.handle_request()`` accepts ONE
connection (get_request -> accept), verifies it, and dispatches it to a freshly
constructed ``RequestHandlerClass(request, client_address, server)`` -- whose
``setup()`` wraps the accepted socket in a buffered ``self.rfile`` (a
BufferedReader) and an unbuffered ``self.wfile``, runs ``handle()``, then
``finish()`` flushes + closes those file objects and socketserver closes the
accepted socket.  The load-bearing state under attack is the per-connection
framing carried on the handler's grown-down C stack ACROSS a cooperative park:

  * the buffered ``self.rfile`` read position (how many request bytes remain);
  * the accepted-socket fd + the wfile buffer the response is written through;
  * the recv/send offsets while a response is delivered in TWO wire slices.

WHERE M:N COULD BREAK IT.  runloom runs each fiber on its own grown-down C stack
and migrates parked fibers across hubs.  A single handler fiber, mid-``handle()``,
does ``self.wfile.write(first_slice); flush; sleep; write(tail); flush`` while the
client reads the response in two ``recv`` calls straddling a ``yield`` -- the second
client read PARKS on recv (the handler is sleeping between its two wire writes), a
real cross-hub park.  If, on resume (possibly on a different hub), the handler's
wfile/socket state, or the client's recv offset, or the buffered rfile read
position for the NEXT request has desynced, the reassembled response will not match
the wid+salt-tagged bytes the server computed -- a torn/dropped/duplicated region,
or a cross-fiber leak of a sibling connection's bytes.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner, closed-world).

  SINGLE-OWNER AF_UNIX LOOPBACK (worker, HARD, fail-fast).  Each worker owns:
    * its OWN ``UnixStreamServer`` on a PRIVATE, per-wid socket path
      (``.../p600_wNNN.sock``) -- no sibling ever binds/connects that path, so the
      whole server + accepted connection + handler is single-owner;
    * ONE persistent client connection to that path (established once), over which
      it drives many request/response exchanges (one per round) -- so there is NO
      per-round port/inode churn (AF_UNIX has no ephemeral-port / TIME_WAIT limit);
    * a module-level ``RoundTripHandler`` whose single ``handle()`` LOOPS: it reads
      a fixed-length request, recovers the salt the client encoded, rebuilds the
      exact request tail and asserts it (a torn request fails server-side), then
      writes back a wid+salt-tagged response in TWO wire slices with a tiny sleep
      between (forcing the client's second read to park), and loops until the
      client closes (EOF) -- so one ``handle_request()`` covers all rounds.
  Per exchange the CLIENT sends a request whose 8-byte prefix is the round's salt
  and whose tail is ``make_frame(wid, salt)`` (every byte self-identifies its
  owner), then reads the response in ``recv_exactly(HALF)`` / ``yield_now()`` /
  ``recv_exactly(rest)`` and asserts the reassembled bytes are EXACTLY
  ``make_frame(wid, salt, RESP_LEN)``.  Because the server, both socket ends, the
  handler, and the expected bytes are all single-owner, a mismatch is NOT the
  documented "a shared handler/socket is not concurrency-safe" semantics -- it is a
  runloom framing desync across the park (torn/dropped/leaked bytes, a corrupted
  recv/send offset, or a buffered-rfile read-position leak between requests).

  COMPLETENESS (post, HARD): require_no_lost -- a handler parked mid-write, or a
  client parked in recv on a lost wakeup, never returns; the watchdog +
  require_no_lost catch it.

  NON-VACUITY (post, HARD): the load-bearing exchange actually ran (done > 0).

There is NO shared-mutable arm by construction: every server, socket, and handler
is single-owner per worker, which is exactly what makes a FAIL meaningful.  A
shared socketserver instance under M:N would race like any shared object across
threads (documented behavior) and is deliberately NOT tested.

TRANSIENT OS ERRORS ARE NOT FAILURES.  Under the forever-loop's sustained churn an
occasional ``OSError`` (a peer reset during teardown, a bind race after unlink) is
an environment/scale condition, not a runtime bug: the worker swallows OSError and
only ever calls ``H.fail`` on a BYTE-LEVEL framing/isolation mismatch on a fully
received response.  fd-heavy, so ``max_funcs`` caps the forever-loop's --funcs.

Stresses: socketserver.UnixStreamServer bind/listen/handle_request/get_request
(accept), StreamRequestHandler setup/handle/finish, the buffered rfile read
position + unbuffered wfile carried across a mid-response cooperative recv park,
per-fiber connection isolation.
"""
import os
import socket
import socketserver

import harness
import runloom

# Fixed request / response geometry.  RESP_LEN is odd so HALF != tail and both wire
# slices are non-empty; SPLIT sends >HALF of the response first (so recv_exactly(HALF)
# is satisfied without parking, leaving the framing mid-response) and a non-empty
# tail (so the SECOND recv genuinely parks on the writer's sleep).
REQ_LEN = 128
RESP_LEN = 201
HALF = RESP_LEN // 2                        # first recv_exactly; leaves framing mid-body
SPLIT = (RESP_LEN * 2) // 3                 # first wire slice: header-free, >HALF bytes
SALT_PREFIX = 8                             # request bytes [0:8] = zero-padded salt

# Tiny pause the handler takes BETWEEN its two wire writes so the client reliably
# reaches its second recv, parks, and only then gets the tail -- the cross-hub park
# window the framing state must survive.
WRITER_GAP = 0.0004


def make_frame(wid, salt, n):
    """A fixed n-byte payload whose repeated tag ``W<wid>R<salt>.`` self-identifies
    its owner: any wrong/leaked/torn byte is caught against a known expectation.
    Contains no digits-only 8-char run that could be confused with the salt prefix
    on the request path (the tag has letters), and no newline."""
    tag = "W{0}R{1}.".format(wid, salt).encode("ascii")
    reps = (n // len(tag)) + 1
    return (tag * reps)[:n]


def make_request(wid, salt):
    """The REQ_LEN-byte request for one exchange: an 8-byte zero-padded salt prefix
    the server parses back, followed by a wid+salt-tagged tail.  A torn request tail
    (or a wrong salt) is detected SERVER-side; a torn response is detected client-side."""
    prefix = ("%08d" % (salt % 100000000)).encode("ascii")
    return prefix + make_frame(wid, salt, REQ_LEN - SALT_PREFIX)


def recv_exactly(sock, n):
    """recv exactly n bytes, or fewer on EOF (clean teardown).  The caller checks
    the returned length: a short read while the run is live + un-failed is a real
    truncation; a short read during teardown/after a fail is benign."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            break
        buf += chunk
    return bytes(buf)


# ---- module-level single-owner handler -----------------------------------
class RoundTripHandler(socketserver.StreamRequestHandler):
    """One ``handle()`` per accepted connection; LOOPS one request/response exchange
    per iteration until the client closes (rfile EOF).  Everything it touches --
    the accepted socket, self.rfile/self.wfile, and its server -- is single-owner
    (this worker's private server), so a fail here is a runloom framing desync, not
    shared-object contention.  Writes the response in TWO wire slices with a sleep
    between to force the client's second recv to park."""

    def handle(self):
        srv = self.server
        H = srv.harness
        wid = srv.wid
        while True:
            req = self.rfile.read(REQ_LEN)
            if len(req) < REQ_LEN:
                return                     # EOF / clean teardown -> handler done
            try:
                salt = int(req[:SALT_PREFIX])
            except ValueError:
                H.fail("socketserver handler wid={0}: request salt prefix {1!r} is "
                       "not an int -- the buffered rfile read position desynced or a "
                       "sibling connection's bytes leaked into this request".format(
                           wid, req[:SALT_PREFIX]))
                return
            exp_tail = make_frame(wid, salt, REQ_LEN - SALT_PREFIX)
            if req[SALT_PREFIX:] != exp_tail:
                pos = next((i for i in range(len(exp_tail))
                            if req[SALT_PREFIX + i] != exp_tail[i]), -1)
                H.fail("socketserver handler wid={0} salt={1}: request tail torn at "
                       "byte {2} (got {3!r} want {4!r}) -- the buffered rfile read "
                       "position desynced or a cross-connection leak".format(
                           wid, salt, pos, req[SALT_PREFIX + pos:SALT_PREFIX + pos + 8],
                           exp_tail[pos:pos + 8]))
                return
            resp = make_frame(wid, salt, RESP_LEN)
            try:
                self.wfile.write(resp[:SPLIT])
                self.wfile.flush()
                runloom.sleep(WRITER_GAP)   # client parks on its 2nd recv here
                self.wfile.write(resp[SPLIT:])
                self.wfile.flush()
            except OSError:
                return                     # peer closed during teardown -> done


class RoundTripServer(socketserver.UnixStreamServer):
    """Per-worker single-owner Unix-domain stream server.  allow_reuse_address lets
    a re-bind after unlink succeed under sustained forever-loop churn."""
    allow_reuse_address = True


def serve_conn(H, server, wg):
    """Accept + dispatch exactly ONE connection (the worker's persistent client);
    the handler loops all exchanges until the client closes.  Single-owner."""
    try:
        server.handle_request()
    except OSError:
        pass
    finally:
        wg.done()


def do_exchange(H, wid, salt, csock):
    """One single-owner request/response round-trip over the persistent connection.
    Sends the wid+salt-tagged request, then reads the response in two recv calls
    straddling a yield (the second parks on the handler's mid-response sleep) and
    asserts the reassembled bytes are EXACTLY the wid+salt frame.  Returns True on a
    verified exchange, False on a clean-teardown short read.  Calls H.fail ONLY on a
    byte-level framing/isolation mismatch of a fully received response."""
    csock.sendall(make_request(wid, salt))
    expected = make_frame(wid, salt, RESP_LEN)

    first = recv_exactly(csock, HALF)
    if len(first) < HALF:
        return False                       # EOF -> teardown / server-side fail
    runloom.yield_now()                    # force a sibling interleave at the hazard
    rest = recv_exactly(csock, RESP_LEN - HALF)   # PARKS on recv for the tail slice
    got = first + rest

    if len(got) != RESP_LEN:
        if not H.running() or H.failed:
            return False                   # short read during teardown -> benign
        H.fail("socketserver client wid={0} salt={1}: response length {2} != {3} -- "
               "the handler's wfile/socket framing desynced across the mid-response "
               "recv park (bytes were {4})".format(
                   wid, salt, len(got), RESP_LEN,
                   "DROPPED" if len(got) < RESP_LEN else "DUPLICATED"))
        return False
    if got != expected:
        pos = next((i for i in range(RESP_LEN) if got[i] != expected[i]), -1)
        H.fail("socketserver client wid={0} salt={1}: response MISMATCH at byte {2} "
               "(got {3!r} want {4!r}) -- the framing state was corrupted across the "
               "park or leaked a sibling connection's bytes".format(
                   wid, salt, pos, got[pos:pos + 8], expected[pos:pos + 8]))
        return False
    return True


def worker(H, wid, rng, state):
    """Single-owner: build this worker's private Unix server, open ONE persistent
    client connection, and drive one request/response exchange per round over it.
    No per-round port/inode churn; a fail is a runloom framing desync."""
    tmpdir = state["tmpdir"]
    path = os.path.join(tmpdir, "p600_w{0}.sock".format(wid))
    try:
        os.unlink(path)                    # clear a stale path from a prior forever-loop
    except OSError:
        pass

    try:
        server = RoundTripServer(path, RoundTripHandler)
    except OSError:
        return                             # bind race under churn -> not a runtime bug
    server.harness = H
    server.wid = wid

    csock = None
    wg = runloom.WaitGroup()
    wg.add(1)
    server_spawned = False
    try:
        H.fiber(serve_conn, H, server, wg)
        server_spawned = True

        csock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        csock.connect(path)
        H.register_close(csock)            # teardown closes -> handler sees EOF, returns

        idx = 0
        for _ in H.round_range():
            if not H.running():
                break
            salt = idx & 0xFFFFFF
            ok = do_exchange(H, wid, salt, csock)
            if H.failed:
                break
            if ok:
                state["done"][wid] += 1    # single-writer-per-slot, race-free
                H.op(wid)
                H.task_done(wid)
            else:
                break                      # clean EOF -> connection gone, stop
            idx += 1
    except OSError:
        # Transient churn error (peer reset during teardown, connect race) -- not a
        # runtime bug; the byte-level oracle above is the only thing that fails.
        pass
    finally:
        if csock is not None:
            try:
                csock.close()              # -> handler rfile EOF -> handler returns
            except OSError:
                pass
        if server_spawned:
            wg.wait()                      # handler fully drained before teardown
        try:
            server.server_close()
        except OSError:
            pass
        try:
            os.unlink(path)
        except OSError:
            pass


def setup(H):
    # tmpdir holds the per-worker Unix socket files (rmtree'd at the end).  ``done``
    # is one race-free slot per worker (single writer) feeding the NON-VACUITY check.
    H.state = {
        "tmpdir": H.make_tmpdir("big100_p600_"),
        "done": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    ndone = sum(H.state["done"])
    H.log("socketserver single-owner exchanges verified: {0} (every request-tail + "
          "response-framing oracle passed fail-fast); ops={1}".format(
              ndone, H.total_ops()))

    # NON-VACUITY: the load-bearing request/response framing hazard actually ran.
    H.check(ndone > 0,
            "no socketserver exchanges completed -- the StreamRequestHandler "
            "request/response framing hazard across the mid-response recv park was "
            "never exercised (oracle would be vacuous)")

    # COMPLETENESS: no handler parked mid-write / client parked in recv on a lost
    # wakeup vanished without returning.
    H.require_no_lost("socketserver request/response framing")


if __name__ == "__main__":
    harness.main(
        "p600_socketserver_requesthandler_framing", body, setup=setup, post=post,
        default_funcs=512, max_funcs=512,
        describe="socketserver.UnixStreamServer + StreamRequestHandler "
                 "request/response framing carried across a mid-response cooperative "
                 "recv park.  Single-owner AF_UNIX loopback: each worker owns a "
                 "private server on a per-wid socket path and one persistent client "
                 "connection, driving one wid+salt-tagged request/response exchange "
                 "per round.  The handler reads a fixed-length request (recovering "
                 "the salt + asserting the tail), then writes a tagged response in "
                 "two wire slices with a sleep between so the client's second recv "
                 "parks; every response byte MUST exactly match what the server "
                 "computed.  A torn request tail, a response length/content "
                 "mismatch, or a cross-connection byte leak is a runloom framing "
                 "desync across the park")
