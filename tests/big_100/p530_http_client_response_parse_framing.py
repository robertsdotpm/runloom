"""big_100 / 530 -- http.client.HTTPResponse body-framing state carried across a
mid-body park, single-owner loopback, under M:N.

http.client.HTTPResponse is the stdlib HTTP/1.1 response PARSER.  Constructed over
a socket, it does ``self.fp = sock.makefile("rb")`` and then, in ``begin()``, reads
the status line + headers off that buffered reader; ``read()`` / ``read(amt)`` then
decode the BODY according to the framing it recorded during ``begin()``.  The
load-bearing framing state is a small pile of INSTANCE attributes:

  * ``self.length``      -- remaining Content-Length bytes (length-delimited body);
  * ``self.chunked`` / ``self.chunk_left`` -- the chunked-transfer decoder's state,
    where ``chunk_left`` is how many bytes remain IN THE CURRENT CHUNK;
  * ``self.fp``          -- the BufferedReader wrapping the cooperative socket.

The exact non-atomic thing under attack is that ``(self.length)`` OR
``(self.chunk_left)`` -- a plain instance int -- is carried, on a grown-down C
stack, ACROSS a cooperative park.  We force the park by splitting the response on
the wire: a writer fiber sends the header + the first slice of the body, sleeps,
then sends the rest.  The reader (this worker) does ``resp.read(half)`` (satisfied
from the first slice, leaving ``chunk_left`` / ``length`` mid-body), then YIELDS,
then ``resp.read()`` (which must recv the second slice -- a REAL cross-hub park
between two partial body reads).  If, on resume (possibly on a different hub), the
HTTPResponse's framing state has desynced -- length off by the bytes consumed
before the park, chunk_left pointing into the wrong chunk, or fp's buffer torn --
the reassembled body / headers will not match what was written.

WHICH ORACLE IS LOAD-BEARING, AND WHY.

  SINGLE-OWNER LOOPBACK (worker, HARD, fail-fast).  Each round a worker:
    * makes a PRIVATE ``socket.socketpair()`` -- (rsock, wsock) -- owned only by
      this round; no sibling ever touches either end (register/close per round);
    * builds a COMPLETE HTTP/1.1 response whose reason phrase, an ``X-Wid`` header,
      an ``X-Salt`` header, and every byte of the BODY embed THIS fiber's wid+salt,
      so a cross-fiber leak (reading a sibling's framing/body) yields a WRONG,
      detectable value.  Two variants, round-robined by (wid+idx)%2:
        - CASE_CLEN    : Content-Length delimited body   (drives ``self.length``);
        - CASE_CHUNKED : chunked transfer-encoded body    (drives ``self.chunk_left``);
    * spawns a writer fiber that sends the response in TWO wire writes split inside
      the body (header + leading body slice, then a tiny sleep, then the tail +
      chunk terminator), so the reader's SECOND body read genuinely parks on recv;
    * parses with its OWN HTTPResponse: ``begin()``, then ``read(half)`` /
      ``yield_now()`` / ``read()``, and asserts status==200, reason==expected,
      X-Wid==wid, X-Salt==salt, and body==the exact wid+salt-tagged bytes.
    The HTTPResponse object, both socket ends, and the expected bytes are all
    single-owner (per round, one fiber) -- so a mismatch is NOT the documented
    "HTTPResponse is not shared-safe" semantics; it is a runloom framing-state
    desync across the park (a real runtime bug: lost/torn body, framing int
    corrupted across the yield, or a cross-fiber leak of one fiber's parser state).

  COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-parse (parked
  in ``resp.read()`` on a lost wakeup, or its writer never resumed) never returns;
  the watchdog + require_no_lost catch it.

  NON-VACUITY (post, HARD): both the Content-Length AND the chunked framing paths
  actually ran (each per-wid tally > 0) -- else the oracle would be vacuous.

There is NO shared-mutable arm here by construction: every parser and every socket
end is single-owner per round, which is exactly what makes a FAIL meaningful.  A
shared HTTPResponse under M:N would race like any shared object across threads
(documented Python behavior) and is deliberately NOT tested.

Bounded pool: each round holds one socketpair (2 fds) for the duration of the
round only, and closes both ends in a finally -- so max_funcs=512 caps the live
fd count and the forever-loop's --funcs 1000000 never explodes the fd table.

Stresses: http.client.HTTPResponse.begin() status/header parse, Content-Length and
chunked body framing (self.length / self.chunk_left carried across a cooperative
recv park), makefile()/BufferedReader over the cooperative socket, two-partial body
read with a mid-body cross-hub park, per-fiber parser isolation.
"""
import http.client
import socket

import harness
import runloom

# ---- the wid+salt-tagged response ----------------------------------------
# The body is a fixed-length run of the fiber's own "W<wid>R<salt>" tag so EVERY
# stretch of the reassembled body self-identifies its owner: a cross-fiber leak or
# a torn/duplicated/dropped region shows up as a wrong byte at a known position.
# Length is odd so half != tail and both wire slices are non-empty.
BODY_LEN = 201
HALF = BODY_LEN // 2                       # first read(amt); leaves framing mid-body

# The two framing variants, round-robined by (wid+idx)%2 so BOTH are exercised
# whether one worker does many rounds or many workers do one each (the p125/p126/
# p172 flaky-random-coverage lesson: deterministic round-robin, not rng choice).
CASE_CLEN = 0                              # Content-Length body -> self.length
CASE_CHUNKED = 1                           # chunked body        -> self.chunk_left
NCASES = 2

# Tiny pause the writer takes BETWEEN the two wire slices so the reader reliably
# reaches its second read(), parks on recv, and only then gets the tail -- the
# cross-hub park window the framing state must survive.
WRITER_GAP = 0.0004


def make_body(wid, salt):
    """The fixed BODY_LEN-byte tagged body for (wid, salt).  Repeats this fiber's
    unique tag so any wrong/leaked/torn byte is caught against a known expectation."""
    tag = "W{0}R{1}.".format(wid, salt).encode("ascii")
    reps = (BODY_LEN // len(tag)) + 1
    return (tag * reps)[:BODY_LEN]


def enc_chunk(data):
    """One chunked-transfer chunk: hex-length CRLF data CRLF."""
    return "{0:x}\r\n".format(len(data)).encode("ascii") + data + b"\r\n"


def build_response(wid, salt, case):
    """Return (response_bytes, split_point, reason, body).

    split_point is a wire offset INSIDE the body region: the writer sends
    response[:split_point] first, then (after a gap) response[split_point:].  It is
    chosen so (a) the whole header block AND at least HALF the DECODED body live in
    the first slice (so read(HALF) is satisfied without parking, leaving framing
    state mid-body), and (b) the tail is non-empty (so read() genuinely parks)."""
    reason = "OKW{0}R{1}".format(wid, salt)     # no spaces -> exact reason match
    body = make_body(wid, salt)
    head = ("HTTP/1.1 200 " + reason + "\r\n"
            "X-Wid: {0}\r\n"
            "X-Salt: {1}\r\n").format(wid, salt).encode("ascii")

    if case == CASE_CLEN:
        head += ("Content-Length: {0}\r\n".format(BODY_LEN)).encode("ascii")
        head += b"\r\n"
        wire_body = body
        # First slice carries the header + 2/3 of the body (>= HALF decoded bytes);
        # the tail is the remaining ~1/3, non-empty.
        first_body = (BODY_LEN * 2) // 3
        split = len(head) + first_body
        response = head + wire_body
    else:  # CASE_CHUNKED
        head += b"Transfer-Encoding: chunked\r\n"
        head += b"\r\n"
        cut = (BODY_LEN * 2) // 3               # chunk A holds 2/3 of the body
        chunk_a = enc_chunk(body[:cut])
        chunk_b = enc_chunk(body[cut:])
        wire_body = chunk_a + chunk_b + b"0\r\n\r\n"
        # Split right AFTER the complete chunk A: read(HALF) (HALF < cut) stops
        # mid-chunk-A leaving chunk_left > 0; read() must recv chunk B from the
        # tail -- the framing state (chunk_left) is carried across that park.
        split = len(head) + len(chunk_a)
        response = head + wire_body

    return response, split, reason, body


def writer_fiber(H, wsock, response, split, wg):
    """Send the response in two wire slices with a gap between so the reader parks
    on recv between its two partial body reads.  Single-owner write end."""
    try:
        try:
            wsock.sendall(response[:split])
            runloom.sleep(WRITER_GAP)
            wsock.sendall(response[split:])
            try:
                wsock.shutdown(socket.SHUT_WR)     # signal EOF (harmless for both)
            except OSError:
                pass
        except OSError:
            # Round tearing down / peer closed early -> nothing more to do.
            return
    finally:
        wg.done()


def read_body_two_parts(resp):
    """Read the body in TWO read() calls with a yield between, so the HTTPResponse
    framing state (self.length / self.chunk_left) is carried across a cooperative
    park (the second read must recv the writer's tail slice)."""
    first = resp.read(HALF)                # satisfied from the first wire slice
    runloom.yield_now()                    # force a sibling interleave at the hazard
    rest = resp.read()                     # PARKS on recv for the writer's tail
    return first + rest


def run_round(H, wid, idx, case, salt, state):
    """One single-owner loopback parse.  Everything (both socket ends, the
    HTTPResponse, the expected bytes) is owned by this round alone."""
    response, split, reason, body = build_response(wid, salt, case)

    rsock, wsock = socket.socketpair()
    rsock.setblocking(True)
    wsock.setblocking(True)

    wg = runloom.WaitGroup()
    wg.add(1)
    resp = None
    try:
        H.fiber(writer_fiber, H, wsock, response, split, wg)

        resp = http.client.HTTPResponse(rsock)
        try:
            resp.begin()
        except http.client.HTTPException as exc:
            if not H.running():
                return
            H.fail("wid={0} case={1}: HTTPResponse.begin() raised {2!r} on a "
                   "single-owner, well-formed response -- the status/header parse "
                   "desynced (framing state torn across the parse)".format(
                       wid, case, exc))
            return

        # ---- status line + headers ----
        if resp.status != 200:
            H.fail("wid={0} case={1}: parsed status {2} != 200 -- a cross-fiber "
                   "leak or torn status line".format(wid, case, resp.status))
            return
        if resp.reason != reason:
            H.fail("wid={0} case={1}: parsed reason {2!r} != expected {3!r} -- the "
                   "reason phrase was torn or leaked from a sibling's response".format(
                       wid, case, resp.reason, reason))
            return
        got_wid = resp.getheader("X-Wid")
        if got_wid != str(wid):
            H.fail("wid={0} case={1}: X-Wid header {2!r} != {3!r} -- a cross-fiber "
                   "header leak: this parser read another fiber's response".format(
                       wid, case, got_wid, str(wid)))
            return
        got_salt = resp.getheader("X-Salt")
        if got_salt != str(salt):
            H.fail("wid={0} case={1}: X-Salt header {2!r} != {3!r} -- a cross-fiber "
                   "header leak across the parse".format(
                       wid, case, got_salt, str(salt)))
            return

        # ---- body framing across the mid-body park ----
        got_body = read_body_two_parts(resp)
        if len(got_body) != BODY_LEN:
            H.fail("wid={0} case={1}: body length {2} != {3} -- the {4} framing "
                   "state (length/chunk_left) desynced across the mid-body recv "
                   "park: bytes were {5}".format(
                       wid, case, len(got_body), BODY_LEN,
                       "Content-Length" if case == CASE_CLEN else "chunked",
                       "DROPPED" if len(got_body) < BODY_LEN else "DUPLICATED"))
            return
        if got_body != body:
            # Locate the first divergent byte for a precise report.
            pos = next((i for i in range(BODY_LEN) if got_body[i] != body[i]), -1)
            H.fail("wid={0} case={1}: body MISMATCH at byte {2} (got {3!r} want "
                   "{4!r}) -- the parser's framing state was corrupted across the "
                   "park or leaked a sibling fiber's body".format(
                       wid, case, pos,
                       got_body[pos:pos + 8], body[pos:pos + 8]))
            return

        wg.wait()                          # writer fully done before we tear down
        if case == CASE_CLEN:
            state["clen"][wid] += 1        # single-writer-per-slot, race-free
        else:
            state["chunk"][wid] += 1
    finally:
        # Per-round cleanup: HTTPResponse.close() drops the makefile buffer; both
        # socket ends must be closed so the fd table stays bounded across rounds.
        try:
            if resp is not None:
                resp.close()
        except OSError:
            pass
        try:
            rsock.close()
        except OSError:
            pass
        try:
            wsock.close()
        except OSError:
            pass
        # If we bailed early (H.failed / not running) the writer may still be
        # parked in sendall on the now-closed peer -- its OSError path calls
        # wg.done(); nothing to join here since the finally already ran.


def worker(H, wid, rng, state):
    idx = 0
    for _ in H.round_range():
        if not H.running():
            break
        case = (wid + idx) % NCASES
        salt = idx & 0xFFFFFF
        run_round(H, wid, idx, case, salt, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)
        idx += 1


def setup(H):
    # One race-free slot per worker (single writer per slot) for each framing
    # variant -- allocated where H.funcs is known.  These feed the NON-VACUITY
    # coverage check (both Content-Length and chunked paths must have run).
    H.state = {
        "clen": [0] * H.funcs,
        "chunk": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    nclen = sum(H.state["clen"])
    nchunk = sum(H.state["chunk"])
    H.log("http.client single-owner parses: Content-Length={0}, chunked={1} "
          "(every status/header/body oracle passed fail-fast); ops={2}".format(
              nclen, nchunk, H.total_ops()))

    # NON-VACUITY: the load-bearing framing hazard actually ran on BOTH paths.
    H.check(nclen > 0,
            "no Content-Length responses were parsed -- the self.length framing "
            "path across the mid-body park was never exercised (oracle vacuous)")
    H.check(nchunk > 0,
            "no chunked responses were parsed -- the self.chunk_left framing path "
            "across the mid-body park was never exercised (oracle vacuous)")

    # COMPLETENESS: no fiber stranded mid-parse (parked in resp.read() on a lost
    # wakeup, or a writer that never resumed).
    H.require_no_lost("http.client response framing")


if __name__ == "__main__":
    harness.main(
        "p530_http_client_response_parse_framing", body, setup=setup, post=post,
        default_funcs=512, max_funcs=512,
        describe="http.client.HTTPResponse body framing (self.length / "
                 "self.chunk_left) carried across a mid-body cooperative recv park. "
                 "Single-owner loopback: a wid+salt-tagged HTTP/1.1 response "
                 "(Content-Length AND chunked variants) written into one end of a "
                 "per-round socketpair is parsed on the other end; status, reason, "
                 "X-Wid/X-Salt headers, and every body byte MUST exactly match what "
                 "was written, with the body read in two read() calls straddling a "
                 "yield.  A body length/content mismatch, a wrong header, or a torn "
                 "status is a runloom framing-state desync across the park")
