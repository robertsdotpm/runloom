"""big_100 / 618 -- wsgiref.handlers.SimpleHandler single-owner WSGI round-trip.

wsgiref.handlers.SimpleHandler is the reference WSGI gateway: given (stdin,
stdout, stderr, environ) it drives a WSGI application to completion, iterating
the app's returned body and writing a full HTTP/1.0 response (status line +
headers + body) to the stdout stream.  Bound to fiber-local BytesIO streams and
a fiber-local environ it is a completely SELF-CONTAINED, single-owner, CPU-only
transformer: one fiber owns one handler + its three BytesIO streams + its app +
the exact bytes those produce.  The response bytes are a pure, closed-form
function of the fiber-local inputs (status line, header set, and body chunks the
app hands back), so they MUST come out bit-identical -- there is nothing shared
to race.

WHERE M:N COULD BREAK IT (the gap this program probes).  finish_response()
iterates the app's returned iterable and calls handler.write(chunk) per chunk,
buffering headers until the first write then flushing them into stdout.  We make
the app a GENERATOR that runloom.yield_now()s BETWEEN body chunks, so the
handler is suspended mid-response -- headers already flushed, some body chunks
written, more pending -- exactly while sibling fibers on other hubs are running
their OWN handlers writing THEIR own bodies into THEIR own BytesIO.  If runloom
ever leaked one fiber's write target, header buffer, or partially-built response
into another's (a cross-fiber leak of single-owner handler/stream state, a torn
BytesIO buffer, a lost/duplicated chunk), the captured response would contain a
sibling's status/header/body bytes, a wrong Content-Length, a truncated or
doubled body, or non-UTF8 garbage -- all of which the closed-form oracle catches.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  Each fiber builds a body from several
chunks carrying its OWN unique per-fiber token (wid + round + a derived nonce),
plus a response header X-Fiber-Token carrying that same token and a status line
"2NN <reason>" whose code is fiber-derived.  It runs the handler across the
in-response yields, then parses the captured stdout:
  * the status line equals the exact fiber-local "HTTP/1.0 <code> <reason>";
  * the X-Fiber-Token header equals this fiber's token (NOT a sibling's) --
    a cross-fiber header leak;
  * Content-Length equals the true body length;
  * the body bytes equal this fiber's expected body EXACTLY (no lost/doubled
    chunk, no torn buffer, no sibling body);
  * stderr captured nothing (the handler logged no error).
Single-owner: the handler, its stdin/stdout/stderr BytesIO, the environ, the app
closure, and the expected bytes are all created inside this fiber and never
shared.  Only the Date header (wall-clock, non-deterministic by design) is
ignored.  On a correct runtime every round-trip is bit-identical to its closed
form, so the program exits 0 when there is no bug.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-
    finish_response (parked inside the generator's yield, never rewoken) never
    returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually produced round-trips
    (roundtrips > 0), else the oracle would be vacuous.

FAIL ON: a captured response whose status line / X-Fiber-Token / Content-Length
does not match this fiber's closed-form value, a body that is not exactly this
fiber's expected bytes (lost/doubled chunk, torn BytesIO, sibling body leak),
non-parseable output, or handler-logged stderr.  Every such condition is a real
runtime corruption of single-owner handler/stream state, never documented
Python semantics (the handler and all three streams have exactly one owner).

Stresses: wsgiref.handlers.SimpleHandler.run/finish_response write loop, header
buffering + send_headers flush, per-chunk handler.write into a fiber-local
BytesIO across hub-migration + yield, generator-app suspension mid-response,
Content-Length/body framing conservation under M:N.

CPU/BytesIO only -- no sockets, no files, no subprocess -- so it runs at the CPU
default_funcs scale without an fd/resource cap.
"""
import io

from wsgiref.handlers import SimpleHandler

import harness
import runloom

# Number of body chunks the generator app hands back, with a runloom yield
# between each.  >1 so the handler's write loop is genuinely suspended
# mid-response (headers flushed, some chunks written, more pending) while
# siblings run their own handlers.
NCHUNKS = 5

# Status codes drawn per fiber so the status LINE is also fiber-specific (not a
# constant every fiber shares).  All 2xx so the response is a normal success.
STATUS_CODES = ("200 OK", "201 Created", "202 Accepted", "203 Non-Authoritative",
                "204 No Content", "206 Partial Content")

# Sustained round-trips per worker, bounded by H.running().  The mid-response
# suspension hazard only manifests under SUSTAINED churn: many fibers each with a
# handler suspended inside its generator app's yield while siblings write their
# own responses.  A single round-trip per fiber barely overlaps a sibling's.
INNER_CAP = 100000


def build_expected(wid, idx, token):
    """Build this fiber's closed-form (status_str, header_token, body_chunks,
    body).  Everything is a pure function of (wid, idx, token) so the captured
    response is fully predictable and any deviation is a corruption."""
    status_str = STATUS_CODES[(wid + idx) % len(STATUS_CODES)]
    header_token = "wid{0}-idx{1}-tok{2}".format(wid, idx, token)
    # Each chunk embeds wid/idx/token + its own index, so a lost, doubled, or
    # cross-fiber chunk is visible in the assembled body.
    chunks = []
    for c in range(NCHUNKS):
        chunks.append(
            "W{0}.I{1}.T{2}.C{3};".format(wid, idx, token, c).encode("ascii"))
    body = b"".join(chunks)
    return status_str, header_token, chunks, body


def make_app(status_str, header_token, chunks, body):
    """Build a fiber-local WSGI app.  It returns a GENERATOR that yields the body
    chunks one at a time, doing runloom.yield_now() between them so the handler's
    write loop is suspended mid-response and a sibling reliably interleaves."""
    def app(environ, start_response):
        start_response(status_str, [
            ("Content-Type", "text/plain; charset=ascii"),
            ("X-Fiber-Token", header_token),
            ("Content-Length", str(len(body))),
        ])

        def emit():
            for c, chunk in enumerate(chunks):
                yield chunk
                # Suspend the handler mid-response: headers are flushed and
                # chunk c is written; siblings run their own handlers now.
                if c + 1 < len(chunks):
                    runloom.yield_now()
        return emit()
    return app


def build_environ(wid, idx):
    """A minimal, valid, fiber-local WSGI environ.  SimpleHandler fills the
    wsgi.* keys itself; these are the CGI-style request vars it needs."""
    return {
        "REQUEST_METHOD": "GET",
        "SERVER_NAME": "127.0.0.1",
        "SERVER_PORT": "80",
        "SERVER_PROTOCOL": "HTTP/1.0",
        "SCRIPT_NAME": "",
        "PATH_INFO": "/w{0}/i{1}".format(wid, idx),
        "QUERY_STRING": "",
        "REMOTE_ADDR": "127.0.0.1",
    }


def parse_headers(header_blob):
    """Parse the CRLF-delimited header block (after the status line) into a
    lower-cased name -> value dict.  Returns (status_line_bytes, headers)."""
    lines = header_blob.split(b"\r\n")
    status_line = lines[0]
    headers = {}
    for ln in lines[1:]:
        if not ln:
            continue
        i = ln.find(b":")
        if i < 0:
            continue
        name = ln[:i].strip().lower()
        val = ln[i + 1:].strip()
        headers[name] = val
    return status_line, headers


def roundtrip_check(H, wid, idx, state):
    """Single-owner WSGI round-trip check.

    Build fiber-local expected bytes, run a SimpleHandler bound to fiber-local
    BytesIO streams across in-response yields, then assert the captured response
    matches the closed form exactly.  Any deviation is a cross-fiber leak or a
    torn single-owner stream/handler."""
    token = state["rng_nonce"][wid] + idx      # single-writer base, deterministic
    status_str, header_token, chunks, body = build_expected(wid, idx, token)
    app = make_app(status_str, header_token, chunks, body)

    stdin = io.BytesIO(b"")
    stdout = io.BytesIO()
    stderr = io.BytesIO()
    environ = build_environ(wid, idx)

    handler = SimpleHandler(stdin, stdout, stderr, environ,
                            multithread=False, multiprocess=True)
    # SimpleHandler advertises HTTP/1.0; keep it explicit so the status line is
    # a fixed closed form.
    handler.http_version = "1.0"

    # Yield BEFORE running so a sibling is mid-response while we start ours.
    runloom.yield_now()

    handler.run(app)                            # writes full response into stdout

    out = stdout.getvalue()
    err = stderr.getvalue()

    if err:
        H.fail("wsgiref SimpleHandler logged to stderr for wid {0} idx {1}: {2!r} "
               "-- the handler hit an error running a single-owner app (torn "
               "stream/handler state under M:N)".format(wid, idx, err[:200]))
        return

    sep = out.find(b"\r\n\r\n")
    if sep < 0:
        H.fail("wsgiref response has no header/body separator for wid {0} idx "
               "{1}: {2!r} -- the response was truncated or torn under M:N "
               "(single-owner BytesIO corrupted)".format(wid, idx, out[:200]))
        return

    header_blob = out[:sep]
    body_out = out[sep + 4:]
    status_line, headers = parse_headers(header_blob)

    expected_status = ("HTTP/1.0 " + status_str).encode("ascii")
    if status_line != expected_status:
        H.fail("wsgiref status line WRONG for wid {0} idx {1}: got {2!r} expected "
               "{3!r} -- a cross-fiber leak or torn status buffer (this fiber's "
               "single-owner handler emitted a sibling's / corrupted status "
               "line)".format(wid, idx, status_line, expected_status))
        return

    got_token = headers.get(b"x-fiber-token")
    expected_token = header_token.encode("ascii")
    if got_token != expected_token:
        H.fail("wsgiref X-Fiber-Token WRONG for wid {0} idx {1}: got {2!r} "
               "expected {3!r} -- a cross-fiber header leak: this fiber's "
               "response carries another fiber's token (single-owner header "
               "buffer polluted under M:N)".format(
                   wid, idx, got_token, expected_token))
        return

    got_clen = headers.get(b"content-length")
    expected_clen = str(len(body)).encode("ascii")
    if got_clen != expected_clen:
        H.fail("wsgiref Content-Length WRONG for wid {0} idx {1}: got {2!r} "
               "expected {3!r} -- the framing length disagrees with the body "
               "(lost/doubled chunk or torn header under M:N)".format(
                   wid, idx, got_clen, expected_clen))
        return

    if body_out != body:
        H.fail("wsgiref BODY WRONG for wid {0} idx {1}: got {2!r} expected {3!r} "
               "-- a lost/doubled body chunk, a torn BytesIO buffer, or a "
               "sibling's body leaked into this fiber's single-owner response "
               "across the mid-response yield".format(
                   wid, idx, body_out, body))
        return

    state["roundtrips"][wid] += 1               # single-writer-per-slot, race-free


def worker(H, wid, rng, state):
    """Run sustained single-owner WSGI round-trips, each suspending its handler
    mid-response (via the generator app's inter-chunk yields) so siblings on
    other hubs interleave their own handlers' writes."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            roundtrip_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Per-fiber deterministic nonce base (single-writer-per-slot when read; here
    # just a fixed derived seed per wid so the token stream is replayable).
    # roundtrips is the race-free per-wid conservation/non-vacuity tally
    # ([0]*H.funcs, one writer per slot -- see p405 HARD RULE 1).
    H.state = {
        "roundtrips": [0] * H.funcs,
        "rng_nonce": [((wid * 2654435761) & 0xFFFFFF) for wid in range(H.funcs)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rts = sum(H.state["roundtrips"])
    H.log("wsgiref[single-owner LOAD-BEARING]: {0} SimpleHandler WSGI round-trips "
          "(status line + X-Fiber-Token + Content-Length + body all bit-identical "
          "to closed form, fail-fast); ops={1}".format(rts, H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip hazard was actually exercised.
    H.check(rts > 0,
            "no wsgiref round-trips completed -- the SimpleHandler mid-response "
            "write-loop hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid finish_response.
    H.require_no_lost("wsgiref handler round-trip")


if __name__ == "__main__":
    harness.main(
        "p618_wsgiref_handler_roundtrip", body, setup=setup, post=post,
        default_funcs=4000,
        describe="wsgiref.handlers.SimpleHandler drives a fiber-local WSGI app to "
                 "a full HTTP response over fiber-local BytesIO streams -- a "
                 "self-contained single-owner transformer.  LOAD-BEARING: each "
                 "fiber runs a generator app that yields BETWEEN body chunks so "
                 "the handler's write loop is suspended mid-response while sibling "
                 "fibers write their own responses on other hubs; the captured "
                 "status line, X-Fiber-Token header, Content-Length, and body MUST "
                 "be bit-identical to this fiber's closed form.  A sibling's "
                 "status/header/body leaking in, a wrong Content-Length, or a "
                 "lost/doubled/torn body chunk is the runloom single-owner-"
                 "handler-state corruption bug")
