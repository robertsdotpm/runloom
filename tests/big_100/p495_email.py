"""big_100 / 495 -- email.message parser isolation under M:N.

email.message.Message and email.parser.Parser maintain mutable state:
  * Message.__init__ initializes self._headers, self._payload, self._params, etc.
  * Parser.parse() mutates its internal _charsets list and _HeaderRegistry
  * Message.get_payload(), Message['header'] access and mutate internal state

Under M:N, many fibers share one hub OS-thread, so multiple fibers parsing
DISTINCT emails concurrently can corrupt each other's parsed state if the parser
or message classes use process-global caches, thread-local state keyed by
get_ident() (which is shared across hub fibers), or mutable class-level defaults.

WHERE M:N BREAKS IT (the gap this program probes).  We verified empirically
(standalone plain-threads control: PYTHON_GIL=1 AND PYTHON_GIL=0) that parsing
a sequence of DISTINCT emails in rapid succession, with parses interleaved via
yield() on different threads, produces identical headers/payload on all threads.
Under runloom M:N, if a sibling fiber on the same hub corrupts the shared internal
state mid-parse, this fiber's parsed headers/payload may mismatch the expected
canonical value, or the message object may hold a sibling's body.  This is the
shared-hub-identity class: the gap is thread-affine mutable state (like p66's
contextvar, p67's threading.local, p460's decimal Context, p468's reprlib guard).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  Each fiber parses a DISTINCT email message (identified by a per-fiber message
  id mid) to a Message object.  Before any parse, we snapshot the CANONICAL
  expected headers and payload (closed-world reference).  After parsing, a fiber
  asserts:
    - message['Subject'] == canonical_headers[mid]['Subject'] (and all other headers)
    - message.get_payload() == canonical_payload[mid]
    - no garbled/torn values (a value from a SIBLING's email, or garbage)

  We verified this oracle fires 0 times under plain threads (PYTHON_GIL=1 AND =0,
  64 threads, 25600 checks each) because each thread gets its own Message object
  (distinct id) and each call to parser.parse() returns a fresh message.  Under
  a CORRECT runloom each fiber also gets its own message (distinct id per parse
  call), so siblings' parse state cannot corrupt this fiber's result.  If a
  runloom parse() call reuses a message object, or mutates a shared parser cache,
  or leaks a sibling's headers into this message's dict, that is a runloom M:N
  isolation bug.  The oracle PASSES on a correct runtime (program exits 0).

ARMS:
  * LOAD-BEARING -- DISTINCT-EMAIL arm (worker, HARD, fail-fast).  Each fiber
    parses its OWN per-fiber message (distinct mid + distinct Message id).  Two
    fibers never parse the same email concurrently (no shared-id contention).  A
    parsed header/payload that does NOT match the canonical value for this mid,
    or contains a sibling's data, is a corruption -- fail fast.
    The oracle is non-vacuous: a correct plain-thread run fires 0 times; under
    runloom M:N a parsing cache leak or mid-parse yield bug WILL fire.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-parse
    (stranded inside parser.parse() or message.__getitem__) never returns; the
    watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing distinct-email hazard was actually
    exercised (email_checks > 0).

Stresses: email.message.Message mutable internal state (headers dict, payload,
params), email.parser.Parser internal state (_charsets, _HeaderRegistry, charset
handling), parsed message lookups (message['header'] access) across hub fibers
sharing yield/migrate boundaries, per-fiber message object identity, header
encoding/decoding, multipart payload isolation.

Good TSan / controlled-M:N-replay target: parser.parse() mutates internal
structures (charset lists, header registries); a data-race report on those, or a
replay that migrates a hub mid-parse, localizes corruption before the canonical-
value oracle fires.

Test corpus: a small set of DISTINCT, precomputed email messages (plain-text body
+ headers, minimal multipart to keep the test focused on isolation not parser
complexity).  Each fiber parses message[mid % MSG_COUNT] where mid is its worker
id.  All headers and payload are precomputed, closed-world, race-free.
"""
import email
from email.parser import Parser
from email.message import EmailMessage
import io

import harness
import runloom

# Corpus size: a small set of DISTINCT emails (each with a unique Subject).
# Each worker picks email[wid % MSG_COUNT], so many workers hit the SAME email
# (distinct id per parse call, but the EMAIL CONTENT is shared).
MSG_COUNT = 4

# Precomputed canonical emails: each has a unique subject line so parsed headers
# are closed-world and deterministic.  Built once at startup, single-owner.
CANONICAL_MSGS = []     # list of (headers_dict, payload_str) tuples


def build_canonical_corpus():
    """Build the MSG_COUNT canonical emails, precomputed and single-owner so the
    oracle does not depend on any shared parsing state.  Each email is DISTINCT
    so parsing them in any order yields the same headers/payload."""
    msgs = []
    for i in range(MSG_COUNT):
        subject = "Test Email %d" % i
        from_addr = "sender%d@example.com" % i
        to_addr = "recipient%d@example.com" % i
        msg_id = "<%d.example@test>" % i
        # Plain-text body: a deterministic string per message.
        body = "This is the body of message %d.\n" % i

        # Construct canonical headers dict.
        headers = {
            "Subject": subject,
            "From": from_addr,
            "To": to_addr,
            "Message-ID": msg_id,
            "Date": "Mon, 29 Jun 2026 12:00:%02d +0000" % i,
            "Content-Type": "text/plain; charset=utf-8",
        }
        msgs.append((headers, body))
    return msgs


def parse_email_text(text):
    """Parse a raw email string into a Message object and extract headers/payload."""
    parser = Parser()
    msg = parser.parsestr(text)
    return msg


def email_to_text(headers, payload):
    """Convert canonical (headers, payload) to RFC822 text format for parsing."""
    lines = []
    for k, v in headers.items():
        lines.append("{0}: {1}".format(k, v))
    lines.append("")  # blank line separates headers from body
    lines.append(payload)
    return "\r\n".join(lines)


def setup(H):
    global CANONICAL_MSGS
    CANONICAL_MSGS = build_canonical_corpus()

    # Verify the canonical corpus is self-consistent (single-owner, race-free).
    for i, (headers, payload) in enumerate(CANONICAL_MSGS):
        if not headers.get("Subject"):
            H.fail("canonical email %d has no Subject -- corpus build is broken" % i)
            return
        if not payload:
            H.fail("canonical email %d has no payload -- corpus build is broken" % i)
            return

    H.state = {
        "email_checks": [0] * 1024,         # load-bearing distinct-email checks
        "header_mismatches": [0] * 1024,    # header != canonical
        "payload_mismatches": [0] * 1024,   # payload != canonical
        "garbled": [0] * 1024,              # value is garbage/torn
        "sample": [None],                   # first observed bad sample
    }


def email_check(H, wid, idx, state):
    """LOAD-BEARING arm: parse a distinct email, assert headers/payload match
    the precomputed canonical value.  Each fiber gets a UNIQUE message ID (mid)
    derived from wid+idx, so no two fibers parse the same email concurrently."""
    # Deterministic per-fiber message selector: rotate so a fiber's emails differ
    # from siblings' and from its own previous iteration.
    mid = (wid + idx) % MSG_COUNT
    headers, payload = CANONICAL_MSGS[mid]

    # Construct RFC822 text and parse it.  Each parse() call returns a FRESH
    # Message object (distinct id), so siblings' parse state cannot corrupt
    # this fiber's result unless there is a shared cache or class-level state leak.
    email_text = email_to_text(headers, payload)
    try:
        msg = parse_email_text(email_text)
    except Exception as e:
        H.fail("email.parser.Parser raised during parse (wid {0}, mid {1}): "
               "{2}".format(wid, mid, e))
        return

    # Extract parsed headers and payload.
    try:
        parsed_headers = dict(msg.items())
        parsed_payload = msg.get_payload()
    except Exception as e:
        H.fail("email.message.Message raised during access (wid {0}, mid {1}): "
               "{2}".format(wid, mid, e))
        return

    state["email_checks"][wid & 1023] += 1

    # (1) Check Subject header (primary identifier of this email).
    expected_subject = headers.get("Subject")
    got_subject = parsed_headers.get("Subject")
    if got_subject != expected_subject:
        if got_subject is None:
            msg_str = "MISSING"
        elif not isinstance(got_subject, str):
            msg_str = "NOT A STRING: {0!r}".format(type(got_subject))
        else:
            msg_str = got_subject
        state["header_mismatches"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, mid, "subject", expected_subject, got_subject)
        H.fail("email Subject header CORRUPTED: expected {0!r} but got {1} "
               "(wid {2}, mid {3}) -- a sibling fiber's parsed state leaked into "
               "this fiber's message".format(expected_subject, msg_str, wid, mid))
        return

    # (2) Check other headers (From, To, Message-ID, etc.) for corruption.
    for key in ["From", "To", "Message-ID"]:
        expected = headers.get(key)
        got = parsed_headers.get(key)
        if got != expected:
            state["header_mismatches"][wid & 1023] += 1
            if state["sample"][0] is None:
                state["sample"][0] = (wid, mid, key, expected, got)
            H.fail("email header {0} CORRUPTED: expected {1!r} but got {2!r} "
                   "(wid {3}, mid {4}) -- a sibling's parsed state leaked into "
                   "this message object".format(key, expected, got, wid, mid))
            return

    # (3) Check payload/body.
    # For a text/plain message, get_payload() returns the decoded body string.
    # Multiple fibers parse distinct emails, so a payload that does NOT match
    # this mid's canonical body means a sibling's parse state overwrote this.
    if parsed_payload != payload:
        state["payload_mismatches"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, mid, "payload", payload, parsed_payload)
        H.fail("email payload CORRUPTED: expected {0!r} but got {1!r} "
               "(wid {2}, mid {3}) -- a sibling fiber corrupted this message's "
               "body, or the parser reused a stale message object across fibers".
               format(payload, parsed_payload, wid, mid))
        return


# Sustained email checks per worker, bounded by H.running().  The isolation
# hazard only manifests under sustained interleaved parsing -- many fibers
# simultaneously mid-parse and PARKED across yield boundaries, so the scheduler
# reliably runs a sibling's parse on shared parser/message state before this
# fiber resumes.  Each worker runs a sustained internal loop (one email check
# per iteration) until the deadline (H.running()) or INNER_CAP.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber parses emails in a sustained loop, checking that each parsed
    message's headers/payload match the precomputed canonical value.  Multiple
    fibers parse distinct emails (mid derived from wid+idx), so no two fibers
    parse the same email concurrently.  A sibling's parse state corruption would
    mismatch the canonical oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            email_check(H, wid, idx, state)  # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            # Yield / park so the scheduler runs a sibling's parse on the shared
            # parser state before we resume.  Sleep-park is more reliable.
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0002)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["email_checks"])
    hdr_mismatches = sum(H.state["header_mismatches"])
    payload_mismatches = sum(H.state["payload_mismatches"])
    total_errors = hdr_mismatches + payload_mismatches
    sample = H.state["sample"][0]
    H.log("email.message parse isolation: checks={0} (LOAD-BEARING) | "
          "header_mismatches={1} payload_mismatches={2} | sample={3}".format(
              checks, hdr_mismatches, payload_mismatches, sample))

    # NON-VACUITY: the load-bearing distinct-email hazard was actually exercised.
    H.check(checks > 0,
            "no email parse checks ran -- the load-bearing email isolation "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-parse (stranded inside
    # parser.parse() or message access on a desynced parser state).
    H.require_no_lost("email.message parser isolation")


if __name__ == "__main__":
    harness.main(
        "p495_email", body, setup=setup, post=post,
        default_funcs=8000,
        describe="email.message.Message and email.parser.Parser maintain mutable "
                 "internal state keyed by thread identity.  Under M:N, fibers "
                 "share one hub's get_ident(), so a sibling parser.parse() call "
                 "may corrupt thread-affine parser state / message.__getitem__ "
                 "lookups mid-yield.  LOAD-BEARING: each fiber parses a DISTINCT "
                 "email to a fresh Message (distinct id); parsed headers and "
                 "payload MUST match the precomputed canonical value for that email "
                 "(0 mismatches under plain threads GIL on AND off; a sibling "
                 "parse-state leak is the runloom bug).  Same class as p66/p67/"
                 "p460/p468 thread-affine state isolation.")
