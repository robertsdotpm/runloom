"""big_100 / 585 -- poplib.POP3 multiline-response framing conservation under M:N.

poplib.POP3 is a line-based client: every response is parsed by _getline()
(file.readline over the socket, CRLF stripping) and, for multiline replies,
_getlongresp() which loops _getline() until a bare b'.' terminator, UN-STUFFS a
leading b'..' -> b'.' on each body line, and accumulates an octet count.  RETR /
LIST / UIDL all go through this dot-stuffed, byte-exact framing.  A POP3 client
instance owns ONE socket + ONE buffered file object; it is a strictly
SINGLE-OWNER object -- created, driven, and closed by exactly one fiber.

WHERE M:N COULD BREAK IT (the gap this program probes).  Under free-threaded
CPython with hubs>1, tens of thousands of fibers each drive their own POP3
client against a shared loopback POP3 server.  Each fiber's RETR/LIST reply is a
KNOWN, closed-form byte stream (the server and the client compute the same body
from the same deterministic make_body(n)).  The socket recv path parks the fiber
on every readline and resumes it -- possibly on a different hub -- so the hazard
is: does this fiber's buffered readline stream stay byte-exact and private, or
could a torn recv, a mis-resumed park, or a cross-fiber leak of another socket's
bytes into THIS file object corrupt the parsed multiline body?  A correct
runtime delivers each single-owner socket's bytes intact; the parsed body then
matches the closed-form expected exactly.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  The server is a plain, correct minimal
POP3 responder: for message n it emits exactly encode_retr(n) -- the dot-stuffed
wire form of make_body(n).  A single fiber owning one POP3 client that RETRs
message n MUST get back (resp, lines, octets) with:
    * resp.startswith(b'+OK')                       (a valid status line)
    * lines == make_body(n)                         (un-stuffed body, byte-exact)
    * octets == sum(len(L)+2 for L in make_body(n)) (RFC framing octet law:
      each body line contributes its length + CRLF, dot-stuffing is octet-
      neutral because _getlongresp subtracts the one stuffing dot back out)
STAT and LIST add two more closed-form checks (count/size, per-message listing).
This is a CLOSED-WORLD conservation: the client knows precisely what the server
will send, so ANY deviation -- a dropped/duplicated line, a torn octet count, a
byte from a sibling's socket, or a poplib.error_proto (which on a correct server
+ single-owner socket can only mean a mis-framed / corrupted read) while the run
is live -- is a real runtime fault (torn socket read, lost/mis-resumed park,
cross-fiber fd/data leak), NOT documented Python behavior.  The POP3 instance is
never shared, so this is not a shared-mutable-object race.

ORACLES:
  * LOAD-BEARING -- RETR/LIST/STAT FRAMING CONSERVATION (worker, HARD, fail-fast).
    Single-owner POP3 client, closed-form expected body; a mismatch or an
    error_proto while H.running() is a hard fault.  A yield (yield_now + the
    natural recv parks inside every readline) sits at each command boundary so a
    sibling reliably interleaves on the hub before this fiber resumes its stream.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-readline
    (parked on a recv wake that never comes -- a lost wakeup) never returns; the
    watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (retrs > 0).

FAIL ON: a parsed RETR/LIST body that differs from the closed-form expected, a
torn octet count, a wrong STAT tuple, or a poplib.error_proto raised against the
correct server while the run is live.  Teardown EOFs (H.running() false, server
loop already exited) are benign and never fail.

Resource-bounded: one socket + buffered file per fiber, so max_funcs caps the
forever loop's --funcs 1000000 well under the fd/socket ceiling.

Stresses: poplib _getline/_getlongresp framing, dot-stuffing un-escape, buffered
socket.makefile readline under cooperative recv parks + hub migration, per-fiber
socket/fd isolation, multiline RETR/LIST/STAT round-trip conservation.
"""
import socket

import poplib

import harness
import netutil
import runloom

# Fixed, deterministic mailbox.  Both the server and every client compute the
# SAME body for message n from make_body(n), so the client knows exactly what
# bytes the server will send -- the closed world that makes the oracle exact.
NUM_MESSAGES = 12


def make_body(n):
    """Deterministic body (list of CRLF-free byte lines) for message n.

    Purely arithmetic -> server and client agree with zero ambiguity.  The mix
    guarantees (across the mailbox) lines that START WITH DOTS (exercise the
    dot-stuffing un-escape path in _getlongresp), EMPTY lines (octet edge case),
    and plain lines.  No line is ever a bare b'.' (which would collide with the
    multiline terminator): dotted lines always carry a non-empty tail."""
    nlines = 2 + (n * 7 + 3) % 9          # 2..10 lines
    lines = []
    for i in range(nlines):
        k = (n * 31 + i * 17) & 0x3F
        base = bytes([65 + (k % 26)]) * (1 + (k % 24))   # 'A'..'Z' repeated
        sel = (n + i) % 3
        if sel == 0:
            lines.append(b"." * (1 + (i % 2)) + base)     # 1 or 2 leading dots
        elif sel == 1:
            lines.append(b"")                             # empty line
        else:
            lines.append(base)
    return lines


def msg_octets(n):
    """RFC-framing octet count poplib._getlongresp reports for message n: each
    body line contributes len(line)+2 (the stripped CRLF), dot-stuffing being
    octet-neutral (the one stuffing dot is subtracted back out)."""
    return sum(len(line) + 2 for line in make_body(n))


TOTAL_SIZE = sum(msg_octets(n) for n in range(1, NUM_MESSAGES + 1))


def encode_retr(n):
    """Server-side wire bytes for RETR n: a +OK header, the dot-stuffed body,
    then the bare-dot terminator.  Only lines that START WITH '.' get one extra
    leading '.' (RFC 1939 byte-stuffing); the client un-stuffs it back."""
    out = [b"+OK %d octets\r\n" % msg_octets(n)]
    for line in make_body(n):
        stuffed = (b"." + line) if line.startswith(b".") else line
        out.append(stuffed + b"\r\n")
    out.append(b".\r\n")
    return b"".join(out)


# Pre-encoded LIST reply (identical for every connection) -- a scan listing of
# every message's octet size, then the terminator.
def encode_list():
    out = [b"+OK %d messages\r\n" % NUM_MESSAGES]
    for n in range(1, NUM_MESSAGES + 1):
        out.append(b"%d %d\r\n" % (n, msg_octets(n)))
    out.append(b".\r\n")
    return b"".join(out)


LIST_WIRE = encode_list()
STAT_WIRE = b"+OK %d %d\r\n" % (NUM_MESSAGES, TOTAL_SIZE)

# The client-side closed-form expectations (computed once).
EXPECTED_LIST_LINES = [b"%d %d" % (n, msg_octets(n))
                       for n in range(1, NUM_MESSAGES + 1)]
EXPECTED_LIST_OCTETS = sum(len(line) + 2 for line in EXPECTED_LIST_LINES)


# ---- shared server (correct minimal POP3 responder) ----------------------
def pop3_server_handler(H, conn):
    """Serve one client connection with a correct, deterministic POP3 dialogue.

    The connection socket is single-owner to this handler fiber.  We speak the
    minimal command set the client drives (STAT/LIST/RETR/NOOP/QUIT) plus a
    permissive default, all byte-exact.  The loop exits on QUIT, EOF, or when the
    run winds down (H.running() false), then closes -- a client mid-read at that
    point sees a benign EOF (never a fail, guarded by H.running() on the client).
    """
    rf = wf = None
    try:
        conn.setblocking(True)            # cooperative-blocking under monkey.patch
        rf = conn.makefile("rb")
        wf = conn.makefile("wb")
        wf.write(b"+OK big100 POP3 ready\r\n")
        wf.flush()
        while H.running():
            raw = rf.readline(1024)
            if not raw:                   # client closed / EOF
                break
            parts = raw.split()
            name = parts[0].upper() if parts else b""
            if name == b"STAT":
                wf.write(STAT_WIRE)
            elif name == b"LIST" and len(parts) == 1:
                wf.write(LIST_WIRE)
            elif name == b"RETR" and len(parts) == 2:
                try:
                    n = int(parts[1].decode("ascii"))
                except (ValueError, UnicodeDecodeError):
                    n = -1
                if 1 <= n <= NUM_MESSAGES:
                    wf.write(encode_retr(n))
                else:
                    wf.write(b"-ERR no such message\r\n")
            elif name == b"QUIT":
                wf.write(b"+OK bye\r\n")
                wf.flush()
                break
            elif name in (b"USER", b"PASS", b"NOOP", b"RSET", b"DELE"):
                wf.write(b"+OK\r\n")
            else:
                wf.write(b"+OK\r\n")
            wf.flush()
    except OSError:
        pass
    finally:
        for f in (rf, wf):
            if f is not None:
                try:
                    f.close()
                except OSError:
                    pass
        netutil.close_quiet(conn)


# ---- single-owner client oracle ------------------------------------------
def check_retr(H, wid, pop, n):
    """RETR message n on this fiber's single-owner POP3 client and assert the
    parsed multiline reply matches the closed-form expected body byte-exactly.
    Returns True on success, False (after H.fail) on a framing violation."""
    expected = make_body(n)
    resp, lines, octets = pop.retr(n)
    if not resp.startswith(b"+OK"):
        H.fail("RETR {0} status not +OK: {1!r} (wid {2}) -- a torn/misframed "
               "status line on a single-owner socket".format(n, resp, wid))
        return False
    if lines != expected:
        H.fail("RETR {0} body MISMATCH (wid {1}): got {2} lines, expected {3}; "
               "first diff around {4!r} vs {5!r} -- a dropped/duplicated line or "
               "a byte from another fiber's socket leaked into this buffered "
               "readline stream".format(
                   n, wid, len(lines), len(expected),
                   lines[:2], expected[:2]))
        return False
    exp_octets = msg_octets(n)
    if octets != exp_octets:
        H.fail("RETR {0} octet count TORN (wid {1}): got {2}, expected {3} -- "
               "the framing octet accumulator disagrees with the byte-exact body "
               "(a torn read across a cooperative park)".format(
                   n, wid, octets, exp_octets))
        return False
    return True


def client(H, wid, rng, state):
    """One fiber: own a POP3 client, run a closed-form dialogue, assert every
    reply conserves the known bytes.  Repeats per round for the whole window."""
    servers = state["servers"]
    retrs = state["retrs"]
    # Spread the initial connect storm so the shared accept loop isn't thundered.
    H.sleep(rng.random() * 0.3)
    for _ in H.round_range():
        if not H.running():
            break
        host, port = servers[rng.randrange(len(servers))]
        pop = None
        try:
            pop = poplib.POP3(host, port)     # connects + reads welcome (blocking)
            if not pop.getwelcome().startswith(b"+OK"):
                H.fail("welcome not +OK (wid {0}): {1!r}".format(
                    wid, pop.getwelcome()))
                return

            # STAT: closed-form (count, size).
            count, size = pop.stat()
            if count != NUM_MESSAGES or size != TOTAL_SIZE:
                H.fail("STAT wrong (wid {0}): got ({1}, {2}) expected ({3}, {4}) "
                       "-- a torn status line".format(
                           wid, count, size, NUM_MESSAGES, TOTAL_SIZE))
                return

            runloom.yield_now()               # let a sibling interleave the hub

            # LIST: closed-form multiline scan listing.
            resp, lines, octets = pop.list()
            if lines != EXPECTED_LIST_LINES:
                H.fail("LIST body MISMATCH (wid {0}): got {1} lines expected {2} "
                       "-- a dropped/duplicated listing line or cross-fiber byte "
                       "leak".format(wid, len(lines), len(EXPECTED_LIST_LINES)))
                return
            if octets != EXPECTED_LIST_OCTETS:
                H.fail("LIST octets TORN (wid {0}): got {1} expected {2}".format(
                    wid, octets, EXPECTED_LIST_OCTETS))
                return

            # RETR several messages, yielding between each so the recv-park /
            # hub-migration boundary is crossed mid-dialogue every time.
            k = rng.randint(2, 5)
            for _ in range(k):
                if not H.running():
                    break
                n = rng.randint(1, NUM_MESSAGES)
                if not check_retr(H, wid, pop, n):
                    return
                retrs[wid] += 1               # single-writer-per-slot, race-free
                H.op(wid)
                runloom.yield_now()

            pop.quit()
            H.task_done(wid)
        except poplib.error_proto as exc:
            if H.running():
                # A correct server + single-owner socket cannot legitimately
                # desync: an error_proto here means a mis-framed / corrupted read
                # (torn recv, mis-resumed park, cross-fiber byte leak).
                H.fail("poplib.error_proto on single-owner socket while live "
                       "(wid {0}): {1!r} -- a mis-framed/corrupted read on an "
                       "otherwise-correct POP3 dialogue".format(wid, exc))
                return
            # Teardown: server loop exited, in-flight read saw EOF -- benign.
            break
        except OSError:
            # Connection churn / teardown races are benign; a genuine lost wakeup
            # manifests as no-progress and is caught by the watchdog, not masked
            # here.
            if not H.running():
                break
        finally:
            if pop is not None:
                try:
                    pop.close()
                except OSError:
                    pass


def setup(H):
    servers = netutil.listen_all(
        H, lambda conn, addr: H.fiber(pop3_server_handler, H, conn))
    H.state = {
        "servers": servers,
        "retrs": [0] * H.funcs,            # ONE slot per worker (race-free)
    }


def body(H):
    H.run_pool(H.funcs, client, H.state)


def post(H):
    retrs = sum(H.state["retrs"])
    H.log("poplib single-owner framing: {0} RETR round-trips conserved "
          "byte-exact (STAT/LIST/RETR closed-form checks all passed fail-fast); "
          "ops={1}".format(retrs, H.total_ops()))
    # NON-VACUITY: the load-bearing framing oracle actually ran.
    H.check(retrs > 0,
            "no RETR round-trips completed -- the poplib single-owner framing "
            "oracle was never exercised (vacuous run)")
    # COMPLETENESS: no fiber stranded mid-readline (lost recv wakeup).
    H.require_no_lost("poplib framing conservation")


if __name__ == "__main__":
    harness.main(
        "p585_poplib_retr_framing", body, setup=setup, post=post,
        default_funcs=4000, max_funcs=2000,
        describe="tens of thousands of fibers each drive their OWN single-owner "
                 "poplib.POP3 client against a shared correct loopback POP3 "
                 "server.  Each RETR/LIST/STAT reply is a closed-form byte "
                 "stream (server and client compute the same make_body(n)); the "
                 "parsed multiline body, un-stuffed line list, and framing octet "
                 "count MUST match exactly across the recv-park / hub-migration "
                 "boundary.  A mismatch, torn octet count, or error_proto on the "
                 "correct server while live is a torn/mis-resumed socket read or "
                 "cross-fiber byte leak -- a real runtime fault")
