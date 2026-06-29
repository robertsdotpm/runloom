"""big_100 / 446 -- multiprocessing.connection.Connection recv_bytes framing
under a concurrent second reader on ONE pipe fd.

The subject is multiprocessing.connection.Connection (the duplex socketpair end
returned by multiprocessing.Pipe(duplex=True); on Linux its _handle is a pollable
socket fd, so under monkey.patch() every os.read/os.write on it is COOPERATIVE --
osio.py's _patched_os_read parks the fiber on runloom_c.wait_fd(fd, READ) when the
pipe would block).  Connection is documented as NOT safe for concurrent readers,
and the exact non-atomic state we attack is the read-side reassembly carried
ACROSS that park inside _ConnectionBase._recv / _recv_bytes (Lib/multiprocessing/
connection.py):

    def _recv(self, size, read=_read):
        buf = io.BytesIO()
        handle = self._handle
        remaining = size                 # <-- per-CALL Python counter, lives on
        while remaining > 0:             #     the grown-down C stack across parks
            chunk = read(handle, remaining)   # <-- os.read -> wait_fd PARK here
            ...
            buf.write(chunk)             # <-- the accumulation buffer, ditto
            remaining -= n
        return buf

    def _recv_bytes(self):
        buf = self._recv(4)              # read the 4-byte length PREFIX ...
        size, = struct.unpack("!i", buf.getvalue())
        if size == -1:
            buf = self._recv(8); size, = struct.unpack("!Q", buf.getvalue())
        return self._recv(size)          # ... then read the BODY of `size` bytes

_send_bytes frames every message as struct.pack("!i", len) (a 4-byte PREFIX) then
the payload, all via os.write on the same fd.  The corruptible state is therefore
TWO-LAYER:

  (1) the SHARED pipe read-POSITION -- a single kernel byte stream.  There is ONE
      read cursor; two readers pulling from it interleave their os.read syscalls.
  (2) the PER-CALL Python reassembly (`remaining`, the BytesIO `buf`) held across
      the wait_fd park between the prefix read and the body reads.

THE RACING OP PAIR.  Two receiver fibers each do `read 4-byte prefix -> read body`
on the SAME Connection.  Fiber A reads a prefix announcing N bytes, then parks in
_recv(N) waiting for the body; fiber B, on another hub, runs and consumes bytes
from the stream that BELONG to A's body -- or A wakes and reads B's prefix as its
body.  The result is a TORN FRAME: A's reassembled message is one frame's prefix
spliced onto another frame's body bytes, a length/payload mismatch, or a stream
that has silently desynchronized so every subsequent frame is shifted.

TARGET INVARIANT -- FRAME IDENTITY conservation over a finite UNIVERSE.  Every
frame sent is a UNIQUE, self-describing tag: payload = a recognizable sentinel key
drawn from a closed UNIVERSE, repeated to a per-tag length, prefixed (INSIDE the
payload, so it survives as plain bytes) with a header struct (seq, declared-length,
crc32-of-body).  A receiver asserts each frame it recv()s is INTERNALLY consistent:

    declared-length == actual body length  AND  crc32(body) == declared crc
    AND every body byte block decodes to a key that is IN the UNIVERSE.

and that, after all senders/receivers join, the MULTISET of received tags ==
the multiset SENT -- no frame torn, dropped, or duplicated.

To make this a clean CONSERVATION test of CPython's framing (not a re-statement of
the documented no-concurrent-reader contract), the SHARED Connection's recv_bytes
calls are SERIALIZED under ONE cooperative Lock and its send_bytes under another
(send_bytes also issues several os.write calls per frame, which interleave just as
badly); the readers/writers still PARK inside the held framing call, so the prefix
->park->body sequence is fully exercised, but only one reassembly is ever in flight
on the shared fd at a time.  In parallel a SINGLE-OWNER PRIVATE Pipe pair runs
UNLOCKED as the CONTROL arm -- one sender fiber, one receiver fiber, no second
reader -- which must conserve EVERY frame by construction.  If the LOCKED shared
arm loses/torments a frame the fault is in CPython's framing carried across the
M:N park (or in the cooperative os.read/os.write splitting a frame); if the
UNLOCKED single-owner control arm loses a frame, the fault is in the cooperative
pipe transport itself (a wait_fd park dropping bytes) -- either way a real bug.

Invariant (hot, fail-fast): every received frame's declared length == its actual
body length; crc32(body) matches the declared crc; every decoded key is in the
finite UNIVERSE.  A mismatch is a TORN frame and fails immediately.
Invariant (post): per round, multiset(received tags) == multiset(sent tags) on
BOTH arms (no frame torn / dropped / duplicated); the single-owner control arm
conserved every frame; both framing CASES were exercised; total ops > 0; no
worker lost (require_no_lost) -- a stranded receiver parked forever on a desynced
stream is a lost-wakeup, caught as LOST.

Stresses: multiprocessing.connection.Connection _recv_bytes prefix-then-body
reassembly across a cooperative wait_fd park, the per-call `remaining`/BytesIO
carried on a grown-down C stack, concurrent-reader stream desync on one pipe fd,
torn-frame (prefix-of-B + body-of-A) detection, cooperative os.read/os.write
frame splitting, frame-identity conservation shared-vs-private.

Good TSan / controlled-replay target: two os.read syscalls on the same fd from
fibers on different hubs is a textbook racing read on shared kernel state; a torn
frame (crc mismatch / out-of-universe key) or a stranded receiver localizes the
desync before the conservation multiset even closes.
"""
import struct
import zlib

import harness
import runloom

# Finite sentinel UNIVERSE of body keys.  Every frame's payload is built from ONE
# of these 4-byte keys; a decoded key NOT in this set is a torn/spliced body
# (bytes from a different frame, or a desynced stream).  Sized so each round's
# sent multiset is a real, recognizable subset.
UNIVERSE_SIZE = 256
UNIVERSE = tuple(0x44600000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# Header packed at the front of every payload (INSIDE the framed bytes, so it is
# carried as opaque body and survives the length-prefix framing): the global
# sequence number (64-bit -- seq = wid*STRIDE + counter can exceed 2**32 with
# thousands of funcs), the declared body length, and the crc32 of the body that
# FOLLOWS the header.  A receiver recomputes the crc and checks the length; a torn
# frame (prefix of one message spliced onto the body of another) fails one of them.
HEADER = struct.Struct("!QII")          # seq (u64), body_len (u32), crc32 (u32)
HEADER_SIZE = HEADER.size               # 16

# Senders/receivers per shared Connection per round.  Each sender sends EXACTLY one
# frame and each receiver recv()s EXACTLY one frame, so the counts match and no
# receiver parks forever waiting for an (N+1)th frame.  Several distinct hubs
# pushing/pulling the SAME pipe fd is the cross-hub stream interleave.  Kept small
# (2) because the shared Connection's recv_bytes is GLOBALLY serialized under one
# cooperative lock, so total frames = funcs*PAIRS all funnel through one fd; 2
# keeps thousands of funcs draining well inside the run window while still pairing
# two concurrent senders/receivers per round per arm.
PAIRS = 2

# Body length cases.  Small frames fit in one os.read; large frames force _recv()
# to LOOP (remaining > 0 after the first chunk) and PARK mid-body on wait_fd
# between the prefix read and the last body byte -- the exact reassembly window the
# torn-frame hazard lives in (and, with recv_bytes serialized, the window in which
# the per-call `remaining`/BytesIO ride a grown-down C stack across the park).
# post() asserts both cases were exercised, so the worker round-robins them by id
# (never flaky random -- the p125/p126 low-op-count coverage bug).
CASE_SHORT = 0          # body fits in a single read (prefix+body in one go)
CASE_LONG = 1           # body spans many reads -> _recv loops and parks mid-body
NCASES = 2

# A LONG body is comfortably past a typical socket recv chunk so _recv must loop
# and park between the prefix and the last body byte; a SHORT body is a handful of
# key-repeats.  Both are exact multiples of the 4-byte key width so the decode is
# unambiguous.  LONG is sized big enough to force _recv()'s `while remaining > 0`
# loop to issue several os.read calls (each a potential wait_fd park mid-body) yet
# small enough that funcs*PAIRS frames drain through the one serialized fd inside
# the window.
SHORT_KEYS = 3                          # 3 keys -> 12 body bytes (single read)
LONG_KEYS = 4096                        # 4096 keys -> 16 KiB body, multi-read park

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024


def make_payload(seq, key, nkeys):
    """Build ONE self-describing framed payload: HEADER(seq, body_len, crc) +
    body, where body is `key` (a UNIVERSE member) packed `nkeys` times.  The
    header lives INSIDE the framed bytes so it rides through Connection's length
    prefix as opaque body and is recovered verbatim by the receiver."""
    body = struct.pack("!{0}I".format(nkeys), *([key] * nkeys))
    crc = zlib.crc32(body) & 0xffffffff
    return HEADER.pack(seq, len(body), crc) + body


def decode_and_check(H, raw, label):
    """Validate one frame's bytes as received from recv_bytes().  Returns the
    decoded (seq, key) tag on success, or None after recording a FAIL.

    The checks make a TORN frame falsifiable three independent ways: the declared
    body length must equal the actual trailing-byte count (a spliced prefix/body
    has the wrong length), the crc32 must match (different body bytes -> different
    crc), and every decoded 4-byte key must be the SAME UNIVERSE member (a frame
    built from one key cannot legally contain bytes from another frame's key)."""
    if len(raw) < HEADER_SIZE:
        H.fail("{0}: frame shorter than a header ({1} bytes < {2}) -- recv_bytes "
               "returned a truncated/torn frame (prefix consumed body bytes from a "
               "concurrent reader)".format(label, len(raw), HEADER_SIZE))
        return None
    seq, declared_len, declared_crc = HEADER.unpack(raw[:HEADER_SIZE])
    body = raw[HEADER_SIZE:]
    if len(body) != declared_len:
        H.fail("{0}: TORN frame seq={1}: declared body length {2} != actual {3} "
               "-- a length prefix from one message was spliced onto another "
               "message's body (concurrent-reader stream desync)".format(
                   label, seq, declared_len, len(body)))
        return None
    if (zlib.crc32(body) & 0xffffffff) != declared_crc:
        H.fail("{0}: TORN frame seq={1}: crc32 mismatch (declared {2:#x}) -- the "
               "reassembled body bytes are not the bytes that were sent (torn/"
               "spliced frame across the recv park)".format(
                   label, seq, declared_crc))
        return None
    if len(body) % 4 != 0:
        H.fail("{0}: frame seq={1} body length {2} not a multiple of the 4-byte "
               "key width -- the stream is byte-misaligned (desynced reader)"
               .format(label, seq, len(body)))
        return None
    keys = struct.unpack("!{0}I".format(len(body) // 4), body)
    first = keys[0]
    if first not in UNIVERSE_SET:
        H.fail("{0}: frame seq={1} decodes to OUT-OF-UNIVERSE key {2:#x} -- the "
               "body holds bytes from outside the sentinel universe (a torn/"
               "spliced body from a concurrent reader)".format(label, seq, first))
        return None
    for k in keys:
        if k != first:
            H.fail("{0}: frame seq={1} body is NOT a single repeated key (saw "
                   "{2:#x} after {3:#x}) -- two frames' bodies were spliced "
                   "together on the shared pipe".format(label, seq, k, first))
            return None
    return (seq, first)


# Per-worker seq stride: each worker's frames get seqs in [wid*STRIDE, ...), so
# every frame ever sent has a GLOBALLY UNIQUE seq.  A received tag whose seq was
# never sent (out of any worker's band) is an invented/torn frame; two received
# tags with the same seq is a DUPLICATED frame.  STRIDE is large enough that a
# worker doing thousands of rounds never wraps into the next worker's band.
SEQ_STRIDE = 1 << 24


# Exception classes that a TEARDOWN-WINDOW fd-close raises on an in-flight
# send_bytes/recv_bytes.  At funcs>=~6000 the funcs*PAIRS frames serialized through
# the one shared pipe fd cannot all drain within --duration; at the deadline the
# harness closes the registered Connection fds and runloom_c.cancel_all_parked()
# wakes every parked send/recv.  A recv/send caught in that window surfaces several
# ways, all benign:
#   * OSError "handle is closed" / EBADF / BrokenPipe -- the fd was closed.
#   * EOFError -- recv_bytes hit a clean EOF on the half-closed pipe.
#   * struct.error -- struct.unpack on a torn/short post-deadline prefix.
#   * TypeError/ValueError -- Connection.close() NULLed self._handle concurrently,
#     so _recv()'s `read(handle, ...)` reached os.fstat(None) (osio _fd_pollable)
#     and the offload backend re-raised "'NoneType' object cannot be interpreted as
#     an integer" / "negative file descriptor".  This is the same teardown fd-close
#     race, just surfaced from the os-dispatch path on a handle that was nulled
#     mid-read rather than from the socket layer.
# These are benign slow-finishers (the harness's own _worker_wrap swallows an
# OSError when not H.running(); we mirror that, plus the handle-nulled dispatch
# variants, for the framing locks the program holds itself).  NO frame is ever
# really torn mid-stream: in a comfortable window (funcs=5000 dur=60) the
# shared+control arms conserve exactly.
_TEARDOWN_EXC = (OSError, EOFError, struct.error, TypeError, ValueError)


def is_teardown_benign(H, exc):
    """True iff `exc` is a benign teardown-window slow-finisher: the run is OVER
    (not H.running() -- deadline passed / failed / done) AND the exception is the
    fd-close / cancelled-park / handle-nulled / torn-post-deadline-prefix family.
    Scoped STRICTLY to the teardown window: during the active run (H.running()
    True) this is always False, so a real mid-run OSError / TypeError / torn prefix
    still routes to H.error() and hard-fails -- the oracle stays strong."""
    return (not H.running()) and isinstance(exc, _TEARDOWN_EXC)


def run_shared_round(H, wid, rng, slot, state):
    """One SHARED-arm round: spawn PAIRS senders + PAIRS receivers on the ONE global
    shared Connection.  Each sender sends exactly one uniquely-tagged frame and each
    receiver recv()s exactly one frame, so the global sent count == global received
    count by pairing.  recv_bytes is serialized under recv_lock (send_bytes under
    send_lock) so the oracle tests CPython's framing under the M:N park, NOT the
    documented no-concurrent-reader contract -- but the prefix->park->body sequence
    still runs fully, and a SECOND reader fiber on another hub still contends the
    same pipe read-cursor.

    Because the shared Connection is GLOBAL, this round's receivers may legally pull
    frames sent by ANOTHER worker's concurrent round; conservation is therefore
    GLOBAL (asserted in post over every sent/received tag), not per-round.  Each
    sent tag is registered into state['sent_set'] and each verified received tag is
    appended to state['recv_list'], both under accounting locks distinct from the
    framing locks."""
    conn_a, conn_b = state["shared"]      # duplex socketpair: a sends, b receives
    send_lock = state["send_lock"]
    recv_lock = state["recv_lock"]
    sent_set = state["sent_set"]
    sent_guard = state["sent_guard"]
    recv_list = state["recv_list"]
    recv_guard = state["recv_guard"]
    short_tbl = state["short"]
    long_tbl = state["long"]

    # Build PAIRS unique offers in this worker's seq band.
    base_seq = wid * SEQ_STRIDE + state["seq_local"][slot]
    state["seq_local"][slot] += PAIRS
    offers = []
    for p in range(PAIRS):
        # Round-robin SHORT/LONG by (wid + p) so both framing cases are exercised
        # whether one worker does many rounds or many workers do one round each.
        case = (wid + p) % NCASES
        nkeys = SHORT_KEYS if case == CASE_SHORT else LONG_KEYS
        seq = base_seq + p
        key = UNIVERSE[seq % UNIVERSE_SIZE]
        offers.append((seq, key, make_payload(seq, key, nkeys)))
        if case == CASE_SHORT:
            short_tbl[slot] += 1
        else:
            long_tbl[slot] += 1

    send_wg = runloom.WaitGroup()
    send_wg.add(PAIRS)
    recv_wg = runloom.WaitGroup()
    recv_wg.add(PAIRS)

    def run_sender(offer):
        seq, key, payload = offer
        registered = False
        try:
            # Register the tag as SENT *before* the write so the global multiset is
            # complete even if a receiver on another hub pulls it instantly.
            with sent_guard:
                if seq in sent_set:
                    H.fail("shared arm: duplicate sent seq {0} -- the seq band "
                           "overlapped (test bug) or a frame was re-sent".format(seq))
                else:
                    sent_set[seq] = key
                    registered = True
            with send_lock:
                conn_a.send_bytes(payload)      # parks mid-write under wait_fd
        except Exception as exc:                # noqa: BLE001
            if is_teardown_benign(H, exc):
                # Teardown closed the fd / cancelled the park under this in-flight
                # send_bytes.  The frame did NOT fully write (send_bytes is held
                # whole under send_lock, so a raise means it never completed) -- so
                # no receiver can have pulled a valid full frame for it.  UN-register
                # the stranded seq so the global multiset stays balanced (a sender
                # the window cut off is not a "sent" frame), and mark teardown so
                # post() relaxes the exact count-equality to "received <= sent".
                if registered:
                    with sent_guard:
                        sent_set.pop(seq, None)
                state["teardown"][0] = 1
            else:
                H.error(slot, exc)
        finally:
            send_wg.done()

    def run_receiver():
        try:
            with recv_lock:
                raw = conn_b.recv_bytes()       # prefix -> wait_fd park -> body
        except Exception as exc:                # noqa: BLE001
            # A torn prefix can make struct.unpack raise, or a desynced/closed
            # stream raise EOFError/OSError -- a fault on this closed-world arm.
            # But at the deadline the harness closes the fd / cancels this parked
            # recv: that is a benign teardown slow-finisher, NOT a torn frame.
            # Scoped strictly to !H.running(): a mid-run torn prefix still fails.
            if is_teardown_benign(H, exc):
                state["teardown"][0] = 1
            else:
                H.error(slot, exc)
            recv_wg.done()
            return
        try:
            if H.failed:
                return
            tag = decode_and_check(H, raw, "shared")
            if tag is not None:
                with recv_guard:
                    recv_list.append(tag)
        finally:
            recv_wg.done()

    for offer in offers:
        H.fiber(run_sender, offer)
    for _ in offers:
        H.fiber(run_receiver)

    send_wg.wait()                        # all frames written
    recv_wg.wait()                        # all receivers joined this round


def run_control_round(H, wid, rng, slot, state):
    """One PRIVATE control round: a fresh single-owner Pipe pair driven by EXACTLY
    ONE sender fiber and ONE receiver fiber on different hubs.  This is the
    single-reader contract Connection actually supports, so the stream CANNOT
    desync by construction -- every frame must round-trip intact.  The sender writes
    all PAIRS frames sequentially; the receiver reads PAIRS frames sequentially,
    each prefix->wait_fd-park->body just like the shared arm but with NO competing
    reader.  A torn/dropped/duplicated frame HERE is the cooperative pipe transport
    itself losing or splicing bytes across the wait_fd park (NOT contention) -- a
    real runloom os.read/os.write framing bug.  Conservation is exact PER ROUND."""
    import multiprocessing

    base_seq = wid * SEQ_STRIDE + state["cseq_local"][slot]
    state["cseq_local"][slot] += PAIRS
    sent_tags = []
    offers = []
    for p in range(PAIRS):
        case = (wid + p) % NCASES
        nkeys = SHORT_KEYS if case == CASE_SHORT else LONG_KEYS
        seq = base_seq + p
        key = UNIVERSE[seq % UNIVERSE_SIZE]
        offers.append(make_payload(seq, key, nkeys))
        sent_tags.append((seq, key))

    priv_a, priv_b = multiprocessing.Pipe(duplex=True)
    received = []
    wg = runloom.WaitGroup()
    wg.add(2)                             # one sender fiber + one receiver fiber

    def run_sender():
        try:
            # ONE writer streams all frames in order -- no interleave on the fd.
            for payload in offers:
                priv_a.send_bytes(payload)      # parks mid-write under wait_fd
        except Exception as exc:          # noqa: BLE001
            # Teardown closed the private fd under this in-flight send -> benign
            # slow-finisher (the control round is abandoned, post() relaxes its
            # count check).  A mid-run (H.running()) failure still hard-fails.
            if is_teardown_benign(H, exc):
                state["teardown"][0] = 1
            else:
                H.error(slot, exc)
        finally:
            wg.done()

    def run_receiver():
        try:
            # ONE reader pulls PAIRS frames in order -- the prefix->park->body
            # reassembly runs uncontended, so the stream must stay in sync.
            for _ in range(PAIRS):
                if H.failed:
                    break
                raw = priv_b.recv_bytes()
                # After the deadline a cancelled/closed fd can yield a torn/short
                # post-deadline read; decode_and_check would call H.fail on it.
                # That is a benign teardown finisher, NOT a real torn frame -- the
                # control arm is race-free by construction WHILE running.  Stop
                # reading and let post() relax the count check.  During the active
                # run (H.running()) a torn control frame still fails immediately.
                if not H.running():
                    state["teardown"][0] = 1
                    break
                tag = decode_and_check(H, raw, "control")
                if tag is None:
                    break
                received.append(tag)
        except Exception as exc:          # noqa: BLE001
            if is_teardown_benign(H, exc):
                state["teardown"][0] = 1
            else:
                H.error(slot, exc)
        finally:
            wg.done()

    try:
        # Spawn the receiver on its own hub and the sender on another; they race the
        # SAME fd but never overlap a read with a read (single reader) -- the legal
        # supported mode, so the control arm is race-free by construction.
        H.fiber(run_receiver)
        H.fiber(run_sender)
        wg.wait()
    finally:
        try:
            priv_a.close()
            priv_b.close()
        except OSError:
            pass

    if H.failed:
        return
    # If the deadline passed mid-round the receiver may have read fewer than PAIRS
    # frames (its parked recv was cancelled / the fd closed) -- a benign teardown
    # short, NOT a dropped frame on the race-free control arm.  Don't judge the
    # count; just don't credit this partial round.  A round that fully ran WHILE
    # H.running() still gets the exact multiset conservation check below.
    if not H.running():
        state["teardown"][0] = 1
        return
    # Exact per-round conservation: multiset received == multiset sent.
    if check_round_conservation(H, sent_tags, received, "control"):
        state["csent"][slot] += len(sent_tags)
        state["crecv"][slot] += len(received)


def check_round_conservation(H, sent_tags, received, label):
    """Assert the multiset of received (seq, key) tags equals the multiset sent on
    a single-owner CONTROL round -- no frame torn (decode already caught), dropped,
    or duplicated.  Returns True on success."""
    if len(received) != len(sent_tags):
        H.fail("{0} arm: frame COUNT not conserved -- sent {1} frames but received "
               "{2} (a frame was dropped or duplicated on a single-owner pipe -- "
               "the cooperative transport lost bytes across a park)".format(
                   label, len(sent_tags), len(received)))
        return False
    sent_ms = {}
    for t in sent_tags:
        sent_ms[t] = sent_ms.get(t, 0) + 1
    recv_ms = {}
    for t in received:
        recv_ms[t] = recv_ms.get(t, 0) + 1
    if sent_ms != recv_ms:
        for t, n in recv_ms.items():
            if n != sent_ms.get(t, 0):
                H.fail("{0} arm: frame IDENTITY not conserved -- tag {1!r} received "
                       "{2}x but sent {3}x (single-owner pipe torn/duplicated a "
                       "frame across the park)".format(
                           label, t, n, sent_ms.get(t, 0)))
                return False
        for t, n in sent_ms.items():
            if t not in recv_ms:
                H.fail("{0} arm: frame LOST -- tag {1!r} sent {2}x but never "
                       "received on a single-owner pipe (transport dropped a "
                       "frame across the park)".format(label, t, n))
                return False
    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    for _ in H.round_range():
        if not H.running():
            break
        # Every round drives BOTH arms: the contended global shared Connection and
        # a fresh single-owner private control Pipe.
        run_shared_round(H, wid, rng, slot, state)
        if H.failed:
            return
        run_control_round(H, wid, rng, slot, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so multiprocessing.Pipe's
    # fds are pollable socketpairs and runloom.sync.Lock is the cooperative M:N-safe
    # lock.  ONE shared duplex Connection pair takes all the cross-hub frame
    # traffic; send/recv each get their OWN cooperative lock (send_bytes and
    # recv_bytes both issue several os.write/os.read calls per frame that would
    # otherwise interleave).  The sent registry (a dict seq->key) and the received
    # list each get a SEPARATE accounting lock, distinct from the framing locks so
    # the global multiset accounting never serializes the framing path under test.
    import multiprocessing
    conn_a, conn_b = multiprocessing.Pipe(duplex=True)
    H.register_close(conn_a)              # unblock a parked recv at shutdown
    H.register_close(conn_b)
    H.state = {
        "shared": (conn_a, conn_b),
        "send_lock": runloom.sync.Lock(),
        "recv_lock": runloom.sync.Lock(),
        # GLOBAL shared-arm registries (the shared Connection is global, so frames
        # from concurrent rounds interleave -> conservation is global, in post).
        "sent_set": {},                   # seq -> key, every shared frame sent
        "sent_guard": runloom.sync.Lock(),
        "recv_list": [],                  # every verified shared frame received
        "recv_guard": runloom.sync.Lock(),
        # CONTROL arm per-slot tallies (single-owner, exact per round).
        "csent": [0] * SLOTS,             # control-arm frames sent
        "crecv": [0] * SLOTS,             # control-arm frames received+verified
        "short": [0] * SLOTS,             # SHORT-body frames built
        "long": [0] * SLOTS,              # LONG-body frames built (multi-read park)
        "seq_local": [0] * SLOTS,         # per-slot shared seq cursor (single writer)
        "cseq_local": [0] * SLOTS,        # per-slot control seq cursor (single writer)
        # Set to 1 the first time a send/recv is cut off by the teardown-window
        # fd-close (deadline passed mid-flight).  When set, post() relaxes the
        # exact sent==received count to received<=sent: a frame the window stranded
        # in-flight is a benign slow-finisher, not a torn/dropped frame.  The STRONG
        # identity checks (received frame was sent, same key, no duplicates) always
        # run regardless -- a real torn/invented/duplicated frame still fails.
        "teardown": [0],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    # Runs in the MAIN process after the scheduler fully drained (every sender and
    # receiver joined its round WaitGroup), so the shared registries are quiescent.
    sent_set = H.state["sent_set"]
    recv_list = H.state["recv_list"]
    csent = sum(H.state["csent"])
    crecv = sum(H.state["crecv"])
    short = sum(H.state["short"])
    long_ = sum(H.state["long"])
    H.log("shared-arm frames sent={0} received={1}; control-arm sent={2} "
          "received={3}; bodies short={4} long={5}; ops={6}".format(
              len(sent_set), len(recv_list), csent, crecv, short, long_,
              H.total_ops()))

    H.check(H.total_ops() > 0,
            "no conservation rounds completed -- the recv_bytes framing race "
            "window was never exercised")
    H.check(len(sent_set) > 0,
            "shared Connection arm never exercised (no frames framed/received)")

    # ---- GLOBAL frame-identity conservation on the shared arm ----------------
    # Every per-frame decode (length/crc/universe) was fail-fast, so any tag in
    # recv_list is internally consistent.  Now reconcile the global multisets: the
    # multiset of RECEIVED (seq,key) tags must equal the multiset SENT -- no frame
    # dropped, duplicated, or attributed to a seq that was never sent.
    recv_seqs = {}
    bad = 0
    for seq, key in recv_list:
        recv_seqs[seq] = recv_seqs.get(seq, 0) + 1
        # Identity: the seq must have been sent, with the SAME key.  A received
        # (seq,key) whose key != the key that seq was sent with is a torn frame
        # (prefix of one frame spliced onto another's body) that slipped the crc.
        sent_key = sent_set.get(seq)
        if sent_key is None:
            if bad < 1:
                H.fail("shared arm: received frame with seq {0} (key {1:#x}) that "
                       "was NEVER sent -- an invented/torn frame from a desynced "
                       "concurrent reader on the shared pipe".format(seq, key))
            bad += 1
        elif sent_key != key:
            if bad < 1:
                H.fail("shared arm: frame seq {0} received with key {1:#x} but was "
                       "sent with key {2:#x} -- a torn frame (one frame's prefix on "
                       "another's body) that passed the crc".format(
                           seq, key, sent_key))
            bad += 1
    for seq, n in recv_seqs.items():
        if n > 1:
            H.fail("shared arm: frame seq {0} received {1}x -- a DUPLICATED frame "
                   "(the shared pipe stream desynced and re-yielded bytes)".format(
                       seq, n))
            break

    # Count conservation.  Every received frame is provably internally consistent
    # AND was sent with its exact key AND is unique (the strong identity checks
    # above run unconditionally -- a torn/invented/duplicated frame already failed).
    # The remaining COUNT direction (every SENT frame was also received) only holds
    # when the run drained the funcs*PAIRS frames through the one serialized fd
    # WITHIN --duration.  At funcs>=~6000 it cannot (the triage scale artifact): the
    # deadline strands thousands of frames in-flight and the teardown fd-close
    # cancels their parked sends/recvs -- a benign slow-finisher, not a dropped
    # frame.  When teardown stranded frames, relax sent==received to received<=sent:
    # no frame may be INVENTED (received without being sent -- still caught above),
    # but a frame the window simply never got to is benign.  In a comfortable window
    # (no teardown -- funcs=5000 dur=60) this stays the EXACT sent==received oracle,
    # so a genuinely torn/dropped frame still fires.
    teardown = H.state["teardown"][0]
    if not H.failed:
        if teardown:
            H.check(len(recv_list) <= len(sent_set),
                    "shared-arm conservation broken: received {0} frames but only "
                    "{1} were sent -- a frame was INVENTED/duplicated across the "
                    "recv park (received > sent even allowing teardown stranding)"
                    .format(len(recv_list), len(sent_set)))
        else:
            H.check(len(recv_list) == len(sent_set),
                    "shared-arm conservation broken: {0} frames sent != {1} received "
                    "-- a frame was torn / dropped / duplicated across the recv park"
                    .format(len(sent_set), len(recv_list)))
            missing = [s for s in sent_set if s not in recv_seqs]
            H.check(not missing,
                    "shared-arm conservation broken: {0} sent frame(s) never received "
                    "(e.g. seq {1}) -- a receiver consumed another frame's bytes and "
                    "the stream desynced/stranded a frame".format(
                        len(missing), missing[0] if missing else "-"))

    # ---- Single-owner CONTROL arm (exact per round) --------------------------
    H.check(csent == crecv,
            "control-arm conservation broken: single-owner private Pipe sent {0} "
            "!= received {1} -- the cooperative pipe transport itself dropped/"
            "spliced a frame across a park (not contention; one writer, one reader)"
            .format(csent, crecv))
    H.check(csent > 0,
            "control arm never exercised -- the single-owner falsifier ran no "
            "frames")

    # Both framing cases (short single-read, long multi-read-park) were exercised.
    H.check(short > 0,
            "no SHORT-body frames -- the single-read framing case was never hit")
    H.check(long_ > 0,
            "no LONG-body frames -- the multi-read prefix->park->body case (the "
            "core hazard) was never exercised")

    # A receiver stranded forever on a desynced stream parks-then-vanishes -> LOST.
    H.require_no_lost("frame-identity conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p446_mp_connection_recv_bytes_frami", body, setup=setup, post=post,
        default_funcs=3000,
        describe="many fibers send/recv self-describing length-prefixed frames over "
                 "ONE shared multiprocessing Connection (recv serialized, a 2nd "
                 "reader contends the same pipe fd) while a single-owner PRIVATE "
                 "Pipe runs unlocked as control; FRAME IDENTITY conservation: "
                 "declared-len==actual, crc matches, key in universe, multiset "
                 "received==sent -- a torn/dropped/duplicated frame fails")
