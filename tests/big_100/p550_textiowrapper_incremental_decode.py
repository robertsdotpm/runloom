"""big_100 / 550 -- io.TextIOWrapper incremental UTF-8 decode state held across a park.

io.TextIOWrapper decodes its underlying binary stream INCREMENTALLY.  It reads a
chunk of bytes from the buffer (buffer.read1) and feeds them to a stateful
incremental UTF-8 decoder (codecs' _io.IncrementalNewlineDecoder wrapping a C
utf-8 IncrementalDecoder).  When a chunk boundary falls in the MIDDLE of a
multibyte UTF-8 sequence (a 2/3/4-byte char split across two read1() calls), the
decoder retains the leading bytes as PENDING state and emits nothing until the
continuation bytes of that same character arrive on the next chunk.  That pending-
bytes buffer -- a few bytes of C-level decoder state (Modules/_io/textio.c's
`decoder` slot, plus the codec's own pending buffer) -- is the load-bearing thing
this program stresses.

WHERE M:N BREAKS IT (the gap this program probes).  Under pygo the fiber that is
mid-way through TextIOWrapper.read() can PARK: this program drives the underlying
raw stream one 1-3 byte chunk at a time and yields the fiber INSIDE the raw
readinto(), i.e. while the C TextIOWrapper.read() call frame is live on the
stackful coroutine's saved native stack AND the incremental decoder is holding
the leading bytes of a split multibyte character.  A sibling fiber on another hub
is simultaneously doing the exact same thing over ITS OWN TextIOWrapper.  If the
runtime mishandled that saved-across-park C state -- dropped the decoder's pending
bytes, resumed the fiber with a sibling's residual continuation bytes, or tore the
decoder object -- the resumed read() would either raise UnicodeDecodeError on a
now-illegal byte sequence or silently reconstruct the WRONG text.  On a correct
runtime the pending bytes are part of the fiber's own saved stack + its own single-
owner decoder object, so the split character is completed correctly after the park
and the decoded text is byte-for-byte the original.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner value conservation):

  Each fiber owns EVERYTHING: a fresh source string of KNOWN multibyte UTF-8
  (mixing 1/2/3/4-byte characters, tagged with the fiber's wid so a cross-fiber
  residual leak is detectable as wrong content), its own BytesIO holding that
  string's UTF-8 bytes, its own tiny-chunk raw wrapper, its own BufferedReader,
  and its own io.TextIOWrapper.  NOTHING is shared between fibers -- so a shared-
  mutable-container race is structurally impossible and cannot manufacture a false
  positive.  The fiber reads the whole TextIOWrapper in small character counts,
  parking (yield) between every read AND inside every 1-3 byte raw readinto (the
  hazard boundary: the decoder is holding a split multibyte char's leading bytes
  when the fiber is descheduled).  The oracle, checked fail-fast:

    * NO UnicodeDecodeError fires (the source is valid UTF-8; an error means a
      pending-bytes byte was dropped or a foreign byte was injected across a park);
    * the concatenation of every piece read back == the original source string
      EXACTLY (value conservation: every character survived the split-across-park
      decode, none lost/duplicated/corrupted);
    * the reconstructed length equals the source length (belt-and-suspenders on
      the exact-equality check).

  Because the decoder + its pending bytes are single-owner and never shared, a
  CORRECT runtime ALWAYS reconstructs the source exactly -- the program exits 0
  when there is no bug.  A mismatch or a UnicodeDecodeError is therefore a real
  runtime fault: decoder pending-state lost across a park, a sibling's residual
  bytes fed into this fiber's decoder, or a torn TextIOWrapper/decoder object.

  This is a CONSERVATION program (like p405): the closed-world law is "bytes in ==
  characters out, exactly" per fiber, verified after the whole stream is drained.

ORACLES:
  * LOAD-BEARING -- SINGLE-OWNER DECODE CONSERVATION (worker, HARD, fail-fast):
    per round, reconstruct == source and no UnicodeDecodeError.
  * NON-VACUITY (post, HARD): characters were actually decoded (chars_decoded > 0)
    AND the hazard boundary was actually crossed -- fibers parked INSIDE readinto
    while the decoder held split-multibyte pending bytes (parks > 0).  Without the
    parks tally the oracle could pass vacuously if the tiny-chunk splitting never
    happened.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-read
    (parked inside TextIOWrapper.read with pending decoder bytes, never re-woken)
    never returns; the watchdog + require_no_lost catch it.

Distinct from p413/p414 (raw pipe BufferedReader/BufferedWriter, NO codec) and
p415 (BytesIO.getbuffer export count): this is the incremental-CODEC pending-state
angle, and it uses only in-memory BytesIO so there are NO file descriptors.

Stresses: io.TextIOWrapper incremental decode, the UTF-8 IncrementalDecoder's
pending-bytes buffer held across a stackful-coroutine park, multibyte sequences
split at 1-3 byte chunk boundaries, TextIOWrapper.read() C frame saved+restored
across a cooperative yield deep inside a raw readinto callback, per-fiber decoder
isolation under M:N with the GIL off.

Good TSan / controlled-M:N-replay target: the incremental decoder's pending-bytes
buffer is written by the C decode step and read by the next; a data-race report on
that buffer, or a replay that resumes a parked read() with a stale/foreign pending
buffer, localizes a lost/leaked continuation byte before the exact-string
conservation check even closes.
"""
import io

import harness
import runloom

# A palette of characters spanning all four UTF-8 encoded lengths.  Every entry is
# unambiguously valid UTF-8, and the multibyte ones (2/3/4 bytes) are what get
# SPLIT across the 1-3 byte raw chunk boundaries -- forcing the incremental decoder
# to hold pending leading bytes across a park.
ASCII_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"          # 1 byte each
TWO_BYTE = "éñüßαβγà"  # e' n~ u" ss alpha ...
THREE_BYTE = "中文あ★€ꬰカ"      # CJK/kana/star/euro
FOUR_BYTE = "\U0001f600\U0001f389\U0001f680\U0001f4a9"        # emoji (4 bytes)

# Per-round source length in characters.  Short enough that 6000 fibers each churn
# many rounds under the timeout, long enough that dozens of multibyte splits occur.
SRC_MIN_CHARS = 24
SRC_MAX_CHARS = 64


def build_source(rng, wid, rnd):
    """Build a KNOWN, valid-UTF-8 source string for this fiber+round.

    Tagged with the fiber's wid and round so a cross-fiber residual-byte leak would
    show up as WRONG content (not just a length mismatch).  Deliberately mixes
    1/2/3/4-byte characters so that 1-3 byte raw chunks slice through the middle of
    multibyte sequences, exercising the decoder's pending-bytes state.  Contains NO
    newline characters, and the TextIOWrapper is built with newline='' so universal-
    newline translation never alters the content (which would break exact
    reconstruction for a benign, documented reason)."""
    # A recognizable, per-fiber prefix so leaked content is caught by value, not
    # only by length.  Uses multibyte chars right next to ascii digits so the
    # boundary splits land on the tag too.
    parts = ["W", str(wid), "¶", "R", str(rnd), "‖"]   # pilcrow / double-bar
    n = rng.randint(SRC_MIN_CHARS, SRC_MAX_CHARS)
    for _ in range(n):
        bucket = rng.randint(0, 3)
        pool = (ASCII_CHARS, TWO_BYTE, THREE_BYTE, FOUR_BYTE)[bucket]
        parts.append(pool[rng.randrange(len(pool))])
    return "".join(parts)


class TinyChunkRaw(io.RawIOBase):
    """A readable raw binary stream that hands out only 1-3 bytes per readinto()
    and PARKS THE FIBER inside each readinto -- the hazard boundary.

    Backed by an in-memory io.BytesIO (no fds).  Because it returns at most 3 bytes
    per call, TextIOWrapper's chunked decode repeatedly stalls in the MIDDLE of a
    multibyte character, with the leading bytes held as pending decoder state.  The
    runloom.yield_now() inside readinto deschedules the fiber at exactly that
    moment, with the live C TextIOWrapper.read() frame on the saved native stack.

    Single-owner: each fiber constructs its own TinyChunkRaw; no sharing."""

    def __init__(self, data, rng):
        io.RawIOBase.__init__(self)
        self.bio = io.BytesIO(data)
        self.rng = rng
        self.parks = 0                 # readinto yields = hazard-boundary crossings

    def readable(self):
        return True

    def readinto(self, b):
        # Hand out at most 1-3 bytes, guaranteeing many mid-multibyte splits.
        want = self.rng.randint(1, 3)
        chunk = self.bio.read(min(want, len(b)))
        if not chunk:
            return 0                   # EOF: TextIOWrapper finalizes the decoder
        n = len(chunk)
        b[:n] = chunk
        # PARK while the incremental decoder may be holding a split character's
        # leading bytes -- the whole point of the program.  A sibling fiber on
        # another hub is in the same state on its OWN TextIOWrapper right now.
        self.parks += 1
        runloom.yield_now()
        return n


def decode_round(H, wid, rnd, rng, state):
    """One single-owner decode-conservation round: build a known UTF-8 source, feed
    it through a private tiny-chunk TextIOWrapper reading a few characters at a time
    (parking between reads and inside every raw chunk), and assert the reconstructed
    text equals the source EXACTLY with no UnicodeDecodeError."""
    source = build_source(rng, wid, rnd)
    data = source.encode("utf-8")

    raw = TinyChunkRaw(data, rng)
    buf = io.BufferedReader(raw)
    tw = io.TextIOWrapper(buf, encoding="utf-8", errors="strict", newline="")

    out = []
    try:
        while True:
            # Read a small, varying number of CHARACTERS.  Each read() spans several
            # 1-3 byte raw chunks (several parks), each of which may leave the
            # decoder holding a split character across the park.
            piece = tw.read(rng.randint(1, 3))
            if piece == "":
                break
            out.append(piece)
            runloom.yield_now()        # park between reads too (decoder at rest here)
    except UnicodeDecodeError as exc:
        # The source is valid UTF-8 and the decoder is single-owner: a decode error
        # can only mean the pending continuation bytes were dropped across a park or
        # a sibling's residual bytes were injected into this fiber's decoder.
        H.fail("TextIOWrapper raised UnicodeDecodeError on valid single-owner UTF-8 "
               "(wid {0} round {1}): {2!r} -- the incremental decoder's pending "
               "split-multibyte bytes were lost or a foreign byte was injected "
               "across a park".format(wid, rnd, exc))
        return

    result = "".join(out)

    # CONSERVATION: exact value reconstruction.  A single lost/duplicated/corrupted
    # character (a decoder pending-state desync across the park) breaks this.
    if result != source:
        # Localize the first divergence for the failure message.
        lim = min(len(result), len(source))
        div = next((i for i in range(lim) if result[i] != source[i]), lim)
        H.fail("TextIOWrapper decode MISMATCH (wid {0} round {1}): reconstructed "
               "len {2} vs source len {3}, first divergence at char {4} "
               "(got {5!r}, want {6!r}) -- the incremental UTF-8 decoder's pending "
               "bytes were dropped or fed a sibling's residual across a park".format(
                   wid, rnd, len(result), len(source), div,
                   result[div:div + 4], source[div:div + 4]))
        return

    # Redundant length check (the exact compare above already implies it) kept as an
    # explicit belt-and-suspenders on the conservation law.
    if len(result) != len(source):
        H.fail("TextIOWrapper decode length conservation broken (wid {0}): "
               "{1} chars out for {2} chars in".format(wid, len(result), len(source)))
        return

    state["chars"][wid] += len(result)          # single-writer-per-slot (race-free)
    state["parks"][wid] += raw.parks            # hazard-boundary crossings this round
    state["rounds"][wid] += 1


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        decode_round(H, wid, rnd=state["rounds"][wid], rng=rng, state=state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Per-worker slots (one writer per slot, allocated where H.funcs is known ->
    # race-free conservation/non-vacuity tallies; NEVER wid & MASK for these).
    H.state = {
        "chars": [0] * H.funcs,     # characters decoded (conservation / non-vacuity)
        "parks": [0] * H.funcs,     # readinto parks = hazard boundary crossings
        "rounds": [0] * H.funcs,    # completed decode rounds
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    total_chars = sum(H.state["chars"])
    total_parks = sum(H.state["parks"])
    total_rounds = sum(H.state["rounds"])
    H.log("TextIOWrapper single-owner decode conservation: {0} chars reconstructed "
          "EXACTLY across {1} rounds (every round's decode == source, no "
          "UnicodeDecodeError, fail-fast); {2} mid-decode parks (fibers parked "
          "inside readinto with split-multibyte pending decoder state)".format(
              total_chars, total_rounds, total_parks))

    # NON-VACUITY: the load-bearing decode arm actually ran.
    H.check(total_chars > 0,
            "no characters decoded -- the TextIOWrapper incremental-decode "
            "conservation oracle was never exercised (vacuous run)")
    # NON-VACUITY of the HAZARD: fibers actually parked mid-decode with pending
    # split-multibyte bytes.  Without this the conservation check could pass without
    # ever crossing the boundary the program exists to stress.
    H.check(total_parks > 0,
            "no mid-decode parks recorded -- the tiny-chunk raw never split a "
            "multibyte sequence across a park, so the decoder-pending-state hazard "
            "boundary was never crossed")

    # COMPLETENESS: no fiber parked-then-vanished mid-read (stranded inside
    # TextIOWrapper.read holding pending decoder bytes with no waker).
    H.require_no_lost("textiowrapper incremental decode conservation")


if __name__ == "__main__":
    harness.main(
        "p550_textiowrapper_incremental_decode", body, setup=setup, post=post,
        default_funcs=6000,
        describe="each fiber reads its OWN io.TextIOWrapper over a BytesIO of KNOWN "
                 "multibyte UTF-8 in tiny (1-3 byte) chunks, parking the fiber "
                 "INSIDE readinto while the incremental UTF-8 decoder holds a split "
                 "multibyte character's pending leading bytes.  LOAD-BEARING single-"
                 "owner conservation: the concatenated reconstruction equals the "
                 "source string EXACTLY and no UnicodeDecodeError fires -- a "
                 "mismatch or decode error means the decoder's pending bytes were "
                 "dropped, or a sibling's residual bytes were fed into this fiber's "
                 "decoder, across the park (a real runtime fault; single-owner so a "
                 "shared-container race cannot manufacture it)")
