"""Safe ``sys._current_frames()`` for the free-threaded M:N runtime.

THE HAZARD.  ``sys._current_frames()`` (free-threaded 3.13t) stop-the-worlds only
the SNAPSHOT (gh-117300) but hands back ``PyFrameObject``s whose ``f_frame`` still
points at the *owning thread's live* ``_PyInterpreterFrame``.  Reading
``frame.f_lineno`` / ``frame.f_code`` later is unsynchronised against that owner
popping the frame (CPython ``frame.c`` ``take_ownership`` / ``_PyThreadState_PopFrame``)
-- the official 3.13 free-threading HOWTO states verbatim that
``sys._current_frames()`` "is generally not safe to use in a free-threaded build
... may cause your program to crash."  In stock CPython the vacated datastack stays
mapped (one chunk of hysteresis) so it is a benign stale read; under runloom the
goroutine's datastack chunk is recycled, so a *held* stale frame becomes a hard
use-after-free on arm64 (big_100 p200 SIGSEGV).

THE FIX (layer C; the chunk-pool GRACE delay-ring is layer E).  Rebind
``sys._current_frames`` to a wrapper that, IMMEDIATELY after the real call (while
the chunk GRACE-ring still guarantees the just-vacated chunk bytes are intact --
no reuse for RUNLOOM_CHUNK_GRACE further completions), reads each live frame and
returns a SELF-CONTAINED snapshot view.  The caller then holds views with no live
``f_frame``, so an arbitrarily kernel-descheduled reader (e.g. p200's foreign OS
thread holding frames across a 1 ms sleep) can never deref a reused chunk.  This
converts every cross-thread frame read from "held across an unbounded wait" into
"immediate, GRACE-covered" -- the same safety the goroutine sampler already enjoys.

LIMITS (honest).  This closes every caller that routes through the rebound
``sys._current_frames`` -- which is all in-scheduler code and any app that looks
the function up after ``patch()``.  A truly foreign C thread, or Python that
captured ``sys._current_frames`` BEFORE ``patch()``, bypasses the wrapper (same
caveat as the stdlib lock singletons).  And the wrapper's own immediate reads are
GRACE-covered, not synchronised -- the data race is fundamentally CPython's; the
source fix is a CPython patch (see docs/dev/frame_uaf.md).  Disable with
RUNLOOM_SAFE_CURRENT_FRAMES=0.

House style: .format(), no f-strings.
"""
import os
import sys

_orig_current_frames = None


class _FrameView(object):
    """A self-contained snapshot of a frame -- a drop-in for the common read
    surface (f_code/f_lineno/f_lasti/f_back/f_globals/f_builtins).  Holds only a
    ref to the code/globals objects (NOT in the recyclable datastack chunk) and
    plain ints; never a live ``f_frame``, so it is safe to read at any later time
    from any thread.  f_locals is intentionally empty: materialising a live
    frame's locals dereferences the iframe heavily and is exactly the unsafe op
    this view exists to avoid."""

    __slots__ = ("f_code", "f_lineno", "f_lasti", "f_back", "f_globals",
                 "f_builtins", "f_locals", "f_trace", "f_trace_lines",
                 "f_trace_opcodes")

    def __init__(self, code, lineno, lasti, glob, builtins):
        self.f_code = code
        self.f_lineno = lineno
        self.f_lasti = lasti
        self.f_globals = glob
        self.f_builtins = builtins
        self.f_locals = {}
        self.f_back = None
        self.f_trace = None
        self.f_trace_lines = True
        self.f_trace_opcodes = False

    def clear(self):
        return None

    def __repr__(self):
        name = getattr(self.f_code, "co_name", "?")
        fn = getattr(self.f_code, "co_filename", "?")
        return "<runloom frame snapshot {0!r} in {1!r}, line {2}>".format(
            name, fn, self.f_lineno)


def _snapshot(frame):
    """Snapshot ONLY the topmost frame -- the just-active frame _current_frames
    points at -- with f_back = None.  Deliberately does NOT walk the f_back chain:
    the ancestor frames are exactly the ones the owning goroutine UNWINDS through
    (CPython take_ownership / PopFrame mid-flip as it returns), and reading a frame
    mid-pop is a torn-pointer crash the GRACE-ring cannot cover (the ring covers
    chunk REUSE, not the pop transition).  The topmost frame is the one the owner
    is EXECUTING, not popping, so its immediate read is as safe as the goroutine
    sampler's, and is what callers of _current_frames overwhelmingly want ("what
    line is each thread on").  Walking another thread's full chain via
    _current_frames is the operation CPython's docs call unsafe; there is no way to
    snapshot it without racing the unwind, so f_back is truncated to None rather
    than crash-or-lie."""
    code = frame.f_code
    lineno = frame.f_lineno
    lasti = frame.f_lasti
    glob = frame.f_globals
    builtins = frame.f_builtins
    return _FrameView(code, lineno, lasti, glob, builtins)


def safe_current_frames():
    """Drop-in for sys._current_frames returning {thread_id: _FrameView-chain}.
    The returned views hold no live frame, so reading them later (cross-thread,
    descheduled) cannot use-after-free a recycled datastack chunk."""
    raw = _orig_current_frames()
    out = {}
    for tid, top in raw.items():
        out[tid] = _snapshot(top)
    return out


def enabled():
    return os.environ.get("RUNLOOM_SAFE_CURRENT_FRAMES", "1") not in ("0", "", "off")


def install():
    """Rebind sys._current_frames to the snapshotting wrapper.  Idempotent."""
    global _orig_current_frames
    if not enabled():
        return
    if _orig_current_frames is not None:
        return
    cf = getattr(sys, "_current_frames", None)
    if cf is None or getattr(cf, "__module__", None) == __name__:
        return
    _orig_current_frames = cf
    sys._current_frames = safe_current_frames


def uninstall():
    """Restore the real sys._current_frames."""
    global _orig_current_frames
    if _orig_current_frames is None:
        return
    sys._current_frames = _orig_current_frames
    _orig_current_frames = None
