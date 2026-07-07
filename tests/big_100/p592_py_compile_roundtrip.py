"""big_100 / 592 -- py_compile.compile source->pyc round-trip determinism under M:N.

py_compile.compile(src, cfile=...) reads a SOURCE .py file, hands the bytes to the
builtin compile()/marshal machinery, prepends a 16-byte pyc header (magic + bit
field + source-hash or mtime/size) and writes the resulting .pyc atomically.  With
PycInvalidationMode.UNCHECKED_HASH the header carries the source HASH (not the file
mtime), so the ENTIRE .pyc byte stream is a pure, deterministic function of the
source bytes alone: identical source bytes -> byte-identical .pyc, every time, with
no wall-clock/mtime term to perturb it.

WHERE M:N COULD BREAK IT (the gap this program probes).  py_compile.compile is meant
to be a stateless, reentrant pure-ish function: read file -> compile -> marshal ->
write file.  Under runloom with the GIL off and tens of thousands of goroutines each
compiling their OWN fiber-local source across hubs, the hazards are:
  * the compile/marshal path leaks a sibling fiber's code object, constant pool, or
    intermediate buffer into THIS fiber's .pyc (a cross-fiber leak of single-owner
    state) -- the produced bytes would then differ from a re-compile of the SAME
    source, or the marshalled code would execute to a SIBLING's constant;
  * a torn write/marshal buffer under concurrent mutation yields a .pyc whose
    embedded code object executes to a WRONG value or fails to unmarshal;
  * the compile output is non-deterministic across a yield (the same source bytes,
    compiled before and after a hub migration, produce different .pyc bytes) --
    which for UNCHECKED_HASH (no mtime term) can only be a runtime corruption.

SINGLE-OWNER, CLOSED-FORM ORACLE (verified deterministic, see the prototype in the
task log).  Each fiber OWNS its own tmp subdir and one source file.  Per iteration it
embeds a UNIQUE per-fiber constant  v = wid*STRIDE + idx  into fiber-local source
text (RESULT = v, plus a tiny function derived from v and a mixed-type const tuple so
the marshalled const pool is non-trivial).  It then:
  1. writes the source file (fiber-local, single-owner),
  2. compiles it to pyc #1 via py_compile.compile(UNCHECKED_HASH) and reads the bytes,
  3. YIELDS (runloom.yield_now / sleep) so a sibling reliably interleaves its own
     compile on this or another hub,
  4. re-compiles the SAME source file to pyc #2 and reads the bytes,
  5. asserts pyc #2 == pyc #1  BYTE-FOR-BYTE (determinism across the yield -- with
     UNCHECKED_HASH there is no mtime term, so any difference is a runtime bug, not a
     documented timestamp), and
  6. unmarshals the code object out of pyc #1 (strip the 16-byte header, marshal.loads
     the rest), execs it in a FRESH namespace, and asserts RESULT == v exactly and
     the compiled function returns its closed-form value -- i.e. the produced code
     object carries THIS fiber's constant, never a sibling's (no cross-fiber leak).

Single-owner: the source file, both pyc byte strings, the unmarshalled code object and
the exec namespace are all fiber-local -- nothing is shared, so a mismatch cannot be
"documented shared-object races".  It can only be a runloom isolation/tearing bug.

COMPLETENESS (post): require_no_lost -- a fiber that vanished mid-compile (stranded in
the offloaded file read/write or the marshal loop) never returns; the watchdog +
require_no_lost catch it.
NON-VACUITY (post): the load-bearing arm actually ran (compile_checks > 0).

File-heavy (each iteration does a source write + two pyc writes + reads through the
monkey offload), so max_funcs is capped so the forever loop's --funcs 1000000 does not
try to field a million concurrent compilers.

Stresses: py_compile.compile source->pyc pipeline (file read, builtin compile,
marshal.dumps of the code object + const pool, atomic pyc write), UNCHECKED_HASH
header determinism, marshal.loads round-trip of the produced code object, all across
hub migration + yield with tens of thousands of fibers each owning a distinct source.
"""
import marshal
import os

import py_compile

import harness
import runloom

# 16-byte pyc header since CPython 3.7: magic(4) + bitfield(4) + hash-or-mtime(8).
# Under UNCHECKED_HASH the trailing 8 bytes are the source hash, so the whole
# header is a deterministic function of the source bytes (no wall-clock term).
PYC_HEADER_LEN = 16

# Per-fiber unique constant stride.  v = wid*STRIDE + idx is unique across all
# (wid, idx) pairs for idx < STRIDE, so no two fibers ever embed the same value.
STRIDE = 1000003

# Bound the inner compile loop per round so a single worker at --rounds 0 keeps
# yielding control (and idx stays < STRIDE so v remains unique per fiber).
INNER_CAP = 100000

INVALIDATION = py_compile.PycInvalidationMode.UNCHECKED_HASH


def make_source(v):
    """Fiber-local source text embedding the unique constant v.

    Includes a function whose return value is a closed form of v and a mixed-type
    constant tuple, so the marshalled const pool has ints, a string, a tuple and a
    float -- a non-trivial marshal payload, not just a bare assignment."""
    return (
        "RESULT = {0}\n"
        "CONSTS = ({0}, {1}, 'v{0}', 3.5)\n"
        "def derive(x):\n"
        "    return x * 2 + RESULT - 7\n"
    ).format(v, v + 1)


def expected_derive(x, v):
    return x * 2 + v - 7


def compile_check(H, wid, idx, srcpath, pycpath):
    """Single-owner py_compile round-trip check for one unique fiber-local value."""
    v = wid * STRIDE + idx

    # (1) write fiber-local source.
    src = make_source(v)
    with open(srcpath, "w") as f:
        f.write(src)

    # (2) compile -> pyc #1, read bytes.
    py_compile.compile(srcpath, cfile=pycpath, doraise=True,
                       invalidation_mode=INVALIDATION)
    with open(pycpath, "rb") as f:
        b1 = f.read()

    # (3) YIELD so a sibling interleaves its own compile before we recompile.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # (4) re-compile the SAME source -> pyc #2, read bytes.
    py_compile.compile(srcpath, cfile=pycpath, doraise=True,
                       invalidation_mode=INVALIDATION)
    with open(pycpath, "rb") as f:
        b2 = f.read()

    # (5) determinism: with UNCHECKED_HASH there is NO mtime term, so identical
    # source bytes MUST yield byte-identical pyc.  A difference is a runtime
    # tearing / cross-fiber leak, never a documented timestamp effect.
    if b1 != b2:
        # Localize where the streams diverge for the report.
        n = min(len(b1), len(b2))
        pos = next((i for i in range(n) if b1[i] != b2[i]), n)
        H.fail("py_compile NON-DETERMINISTIC across yield (wid {0} v {1}): pyc "
               "recompile of IDENTICAL source differs -- len {2} vs {3}, first "
               "diff at byte {4} (UNCHECKED_HASH has no mtime term, so this is a "
               "torn/cross-fiber-leaked compile output, not a timestamp)".format(
                   wid, v, len(b1), len(b2), pos))
        return

    if len(b1) <= PYC_HEADER_LEN:
        H.fail("py_compile produced a truncated pyc (wid {0} v {1}): len {2} <= "
               "header {3} -- torn/short write of the marshalled code".format(
                   wid, v, len(b1), PYC_HEADER_LEN))
        return

    # (6) unmarshal the produced code object and exec it in a FRESH namespace;
    # assert it carries THIS fiber's constant, not a sibling's leaked value.
    try:
        code = marshal.loads(b1[PYC_HEADER_LEN:])
    except Exception as e:
        H.fail("marshal.loads of the produced pyc FAILED (wid {0} v {1}): {2!r} "
               "-- a torn/corrupted marshal payload from the compile pipeline "
               "under M:N".format(wid, v, e))
        return

    ns = {}
    try:
        exec(code, ns)
    except Exception as e:
        H.fail("exec of the compiled code object FAILED (wid {0} v {1}): {2!r} "
               "-- corrupted bytecode/const pool from the compile pipeline".format(
                   wid, v, e))
        return

    if ns.get("RESULT") != v:
        H.fail("compiled RESULT WRONG (wid {0}): got {1!r}, expected {2} -- the "
               "produced code object carries a SIBLING's constant (cross-fiber "
               "leak in the compile/marshal path)".format(wid, ns.get("RESULT"), v))
        return

    consts = ns.get("CONSTS")
    if consts != (v, v + 1, "v{0}".format(v), 3.5):
        H.fail("compiled CONSTS pool WRONG (wid {0} v {1}): got {2!r} -- torn or "
               "cross-fiber-leaked constant tuple in the marshalled code".format(
                   wid, v, consts))
        return

    derive = ns.get("derive")
    got = derive(idx + 11)
    exp = expected_derive(idx + 11, v)
    if got != exp:
        H.fail("compiled derive() WRONG (wid {0} v {1}): derive({2})={3}, "
               "expected {4} -- the code object's bytecode computed from a wrong "
               "RESULT (cross-fiber leak / torn code)".format(
                   wid, v, idx + 11, got, exp))
        return

    H.state["checks"][wid] += 1


def worker(H, wid, rng, state):
    d = os.path.join(state["base"], "w{0}".format(wid))
    os.makedirs(d, exist_ok=True)
    srcpath = os.path.join(d, "m.py")
    pycpath = os.path.join(d, "m.pyc")
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            compile_check(H, wid, idx, srcpath, pycpath)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    base = H.make_tmpdir("big100_pycomp_")
    H.state = {
        "base": base,
        "checks": [0] * H.funcs,       # one slot per worker (single-writer, race-free)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("py_compile single-owner round-trip checks (all passed fail-fast): "
          "{0}; ops={1}".format(checks, H.total_ops()))
    # NON-VACUITY: the load-bearing compile round-trip actually ran.
    H.check(checks > 0,
            "no py_compile round-trip checks ran -- the compile determinism/leak "
            "hazard was never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid-compile.
    H.require_no_lost("py_compile round-trip completeness")


if __name__ == "__main__":
    harness.main(
        "p592_py_compile_roundtrip", body, setup=setup, post=post,
        default_funcs=4000,
        max_funcs=1000,                # file-heavy: source + two pyc writes per iter
        describe="each fiber owns a source file and embeds a UNIQUE constant v, "
                 "compiles it to a .pyc via py_compile.compile(UNCHECKED_HASH), "
                 "yields, then re-compiles the SAME source and asserts the pyc is "
                 "BYTE-IDENTICAL (deterministic, no mtime term) and the "
                 "unmarshalled+exec'd code object carries THIS fiber's constant "
                 "(RESULT==v, const pool + derive() closed-form) -- a differing "
                 "pyc across the yield or a leaked sibling constant is a runloom "
                 "compile-path isolation/tearing bug")
