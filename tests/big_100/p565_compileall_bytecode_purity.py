# REGRESSION GUARD for a CPython 3.14t thread-local-bytecode (TLBC) SIGSEGV:
# compileall/marshal.loads code-object creation under runloom's many-hub stackful
# execution corrupts a hub pthread stack/TCB (mimalloc arena aliasing) -> SIGSEGV
# in __tls_get_addr.  Root-caused via rr to CPython 3.14t's TLBC, NOT a runloom
# bug (greenlet ships the same PYTHON_TLBC=0 mitigation).  runloom.run() now
# re-execs ft-3.14 with PYTHON_TLBC=0 (src/runloom/runtime.py _tlbc_reexec_if_needed),
# so this is a normal PASSing program; run with RUNLOOM_TLBC=1 to re-arm the crash.

"""big_100 / 565 -- compileall.compile_file bytecode PURITY + determinism under M:N.

compileall.compile_file(path) drives the whole source->bytecode pipeline:
py_compile.compile -> builtins.compile (the C compiler/parser/optimizer/codegen)
-> importlib._bootstrap_external writes a .pyc (16-byte header + marshalled code
object).  The C compiler is the interesting free-threading surface here: parsing,
constant folding, the peephole/optimizer passes, and marshal all run on per-call
arena state, but they also touch PROCESS-GLOBAL structures (the interned-string
table, the small-int / singleton caches, the marshal writer).  Under M:N with the
GIL off, many hubs each compiling a DISTINCT fiber-local source module at the same
time is exactly the concurrency that would surface a torn compile: a code object
whose bytecode was corrupted mid-codegen by a sibling's concurrent compilation, or
a non-deterministic .pyc for a source that must compile bit-identically.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom runs each fiber's
compileall.compile_file call in parallel across hubs.  If the C compiler's global
state (interning, caches, the marshal path) is not free-threading-safe, a fiber
that compiles its OWN source and gets back a code object could observe:
  * WRONG bytecode -- executing the compiled module yields a RESULT that differs
    from the independently-recomputed closed-form value (a codegen corruption);
  * NON-DETERMINISTIC bytecode -- compiling the SAME fiber-local source twice
    (once before a yield, once after) produces DIFFERENT .pyc bytes, which for a
    hash-invalidation .pyc of a fixed source must be bit-identical (a torn compile
    under a sibling's concurrent compilation);
  * a compile that spuriously FAILS on valid source, or a SIGSEGV in the compiler.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner, closed-form).  Each fiber owns
a private temp directory and a private source file.  The source is generated from a
fiber-local template with fiber-local numeric constants, and it computes a single
RESULT via a small function + a list comprehension + constant folding -- enough to
exercise real codegen, while the value is a deterministic closed form.  The
reference() helper recomputes that EXACT closed form in straight Python (an
independent computation, NOT by executing the compiled module), so the oracle is a
PURITY / round-trip law:

  compile the fiber-local source with compileall.compile_file (hash-invalidation,
  force) -> read the .pyc -> marshal.loads the code object -> exec it in a FRESH
  namespace -> the module's RESULT MUST equal reference(...).  Across a yield,
  recompile the SAME source and assert the .pyc bytes are bit-identical to the
  first compile (deterministic compilation) and RESULT is still exactly the closed
  form.  Everything is single-owner: the temp dir, the source file, the .pyc, and
  the exec namespace are never shared between fibers.

Verified against plain threads: 8 OS threads each compiling their own distinct
source (GIL on and off) produce correct + bit-identical bytecode 100% of the time
-- 0 wrong-value, 0 non-deterministic.  Under a CORRECT runloom it must also hold,
so this program EXITS 0 when there is no bug.  A wrong RESULT, a byte-differing
recompile of a fixed source, a spurious compile failure, or a crash is a real
runtime (or CPython-compiler) fault, not documented Python semantics.

ORACLES:
  * LOAD-BEARING -- BYTECODE PURITY + DETERMINISM (worker, HARD, fail-fast).
    Single-owner source -> compileall.compile_file -> code object; executed RESULT
    == independently recomputed closed form, and a second compile of the same
    source is bit-identical, across a yield.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside the
    compiler / file write / marshal never returns; the watchdog + require_no_lost
    catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (compile_checks>0).

FAIL ON: compile_file returning False on valid source, executed RESULT != closed
form, a non-deterministic .pyc for a fixed source across a yield, or a crash.

File/compile-heavy: each fiber holds a private temp dir + writes + invokes the C
compiler, so max_funcs is capped (the forever-loop --funcs 1000000 would otherwise
spawn a million compilers).  Per-fiber resources are isolated (own tmpdir) and
removed in a finally.

Stresses: builtins.compile / the CPython C compiler (parser, constant folding,
optimizer, codegen), marshal of the code object, importlib .pyc header + cache
path, py_compile hash-invalidation, all under M:N parallel compilation of distinct
fiber-local sources across hubs.
"""
import compileall
import importlib.util
import marshal
import os
import shutil
import tempfile

import py_compile

import harness
import runloom

MASK32 = 0xFFFFFFFF

# Hash-based .pyc (UNCHECKED_HASH) embeds a sha256 of the SOURCE bytes in the
# header instead of the source mtime, so compiling a FIXED source twice produces a
# bit-identical .pyc regardless of filesystem timestamps -- which is exactly what
# makes the cross-yield determinism check load-bearing (any byte difference is a
# torn compile, not a benign mtime change).
INVMODE = py_compile.PycInvalidationMode.UNCHECKED_HASH

# .pyc header is 16 bytes (magic[4] + bitfield[4] + hash-or-mtime+size[8]); the
# marshalled code object follows.
PYC_HEADER = 16


def reference(mul, a, b):
    """Recompute the module's RESULT closed form in straight Python -- an
    INDEPENDENT computation (never by exec'ing the compiled module).  MUST stay in
    exact lockstep with make_source() below."""
    def f(n):
        t = 0
        for i in range(n):
            t = (t * mul + i) & MASK32
        return t
    c = [(x * x) & 0xFFFF for x in range(a % 17)]
    return (f(b % 23) ^ a ^ (sum(c) & MASK32)) & MASK32


def make_source(mul, a, b):
    """Generate the fiber-local module source.  Its RESULT is the closed form that
    reference() recomputes; the body uses a function with a loop, a list
    comprehension, and constant folding so the C compiler does real codegen work.
    Kept in exact lockstep with reference()."""
    return (
        "def f(n):\n"
        "    t = 0\n"
        "    for i in range(n):\n"
        "        t = (t * {mul} + i) & 0xFFFFFFFF\n"
        "    return t\n"
        "A = {a}\n"
        "B = {b}\n"
        "C = [(x * x) & 0xFFFF for x in range(A % 17)]\n"
        "RESULT = (f(B % 23) ^ A ^ (sum(C) & 0xFFFFFFFF)) & 0xFFFFFFFF\n"
    ).format(mul=mul, a=a, b=b)


def compile_and_load(src_path):
    """compileall.compile_file the single-owner source, then read back the .pyc,
    marshal.loads the code object, and return (pyc_bytes, code).  Raises on a
    compile failure so the worker turns it into H.fail."""
    ok = compileall.compile_file(
        src_path, force=True, quiet=1, invalidation_mode=INVMODE)
    if not ok:
        return None, None
    pyc_path = importlib.util.cache_from_source(src_path)
    with open(pyc_path, "rb") as fh:
        data = fh.read()
    code = marshal.loads(data[PYC_HEADER:])
    return data, code


# Sustained checks per worker: the torn-compile hazard only manifests under
# SUSTAINED parallel compilation (many hubs simultaneously inside the C compiler
# while this fiber is sleep-PARKED across its yield), so the scheduler reliably
# interleaves a sibling's compile before this fiber resumes and re-compiles.
INNER_CAP = 100000

# File/compiler-heavy: each fiber holds a private temp dir + drives the full
# compiler.  Cap the goroutine count so the forever-loop --funcs 1000000 doesn't
# spawn a million concurrent compilers (each a real fork of the codegen path).
MAX_FUNCS = 512


def check_once(H, wid, idx, src_path, state):
    """One single-owner PURITY + determinism check on a fiber-local source."""
    # Fiber-local constants -> a distinct source per (wid, idx).  Odd multiplier so
    # the folded/loop arithmetic is non-trivial; bounded so exec is fast.
    mul = (wid * 2 + 3) & MASK32 | 1
    a = (wid * 1000003 + idx * 97 + 1) & MASK32
    b = (wid * 131 + idx * 7 + 5) & MASK32
    expected = reference(mul, a, b)

    src = make_source(mul, a, b)
    with open(src_path, "w") as fh:
        fh.write(src)

    data1, code1 = compile_and_load(src_path)
    if data1 is None:
        H.fail("compileall.compile_file returned False on VALID fiber-local source "
               "(wid {0} idx {1}) -- the C compiler spuriously failed to compile a "
               "well-formed module under M:N".format(wid, idx))
        return
    ns1 = {}
    exec(code1, ns1)
    got1 = ns1.get("RESULT")
    if got1 != expected:
        H.fail("bytecode PURITY broken (pre-yield): compiled module RESULT={0} but "
               "the independently-recomputed closed form is {1} (wid {2} idx {3}) "
               "-- the C compiler produced WRONG bytecode, a codegen corruption "
               "under concurrent compilation".format(got1, expected, wid, idx))
        return

    # YIELD: let sibling fibers run the C compiler on their own sources while this
    # fiber is parked, then recompile the SAME source and assert determinism.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    data2, code2 = compile_and_load(src_path)
    if data2 is None:
        H.fail("compileall.compile_file returned False on the SAME valid source on "
               "recompile (wid {0} idx {1}) -- spurious compiler failure under "
               "M:N".format(wid, idx))
        return
    if data2 != data1:
        H.fail("compilation NON-DETERMINISTIC across a yield: recompiling the SAME "
               "fiber-local source (hash-invalidation .pyc) produced different .pyc "
               "bytes ({0} vs {1} bytes; first differing at offset {2}) for wid {3} "
               "idx {4} -- a torn compile: a sibling's concurrent compilation "
               "corrupted this fiber's codegen/marshal output".format(
                   len(data1), len(data2), _first_diff(data1, data2), wid, idx))
        return
    ns2 = {}
    exec(code2, ns2)
    got2 = ns2.get("RESULT")
    if got2 != expected:
        H.fail("bytecode PURITY broken (post-yield): recompiled module RESULT={0} "
               "!= closed form {1} (wid {2} idx {3}) -- codegen corruption on "
               "recompile under concurrent compilation".format(
                   got2, expected, wid, idx))
        return

    state["compile_checks"][wid] += 1


def _first_diff(a, b):
    """Offset of the first byte where a and b differ (or the shorter length)."""
    n = min(len(a), len(b))
    for i in range(n):
        if a[i] != b[i]:
            return i
    return n


def worker(H, wid, rng, state):
    """Each fiber owns a private temp dir + source file for its whole lifetime and
    runs the load-bearing PURITY + determinism check across a yield, repeatedly,
    while siblings compile their own sources in parallel on other hubs."""
    tmpd = tempfile.mkdtemp(prefix="big100_p565_w{0}_".format(wid))
    src_path = os.path.join(tmpd, "fibermod_{0}.py".format(wid))
    try:
        for _ in H.round_range():
            if not H.running():
                break
            idx = 0
            while H.running() and idx < INNER_CAP:
                check_once(H, wid, idx, src_path, state)
                if H.failed:
                    return
                H.op(wid)
                idx += 1
            H.task_done(wid)
    finally:
        shutil.rmtree(tmpd, ignore_errors=True)


def setup(H):
    # RACE-FREE conservation counter: one slot per worker (single writer per slot),
    # allocated here where H.funcs is known.
    H.state = {
        "compile_checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["compile_checks"])
    H.log("compileall single-owner PURITY+determinism checks: {0} (each compiled a "
          "distinct fiber-local module, verified executed RESULT == closed form and "
          "bit-identical recompile across a yield); ops={1}".format(
              checks, H.total_ops()))

    # NON-VACUITY: the load-bearing compile hazard was actually exercised.
    H.check(checks > 0,
            "no compileall PURITY checks ran -- the load-bearing concurrent-"
            "compilation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the C
    # compiler, the .pyc write, or marshal).
    H.require_no_lost("compileall bytecode purity")


if __name__ == "__main__":
    harness.main(
        "p565_compileall_bytecode_purity", body, setup=setup, post=post,
        default_funcs=3000, max_funcs=MAX_FUNCS,
        describe="many hubs each compile a DISTINCT fiber-local source module via "
                 "compileall.compile_file in parallel; single-owner PURITY law: the "
                 "executed module RESULT must equal an independently-recomputed "
                 "closed form, and recompiling the same source (hash-invalidation "
                 ".pyc) must be bit-identical across a yield -- a wrong RESULT, a "
                 "non-deterministic recompile, a spurious compile failure, or a "
                 "crash is a torn-compile runtime/compiler bug")
