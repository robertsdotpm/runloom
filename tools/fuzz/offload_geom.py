#!/usr/bin/env python3
"""offload_geom.py -- blocking-offload / blockpool GEOMETRY fuzzer.

The single most-cited OPEN runloom bug is the offload wedge: at high concurrent-
offload counts (~100k on Linux p23/p17, lower on mac p92) a goroutine parked in
`runloom.blocking()` is never re-queued -- a lost wake on the foreign-waker
(offload-thread -> parked-caller) path.  The existing fuzzers (mn_stress,
lifefuzz, p96/p97/p227) hammer chan/cldeque/select and a FIXED offload workload;
none SWEEP the offload geometry looking for the wedge boundary.

This does: each seed draws a random geometry -- (hubs, concurrent offloads, pool
worker count, per-job duration profile, submit burstiness, job kind) -- runs a
runloom M:N program that fires exactly that many `runloom.blocking()` calls each
returning a unique token, and checks TOKEN CONSERVATION (every offload result
received exactly once).  A lost wake shows as a wedge (no progress) or a
conservation shortfall, reported WITH the exact reproducing geometry+seed.

SAFETY (this bug can hard-wedge the box -- M:N hubs busy-spin):
  * every run is a SUBPROCESS with a hard wall-clock --timeout; a wedged child is
    killed, the parent survives.  The child ALSO self-arms a real-OS-thread
    watchdog (os._exit(3) on stall).
  * --offloads is capped at SAFE_MAX (default 5000) unless --allow-large; going
    to the real ~100k wedge needs --allow-large AND should be pinned (`taskset
    -c 0-7`) on a box you can afford to lose.

Usage:
    tools/fuzz/offload_geom.py --seeds 40                    # safe sweep
    tools/fuzz/offload_geom.py --seeds 200 --offloads 5000   # heavier, still safe
    tools/fuzz/offload_geom.py --escalate --allow-large      # hunt the wedge boundary (DANGER)
    tools/fuzz/offload_geom.py --selftest                    # prove wedge detection w/o the bug

Exit: 0 = all geometries conserved; 1 = a WEDGE or conservation MISMATCH found
(repro printed); 2 = runner error.
"""
import argparse
import os
import subprocess
import sys

SAFE_MAX = 5000
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
PYBIN = os.environ.get("RUNLOOM_PYTHON",
                       os.path.expanduser("~/.pyenv/versions/3.14.4t/bin/python3"))


# ---------------------------------------------------------------------------
# CHILD: runs ONE geometry under runloom M:N, checks token conservation.
# Re-exec of this file with --child; geometry comes in via OG_* env vars.
# ---------------------------------------------------------------------------
def child_main():
    import time as _time
    REAL_SLEEP = _time.sleep
    import _thread
    G = {k: int(os.environ.get("OG_" + k, "0")) for k in
         ("HUBS", "OFFLOADS", "POOL", "JOBMS_MAX", "BURST", "SEED")}
    jobkind = os.environ.get("OG_JOBKIND", "sleep")
    selftest_wedge = os.environ.get("OG_SELFTEST_WEDGE") == "1"

    import random
    import hashlib
    import runloom

    n = G["OFFLOADS"]
    received = [0] * n               # one writer per slot -> race-free
    progress = [0]
    done = [False]

    # child-side watchdog on a REAL OS thread: a wedge self-kills as exit 3.
    def watchdog():
        last, stalls = 0, 0
        budget = max(10, n // 200)   # seconds of allowed no-progress
        while not done[0]:
            REAL_SLEEP(1.0)
            cur = progress[0]
            stalls = stalls + 1 if cur == last else 0
            last = cur
            if stalls >= budget:
                sys.stderr.write("CHILD-WEDGE stall at progress={0}/{1}\n".format(cur, n))
                sys.stderr.flush()
                os._exit(3)
    _thread.start_new_thread(watchdog, ())

    def job(tok, ms):
        # a GIL-releasing real-blocking body so it genuinely uses an offload worker
        if jobkind == "cpu":
            h = hashlib.sha256()
            for _ in range(50 + (ms * 20)):
                h.update(b"x" * 64)
            return tok
        REAL_SLEEP(ms / 1000.0)
        return tok

    def main():
        wg = runloom.WaitGroup()
        wg.add(n)
        rng = random.Random(G["SEED"])
        durs = [0 if G["JOBMS_MAX"] == 0 else rng.randint(0, G["JOBMS_MAX"]) for _ in range(n)]

        def one(i):
            try:
                if selftest_wedge and i == 0:
                    # deliberately never-returning offload to prove wedge detection
                    runloom.blocking(REAL_SLEEP, 10_000)
                r = runloom.blocking(job, i, durs[i])
                if r == i:
                    received[i] += 1
                progress[0] += 1
            finally:
                wg.done()

        if G["BURST"]:
            for i in range(n):
                runloom.fiber(one, i)
        else:
            for i in range(n):
                runloom.fiber(one, i)
                if (i & 1023) == 0:
                    runloom.sleep(0)        # stagger submission
        wg.wait()

    runloom.run(max(2, G["HUBS"]), main)
    done[0] = True
    got = sum(received)
    if got == n:
        print("OFFLOAD_GEOM_OK received={0}/{1}".format(got, n))
        sys.exit(0)
    print("OFFLOAD_GEOM_MISMATCH received={0}/{1} (lost {2})".format(got, n, n - got))
    sys.exit(1)


# ---------------------------------------------------------------------------
# PARENT: sweep seeds -> geometries, run each child isolated + timed.
# ---------------------------------------------------------------------------
def draw_geometry(seed, offloads_cap, escalate, idx, total):
    import random
    rng = random.Random(seed)
    hubs = rng.choice([2, 4, 8, 16])
    pool = rng.choice([1, 2, 4, 8, 16])
    if escalate:
        # ramp offloads with sweep position toward the cap (hunt the boundary)
        base = int(offloads_cap * (idx + 1) / max(1, total))
        offloads = max(100, base)
    else:
        offloads = rng.randint(200, offloads_cap)
    return {
        "HUBS": hubs, "POOL": pool, "OFFLOADS": offloads,
        "JOBMS_MAX": rng.choice([0, 1, 3, 10]),
        "BURST": rng.choice([0, 1]),
        "JOBKIND": rng.choice(["sleep", "cpu"]),
        "SEED": seed,
    }


def run_child(geom, timeout, selftest_wedge=False):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src") + os.pathsep + env.get("PYTHONPATH", "")
    env["RUNLOOM_BLOCKPOOL_WORKERS"] = str(geom["POOL"])
    env["RUNLOOM_SYSMON_QUIET"] = "1"
    for k in ("HUBS", "OFFLOADS", "POOL", "JOBMS_MAX", "BURST", "SEED"):
        env["OG_" + k] = str(geom[k])
    env["OG_JOBKIND"] = geom["JOBKIND"]
    if selftest_wedge:
        env["OG_SELFTEST_WEDGE"] = "1"
    cmd = [PYBIN, os.path.abspath(__file__), "--child"]
    try:
        p = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True,
                           text=True, timeout=timeout)
        out = (p.stdout or "") + (p.stderr or "")
        if p.returncode == 0:
            return "OK", out.strip().splitlines()[-1] if out.strip() else ""
        if p.returncode == 1:
            return "MISMATCH", out.strip().splitlines()[-1] if out.strip() else ""
        if p.returncode == 3 or p.returncode < 0:
            return "WEDGE", "rc={0}".format(p.returncode)
        return "ERROR", "rc={0} {1}".format(p.returncode, out[-160:])
    except subprocess.TimeoutExpired:
        return "WEDGE", "hard-timeout {0}s".format(timeout)


def repro(geom):
    return ("OG_HUBS={HUBS} OG_OFFLOADS={OFFLOADS} OG_POOL={POOL} "
            "OG_JOBMS_MAX={JOBMS_MAX} OG_BURST={BURST} OG_JOBKIND={JOBKIND} "
            "OG_SEED={SEED} RUNLOOM_BLOCKPOOL_WORKERS={POOL} "
            "PYTHON_GIL=0 PYTHONPATH=src {py} tools/fuzz/offload_geom.py --child"
            ).format(py=PYBIN, **geom)


def main(argv):
    if "--child" in argv:
        return child_main()

    ap = argparse.ArgumentParser(description="offload-geometry fuzzer")
    ap.add_argument("--seeds", type=int, default=30)
    ap.add_argument("--base-seed", type=int, default=1000)
    ap.add_argument("--offloads", type=int, default=2000, help="max offloads per geometry")
    ap.add_argument("--timeout", type=int, default=60, help="per-child wall-clock cap (s)")
    ap.add_argument("--escalate", action="store_true",
                    help="ramp offload count across the sweep to hunt the wedge boundary")
    ap.add_argument("--allow-large", action="store_true",
                    help="permit --offloads above SAFE_MAX (DANGER: can wedge the box)")
    ap.add_argument("--selftest", action="store_true",
                    help="run one deliberately-wedging geometry to prove detection")
    args = ap.parse_args(argv)

    if args.selftest:
        geom = {"HUBS": 4, "POOL": 2, "OFFLOADS": 50, "JOBMS_MAX": 1,
                "BURST": 1, "JOBKIND": "sleep", "SEED": 1}
        print("selftest: a deliberately-wedging offload must be detected as WEDGE...")
        st, det = run_child(geom, timeout=15, selftest_wedge=True)
        ok = st == "WEDGE"
        print("  -> {0} ({1})  [{2}]".format(st, det, "PASS" if ok else "FAIL: detection broken"))
        return 0 if ok else 2

    cap = args.offloads
    if cap > SAFE_MAX and not args.allow_large:
        print("offload_geom: --offloads {0} exceeds SAFE_MAX {1}; pass --allow-large "
              "(and pin with taskset) to hunt the real ~100k wedge.".format(cap, SAFE_MAX))
        return 2

    print("offload_geom: {0} seeds, offloads<= {1}, timeout {2}s{3}".format(
        args.seeds, cap, args.timeout, ", ESCALATE" if args.escalate else ""))
    wedges = []
    mismatches = []
    for i in range(args.seeds):
        seed = args.base_seed + i
        geom = draw_geometry(seed, cap, args.escalate, i, args.seeds)
        st, det = run_child(geom, args.timeout)
        tag = "{0}off/{1}hub/{2}pool/{3}".format(
            geom["OFFLOADS"], geom["HUBS"], geom["POOL"], geom["JOBKIND"])
        print("  seed={0:<6} {1:<28} -> {2:<9} {3}".format(seed, tag, st, det))
        if st == "WEDGE":
            wedges.append(geom)
        elif st == "MISMATCH":
            mismatches.append(geom)

    print("\noffload_geom: {0} wedges, {1} mismatches over {2} geometries".format(
        len(wedges), len(mismatches), args.seeds))
    for g in wedges + mismatches:
        print("  REPRO: " + repro(g))
    return 1 if (wedges or mismatches) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
