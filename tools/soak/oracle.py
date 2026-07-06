"""Slope oracle (docs/dev/RELIABILITY_PROGRAM.md R1).

The core of the soak methodology: FAIL ON A SLOPE, NOT A CRASH.  A leak of 100
bytes/connection never crashes a 48-hour run, but it shows up as a visible
upward trend line within the first hour.  So after a warmup (which absorbs
one-time setup: lazy imports, the offload pool, cache priming, reaching peak
concurrency), we fit a least-squares line to each metric over time and PASS iff
the slope is statistically indistinguishable from flat -- its 95% confidence
interval includes 0 -- OR its magnitude is below a per-metric epsilon (some
metrics have a tiny, harmless real slope: e.g. RSS creeping < 1 MB/h is
allocator noise, not a leak).

Pure Python, no numpy: ordinary least squares + the standard error of the
slope give the CI (t ~ 1.96 for the sample sizes a soak produces).  Reads a CSV
written by worker.py; can be re-run standalone on an archived CSV.
"""
import csv as _csv
import math


# Per-metric slope epsilon (units per HOUR).  A slope whose magnitude is below
# this is treated as flat even if its CI excludes 0 (a real but harmless
# drift).  Anything not listed uses 0 -> the CI test alone decides.
#   rss_kb / vsz_kb: KB/hour.  1 MB/h = 1024.
EPSILON_PER_HOUR = {
    "rss_kb": 4096.0,     # < 4 MB/h RSS drift is allocator/pool warmup noise
    "vsz_kb": 8192.0,     # address space is even noisier (mmap arenas)
    "vmas": 8.0,          # a few VMAs of pool growth
    "coro_depot_pooled": 8.0,
    # g structs: freed structs are RETAINED by design (never returned to the OS)
    # in per-thread slabs (RUNLOOM_G_SLAB_CAP each) + a global pool.  Under M:N
    # the gauge ratchets up to ~slab-cap during warmup and then REBALANCES in
    # small cross-hub batches at equilibrium (measured: plateau at ~4.6K, then
    # occasional +-50 steps -- soak_cserve_echo plateau run, 2026-07-05).  A real
    # per-connection leak accrues THOUSANDS per hour (the cserve smoke's genuine
    # pre-plateau signal was +327K/h), so 64/h keeps >3 orders of magnitude of
    # teeth while not flagging slab-batch noise.
    "g_structs_total": 64.0,
    "stack_hwm": 1e18,    # a high-water max, not a population -- never a leak
    # parked_max_age (item 5 dwell gauge, seconds): the max age among parked
    # fibers.  In a healthy churny workload parked fibers turn over fast so this
    # stays low + flat; a STRANDED fiber's age climbs at ~3600 s/h (1 s/s), so a
    # 120 s/h epsilon forgives normal drift (a genuinely long-lived acceptor park)
    # while a real strand blows past it by >an order of magnitude.
    "parked_max_age": 120.0,
    # hard_deadlock is a 0/1 alarm; a sustained deadlock shows as a positive slope
    # and fails, a one-sample snapshot artifact (all fibers momentarily parked
    # between rounds) is forgiven by the CI -- which is the correct bias.
    "hard_deadlock": 0.5,
    # cumulative odometers: excluded entirely (see ODOMETERS)
}

# Absolute-change floor: a metric is also flat if the line's TOTAL predicted
# change across the fitted window (|slope * span|) is below this floor.  This
# is what makes the oracle robust across timescales: on a SHORT window, pool
# settling produces a small absolute change that extrapolates to a scary
# per-hour slope -- the floor absorbs it.  On a real multi-hour soak a genuine
# leak's total change dwarfs the floor, so it is still caught.  Units are the
# metric's own (KB for rss/vsz, counts otherwise).
ABSOLUTE_FLOOR = {
    "rss_kb": 8192.0,     # < 8 MB total movement over the window = settling
    "vsz_kb": 16384.0,
    # vmas / coro_stack_live: coro stacks are RETAINED in per-thread pools
    # (RUNLOOM_CORO_POOL_CAP=512 each) and each stack is 2 VMAs (map + guard).
    # Under M:N the pools fill in a decelerating staircase (measured steps up to
    # ~65 early, ~10-40 at equilibrium -- soak_cserve_echo plateau run,
    # 2026-07-05); VM-only cost, RSS stays flat (lazy paging + madvise).  A real
    # per-connection stack leak accrues thousands per hour, so these floors keep
    # orders of magnitude of teeth while absorbing pool-fill steps.
    "vmas": 224.0,
    "coro_stack_live": 96.0,
    "coro_depot_pooled": 16.0,
    "g_structs_total": 320.0,   # > the observed +-50 cross-hub slab-batch steps
}
_DEFAULT_ABS_FLOOR = 6.0   # a handful of objects of jitter on a count gauge

# Pool-ratchet gauges: populations that are RETAINED by design (freed g structs
# / coro stacks cached in per-thread slabs) and fill in a DECELERATING staircase
# whose tail can outlast any reasonable warmup -- the first 6h cserve_echo
# iteration measured per-hour g_struct deltas of 7017/2489/814/323/0/0 (a hard
# plateau at 10.7K after ~4h; fds + fibers flat throughout).  For these gauges
# a whole-window slope failure is forgiven ONLY when the FINAL QUARTER of the
# window is flat (max-min <= the gauge's absolute floor): a converging ratchet
# ends flat; a real leak stays linear to the end and still FAILs.
RATCHETS = {"g_structs_total", "coro_stack_live", "coro_depot_pooled",
            "py_parker_free", "vmas"}

# Cumulative counters that only ever rise by design -- excluded from the slope
# test (a soak WANTS these climbing; they confirm work is happening).
ODOMETERS = {
    "progress", "mn_completed_total", "stale_arm_heals", "completed",
    "stack_completed",
}

# Constant/context columns with no leak meaning.
IGNORE = {
    "t", "running", "stack_calibrated", "stack_painting", "ready_capacity",
    "stack_size_default", "hubs_live", "py_offload_workers", "threads",
}

T_95 = 1.96   # normal approx; soak sample counts (>= ~10 post-warmup) suffice


def _ols(xs, ys):
    """Ordinary least squares.  Returns (slope, slope_stderr, n) or
    (0,0,n) when it cannot be fit (n<3 or zero x-variance)."""
    n = len(xs)
    if n < 3:
        return 0.0, 0.0, n
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return 0.0, 0.0, n
    sxy = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    slope = sxy / sxx
    intercept = my - slope * mx
    resid = [ys[i] - (intercept + slope * xs[i]) for i in range(n)]
    dof = n - 2
    if dof <= 0:
        return slope, 0.0, n
    s2 = sum(r * r for r in resid) / dof
    stderr = math.sqrt(s2 / sxx) if s2 > 0 else 0.0
    return slope, stderr, n


def analyze(csv_path, warmup_seconds):
    """Return (verdict, rows) where verdict is 'PASS'/'FAIL' and rows is a list
    of dicts (one per metric) with slope-per-hour, CI, epsilon, ok."""
    with open(csv_path) as f:
        reader = _csv.DictReader(f)
        cols = reader.fieldnames or []
        data = list(reader)
    if not data:
        return "FAIL", [{"metric": "(no data)", "note": "empty CSV"}]

    ts = []
    series = {c: [] for c in cols if c != "t"}
    for r in data:
        try:
            t = float(r["t"])
        except (KeyError, ValueError):
            continue
        ts.append(t)
        for c in series:
            try:
                series[c].append(float(r[c]))
            except (KeyError, ValueError, TypeError):
                series[c].append(float("nan"))

    # post-warmup window
    keep = [i for i, t in enumerate(ts) if t >= warmup_seconds]
    if len(keep) < 3:
        # not enough post-warmup samples: relax to all samples but flag it
        keep = list(range(len(ts)))
        short = True
    else:
        short = False
    xs = [ts[i] for i in keep]

    rows = []
    overall_ok = True
    for metric in sorted(series):
        if metric in IGNORE or metric in ODOMETERS:
            continue
        ys = [series[metric][i] for i in keep]
        if any(math.isnan(y) for y in ys):
            continue
        slope, stderr, n = _ols(xs, ys)          # per SECOND
        slope_h = slope * 3600.0                  # per HOUR
        ci_half_h = T_95 * stderr * 3600.0
        lo, hi = slope_h - ci_half_h, slope_h + ci_half_h
        eps = EPSILON_PER_HOUR.get(metric, 0.0)
        span = (xs[-1] - xs[0]) if len(xs) > 1 else 0.0
        pred_change = abs(slope * span)           # total fitted change over window
        abs_floor = ABSOLUTE_FLOOR.get(metric, _DEFAULT_ABS_FLOOR)
        ci_includes_zero = (lo <= 0.0 <= hi)
        below_eps = abs(slope_h) <= eps
        below_floor = pred_change <= abs_floor
        ok = ci_includes_zero or below_eps or below_floor
        ratchet_converged = False
        if not ok and metric in RATCHETS and len(ys) >= 16:
            tail = ys[-max(8, len(ys) // 4):]      # final quarter of the window
            if (max(tail) - min(tail)) <= abs_floor:
                ok = True
                ratchet_converged = True
        if not ok:
            overall_ok = False
        rows.append({
            "metric": metric, "slope_per_h": slope_h,
            "ci_lo": lo, "ci_hi": hi, "eps": eps, "n": n,
            "pred_change": pred_change, "abs_floor": abs_floor,
            "ok": ok,
            "reason": ("ci~0" if ci_includes_zero else
                       ("<eps" if below_eps else
                        ("<floor" if below_floor else
                         ("ratchet-converged" if ratchet_converged else "SLOPE")))),
        })
    verdict = "PASS" if overall_ok else "FAIL"
    if short:
        verdict += " (short: warmup left <3 samples; used all)"
    return verdict, rows


def format_report(csv_path, verdict, rows):
    """Render a compact markdown table of the slope analysis."""
    out = []
    out.append("| metric | slope/h | 95% CI | eps/h | n | ok |")
    out.append("|---|---:|---|---:|---:|:-:|")
    for r in rows:
        if "note" in r:
            out.append("| %s | | %s | | | |" % (r["metric"], r["note"]))
            continue
        mark = "✅" if r["ok"] else "❌ " + r["reason"]
        out.append("| `%s` | %+.2f | [%+.1f, %+.1f] | %.0f | %d | %s |" % (
            r["metric"], r["slope_per_h"], r["ci_lo"], r["ci_hi"],
            r["eps"], r["n"], mark))
    return "\n".join(out)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Run the slope oracle on a soak CSV.")
    ap.add_argument("csv")
    ap.add_argument("--warmup", type=float, default=600.0,
                    help="warmup seconds to drop before fitting (default 600)")
    a = ap.parse_args()
    verdict, rows = analyze(a.csv, a.warmup)
    print(format_report(a.csv, verdict, rows))
    print("\nVERDICT:", verdict)
    raise SystemExit(0 if verdict.startswith("PASS") else 1)
