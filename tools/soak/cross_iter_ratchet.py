#!/usr/bin/env python3
"""Cross-iteration ratchet-leak backstop for the forever.sh soak loop.

The per-run slope oracle (oracle.py) forgives a retain-forever pool
(g_structs_total, coro_stack_live, ...) whose CURRENT rate has settled -- it must,
or a converged pool false-FAILs every iteration.  But that forgiveness has one
single-window blind spot: a *slow constant* leak whose movement over one 6h window
stays under the metric's abs_floor is indistinguishable from slow pool-creep
WITHIN that window (they have overlapping tail slopes).

Across ITERATIONS the two separate cleanly: a retain-forever pool has a FIXED
asymptote (the max-concurrency high-water-mark), so once filled its per-iteration
END value plateaus and stays flat forever; a leak has NO asymptote, so its END
value climbs iteration-over-iteration without bound (a +200/h leak that slips the
single-window floor still adds ~+1200 to the END every 6h iteration).

This tool is called once per iteration by forever.sh.  It appends each RATCHET
metric's END estimate (the median of the final eighth of each worker CSV -- a
noise-robust plateau reading) to a per-workload ledger, then judges the ledger:

  * PLATEAU  -- the recent per-iteration END values are flat (a settled pool): OK.
  * FILLING  -- still climbing but DECELERATING across iterations (a pool that
                needs several iterations to reach its HWM): OK, not yet a verdict.
  * LEAK     -- climbing across the recent window, NOT decelerating, by more than
                the metric's floor per iteration: a real cross-iteration leak the
                per-run oracle cannot see.  Emitted as CROSS-ITER-LEAK.

A leak cannot masquerade as PLATEAU (it never stops climbing) nor as FILLING (its
cross-iteration slope does not decelerate) -- the asymptote is the discriminator.

ROBUSTNESS (adversarially verified): the difference-of-block-means trend estimate
(judge_metric) holds against any above-floor leak whose per-iteration END jitter is
below ~12x the metric floor -- below that, at least one sliding-window phase fires
LEAK every cycle, so forever.sh's per-iteration ledger check flags it.  Real HWM
jitter is ~+-50 for g_structs_total (~0.16x the 320 floor), a ~75x margin.  Two
accepted limits remain, neither a realistic failure mode: (1) a leak at a rate <=
the floor (~53/hour) reads PLATEAU forever -- but that is below the per-run oracle's
harmless-drift epsilon everywhere, accepted by design; (2) a leak carrying jitter of
amplitude >~12x the floor at a period chosen against the block size (e.g. a period-3
deep sawtooth vs the 4-iter block that pins slope_late ~= 0.5*slope_early to fake
FILLING) can evade -- but that needs ~75x the observed jitter, not physical.
"""
import csv
import os
import sys

# Reuse the oracle's RATCHET set + per-metric floors so the two stay in lockstep.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import oracle  # noqa: E402

MIN_ITERS = 9          # >= 3 blocks before FILLING vs LEAK is decidable
RECENT = 12            # judge the trend over the most recent RECENT iterations
DECEL_FRAC = 0.5       # FILLING iff the late-block slope has halved vs the early
LEDGER_HEADER = ["iter", "worker", "metric", "end"]


def _final_eighth_median(values):
    """Noise-robust plateau reading: the median of the final eighth of a series."""
    if not values:
        return None
    q = max(4, len(values) // 8)
    tail = sorted(values[-q:])
    m = len(tail)
    return tail[m // 2] if m % 2 else 0.5 * (tail[m // 2 - 1] + tail[m // 2])


def read_end_values(outdir):
    """Return {(worker, metric): end_estimate} for every RATCHET metric present in
    each worker*.csv under outdir."""
    ends = {}
    for name in sorted(os.listdir(outdir)):
        if not (name.startswith("worker") and name.endswith(".csv")):
            continue
        worker = name[:-4]
        path = os.path.join(outdir, name)
        try:
            rows = list(csv.DictReader(open(path)))
        except OSError:
            continue
        if not rows:
            continue
        for metric in oracle.RATCHETS:
            if metric not in rows[0]:
                continue
            vals = []
            for r in rows:
                try:
                    vals.append(float(r[metric]))
                except (KeyError, ValueError, TypeError):
                    vals.append(float("nan"))
            vals = [v for v in vals if v == v]      # drop NaN
            e = _final_eighth_median(vals)
            if e is not None:
                ends[(worker, metric)] = e
    return ends


def append_ledger(ledger_path, iteration, ends):
    fresh = not os.path.exists(ledger_path)
    with open(ledger_path, "a", newline="") as f:
        w = csv.writer(f)
        if fresh:
            w.writerow(LEDGER_HEADER)
        for (worker, metric), end in sorted(ends.items()):
            w.writerow([iteration, worker, metric, "%.3f" % end])


def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def judge_metric(iters_ends, floor):
    """iters_ends: list of (iter, end_mean_across_workers), ascending by iter.
    `floor` is the metric's per-iteration climb tolerance (its abs_floor: a
    settled pool's cross-iteration END noise stays within it).  Returns
    (state, detail), state in {'SHORT','PLATEAU','FILLING','LEAK'}.

    A retain-forever pool has a FIXED asymptote, so once filled its per-iteration
    END is flat (PLATEAU); while filling it climbs but DECELERATES (FILLING).  A
    leak has no asymptote: it climbs across the recent window and does NOT
    decelerate (LEAK).

    The trend is estimated by DIFFERENCE-OF-BLOCK-MEANS, not an OLS slope + CI.
    An OLS slope's confidence interval is inflated by run-to-run HWM jitter (the
    END reading is a per-soak sample that legitimately varies): a leak buried in
    jitter of amplitude >~3x the per-iter leak keeps the CI permanently straddling
    0, so the slope test reads 'flat' forever no matter how long the leak runs.
    Averaging each block first CANCELS symmetric zero-mean jitter (an alternating
    +/-A oscillation sums to ~0 over a block), recovering the underlying trend --
    so a leak's inter-block climb survives the jitter that hid it from OLS."""
    if len(iters_ends) < MIN_ITERS:
        return "SHORT", "%d iters (<%d)" % (len(iters_ends), MIN_ITERS)
    ys = [e for _, e in iters_ends[-RECENT:]]
    b = len(ys) // 3
    ys = ys[-3 * b:]                         # three EQUAL blocks (drop remainder)
    m_early = _mean(ys[:b])
    m_mid = _mean(ys[b:2 * b])
    m_late = _mean(ys[2 * b:])
    slope_early = (m_mid - m_early) / b      # block centers are b iters apart
    slope_late = (m_late - m_mid) / b
    overall = (m_late - m_early) / (2 * b)
    if abs(overall) <= floor:
        return "PLATEAU", "block-mean slope %.0f/iter within floor %.0f" % (overall, floor)
    # Climbing beyond the floor -> pool-fill (decelerating) or leak (linear)?
    if slope_late <= DECEL_FRAC * slope_early and slope_late <= 2.0 * floor:
        return "FILLING", "decelerating (early %.0f -> late %.0f /iter)" % (
            slope_early, slope_late)
    return "LEAK", "sustained +%.0f/iter (late %.0f, floor %.0f) -- not decelerating" % (
        overall, slope_late, floor)


def check(ledger_path):
    """Read the ledger and judge every RATCHET metric.  Returns
    (overall_ok, [(metric, state, detail), ...])."""
    if not os.path.exists(ledger_path):
        return True, []
    rows = list(csv.DictReader(open(ledger_path)))
    # per (metric, iter): mean END across workers
    agg = {}
    for r in rows:
        try:
            it = int(r["iter"]); end = float(r["end"])
        except (KeyError, ValueError):
            continue
        agg.setdefault(r["metric"], {}).setdefault(it, []).append(end)
    results = []
    ok = True
    for metric in sorted(agg):
        series = sorted((it, sum(v) / len(v)) for it, v in agg[metric].items())
        floor = oracle.ABSOLUTE_FLOOR.get(metric, oracle._DEFAULT_ABS_FLOOR)
        state, detail = judge_metric(series, floor)
        results.append((metric, state, detail))
        if state == "LEAK":
            ok = False
    return ok, results


def main(argv):
    if len(argv) >= 3 and argv[1] == "--check":
        ok, results = check(argv[2])
        for metric, state, detail in results:
            print("%-18s %-8s %s" % (metric, state, detail))
        if not ok:
            print("CROSS-ITER-LEAK: a retain-forever pool is climbing across "
                  "iterations without an asymptote (see LEAK rows)")
        return 0 if ok else 1
    if len(argv) < 4:
        sys.stderr.write("usage: cross_iter_ratchet.py <outdir> <ledger> <iter>\n"
                         "       cross_iter_ratchet.py --check <ledger>\n")
        return 2
    outdir, ledger, iteration = argv[1], argv[2], int(argv[3])
    if os.path.isdir(outdir):
        append_ledger(ledger, iteration, read_end_values(outdir))
        ok, results = check(ledger)
        if not ok:
            sys.stderr.write("CROSS-ITER-LEAK detected:\n")
            for metric, state, detail in results:
                if state == "LEAK":
                    sys.stderr.write("  %s %s\n" % (metric, detail))
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
