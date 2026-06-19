#!/usr/bin/env python3
"""Generate the README benchmark sections (a curated SUBSET of the full data)
with footnotes linking back to benchmark/report.html for the full tables,
methodology and source.  Writes benchmark/README_SECTIONS.md.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness"))
import config

RES = config.RESULTS_DIR
BENCH = config.BENCH_DIR
REPORT = "benchmark/report.html"


def load(name):
    for n in (name, name.replace(".json", "_quick.json")):
        p = os.path.join(RES, n)
        if os.path.exists(p):
            with open(p) as f:
                return json.load(f)
    return None


def commafy(x, nd=0):
    if x is None:
        return "n/a"
    return ("{:,.%df}" % nd).format(x)


def main():
    perf = load("perf.json")
    speed = load("speed.json")
    mem = load("mem.json")
    work = load("work_curve.json")
    envd = load("env.json") or {}
    quick = any(d and d.get("quick") for d in (perf, speed, mem))

    L = []
    L.append("## Benchmarks")
    L.append("")
    hw = "%s, %s vCPU, %s GiB" % (
        (envd.get("cpu_model") or "").replace("(R)", "").replace("(TM)", ""),
        envd.get("logical_cpus"), envd.get("mem_total_gib"))
    L.append("Measured on %s (free-threaded CPython 3.13t, GIL off) under a two-netns veth "
             "topology with disjoint CPU pinning.%s Full tables, per-connection curves, "
             "assumed constraints and every benchmark program are in the detailed "
             "report.[^bench]" % (hw, " **(smoke data &mdash; rerun for real numbers.)**" if quick else ""))
    L.append("")

    # --- req/s per core ---
    if perf:
        servers = perf.get("servers", {})
        rows = []
        for name, s in servers.items():
            pk = s.get("metrics", {}).get("reqps", {}).get("peak", {})
            bt = s.get("metrics", {}).get("reqps", {}).get("bottleneck_at_peak", "")
            if not pk or "rps_median" not in pk:
                continue
            cores = s.get("cores", 1)
            rows.append((s.get("label", name), cores, pk["rps_median"],
                         pk["rps_median"] / cores, bt))
        rows.sort(key=lambda r: -r[3])
        if rows:
            L.append("### Echo throughput (1 KiB requests)")
            L.append("")
            L.append("Requests/second, raw and normalised to a single core "
                     "(multi-core servers divided by their core count).[^bench]")
            L.append("")
            L.append("| Server | Cores | req/s | req/s per core | CPU-bound side |")
            L.append("|---|--:|--:|--:|---|")
            for label, cores, rps, per, bt in rows:
                L.append("| %s | %d | %s | %s | %s |"
                         % (label, cores, commafy(rps), commafy(per), bt))
            L.append("")
            L.append("> The 16-core Go loadgen saturates before the fastest servers "
                     "(`client`-bound rows); the report gives a server-ceiling estimate "
                     "from server CPU utilisation.[^bench]")
            L.append("")
            L.append("> **io_uring:** driven through the Stage-2 proactor (`loop_recv`), the "
                     "io_uring loop backend is a major win &mdash; the Cython handler on io_uring "
                     "reaches a **1.16M req/s server ceiling (+2.17× over epoll)**, the fastest "
                     "runloom config measured. \"io_uring loses on loopback\" was an artifact of "
                     "driving it through the readiness path; see the findings writeup.[^bench]")
            L.append("")

    # --- handler work curve ---
    if work:
        res = work.get("results", {})
        wmeta = work.get("meta", {})
        py, cy = res.get("py", {}), res.get("cython", {})
        works = wmeta.get("works", [])
        # a curated subset of work levels for the README (full curve in the report)
        show = [w for w in works if w in (0, 1, 4, 16, 64)] or works
        rows = []
        for w in show:
            ppy = py.get(str(w), {}).get("peak", {})
            pcy = cy.get(str(w), {}).get("peak", {})
            vpy, vcy = ppy.get("rps_median"), pcy.get("rps_median")
            if vpy is None or vcy is None:
                continue
            rows.append((w, vpy, vcy, (vcy / vpy) if vpy else 0))
        if rows:
            pl = wmeta.get("payload", 1024)
            mx = max(r[3] for r in rows)
            L.append("### Handler work curve (what compiling the handler buys)")
            L.append("")
            L.append("Echo ties every handler optimisation because it does no CPU work in the "
                     "handler. This is the one experiment that gives the handler something to do: "
                     "**one server, one knob** (`--work N` = an FNV-1a byte hash over the %d B "
                     "payload, repeated N times), **two builds of the identical algorithm** &mdash; "
                     "interpreted Python vs Cython-compiled &mdash; on the same runtime and I/O "
                     "path. `--work 0` **is** the echo (lowest point), so it consolidates the echo "
                     "load and reproduces it as a cross-check.[^bench]" % pl)
            L.append("")
            L.append("| --work (FNV passes) | Python handler req/s | Cython handler req/s | Cython / Python |")
            L.append("|--:|--:|--:|--:|")
            for w, vpy, vcy, spd in rows:
                tag = " (echo)" if w == 0 else ""
                L.append("| %d%s | %s | %s | %.2f× |"
                         % (w, tag, commafy(vpy), commafy(vcy), spd))
            L.append("")
            L.append("> As the knob grows the interpreted handler goes server-bound and collapses "
                     "while the compiled handler holds (up to **%.1f×** here). The work is pure "
                     "inline arithmetic, never offloaded to a worker thread, so per-core accounting "
                     "stays valid. **Honest framing:** if the handler delegated to a C library "
                     "(`hashlib`/`json`/`struct`) Python and Cython would converge &mdash; the gap "
                     "is specific to *handler-level* Python work.[^bench]" % mx)
            L.append("")

    # --- memory 1M ---
    if mem:
        cfgs = mem.get("configs", {})
        rows = []
        for name, c in cfgs.items():
            mil = c.get("million", {})
            if mil.get("rss_total"):
                rows.append((name, mil["rss_total"], mil.get("rss_per_fiber"), mil.get("n")))
        rows.sort(key=lambda r: r[1])
        if rows:
            n = rows[0][3]
            L.append("### Memory per idle fiber")
            L.append("")
            L.append("Used resident memory (RSS, not virtual) for %s live parked fibers/"
                     "goroutines.[^bench]" % commafy(n))
            L.append("")
            L.append("| Config | total RSS | bytes / fiber |")
            L.append("|---|--:|--:|")
            for name, tot, per, _ in rows:
                L.append("| %s | %.2f GiB | %s |" % (name, tot / 2**30, commafy(per)))
            L.append("")

    # --- speed headline ---
    if speed:
        m = speed.get("metrics", {})
        sp = m.get("spawn", {})
        cx = m.get("ctxswitch", {})
        if sp or cx:
            L.append("### Scheduler micro-benchmarks")
            L.append("")
            L.append("| Runtime | spawn (tasks/s) | ctx-switch (ns) |")
            L.append("|---|--:|--:|")
            rts = ["runloom", "go", "asyncio", "uvloop", "greenlet"]
            for rt in rts:
                s = sp.get(rt, {})
                c = cx.get(rt, {})
                L.append("| %s | %s | %s |" % (
                    rt, commafy(s.get("rate_per_s")) if s.get("rate_per_s") else "n/a",
                    commafy(c.get("ns_per_switch")) if c.get("ns_per_switch") else "n/a"))
            L.append("")
            L.append("> Runloom fibers carry real C stacks (heavier to spawn than goroutines); "
                     "its loaded-yield context-switch hits the free-threaded refcount wall at high "
                     "hub counts. Strength is parallel I/O throughput, not single-stream latency.[^bench]")
            L.append("")

    L.append("[^bench]: Full data, methodology, per-connection ladder curves, the assumed "
             "constraints, every benchmark program's source, and the zero-PyObject Cython "
             "disassembly proof: [`%s`](%s). Cross-platform backend syscall profiles "
             "(Linux/macOS/Windows) are linked from there." % (REPORT, REPORT))
    L.append("")

    out = os.path.join(BENCH, "README_SECTIONS.md")
    with open(out, "w") as f:
        f.write("\n".join(L))
    print("wrote", out)


if __name__ == "__main__":
    main()
