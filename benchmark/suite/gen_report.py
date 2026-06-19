#!/usr/bin/env python3
"""Generate the single consolidated benchmark report: benchmark/report.html.

Consumes results/{env,perf,speed,mem}.json, renders sortable tables (per-core
normalised + raw), the assumed constraints/methodology, an embedded code viewer
for every benchmark program, and links to the pre-existing cross-platform
backend syscall profiles (linux/mac/win).
"""
import html
import json
import os
import sys
import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness"))
import config

RES = config.RESULTS_DIR
BENCH = config.BENCH_DIR
SUITE = config.SUITE_DIR


def load(name):
    p = os.path.join(RES, name)
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return None


def esc(s):
    return html.escape(str(s))


def fmt(x, nd=0):
    if x is None:
        return "&mdash;"
    if isinstance(x, float):
        if nd == 0:
            return "{:,.0f}".format(x)
        return ("{:,.%df}" % nd).format(x)
    return esc(x)


# ---------------------------------------------------------------- table helper
def table(tid, headers, rows, note="", mark_best=True):
    """headers: list of (label, numeric?bool). rows: list of list of (display,
    sortvalue), ALREADY sorted best-first. With mark_best, row 0 is tagged as the
    winner (a trophy + a highlighted row) so the best config in each bench is
    visible at a glance; the tag rides the row if the reader re-sorts."""
    out = ['<table id="%s" class="sortable"><thead><tr>' % tid]
    for i, (lbl, num) in enumerate(headers):
        out.append('<th onclick="sortT(\'%s\',%d,%s)">%s<span class="ar"></span></th>'
                    % (tid, i, "true" if num else "false", esc(lbl)))
    out.append("</tr></thead><tbody>")
    for ri, r in enumerate(rows):
        best = mark_best and ri == 0
        out.append('<tr class="best">' if best else "<tr>")
        for ci, (disp, sortv) in enumerate(r):
            sv = "" if sortv is None else ' data-v="%s"' % sortv
            cell = ('<span class="trophy">&#127942;</span>' + disp) if (best and ci == 0) else disp
            out.append("<td%s>%s</td>" % (sv, cell))
        out.append("</tr>")
    out.append("</tbody></table>")
    if note:
        out.append('<p class="note">%s</p>' % note)
    return "\n".join(out)


def code_block(title, path, lang=""):
    if not os.path.exists(path):
        return ""
    with open(path) as f:
        src = f.read()
    rel = os.path.relpath(path, BENCH)
    return ('<details class="code"><summary>%s <span class="path">%s</span></summary>'
            '<pre><code>%s</code></pre></details>'
            % (esc(title), esc(rel), esc(src)))


# ---------------------------------------------------------------- sections
def sec_header(envd):
    e = envd or {}
    numa = ", ".join("%s=%s" % (k, v) for k, v in (e.get("numa_nodes") or {}).items())
    rb = e.get("runloom_build", {})
    rows = [
        ("Host", "%s (%s, %s)" % (e.get("hostname"), e.get("virtualization"), e.get("os"))),
        ("Kernel", "%s %s" % (e.get("kernel"), e.get("arch"))),
        ("CPU", "%s" % e.get("cpu_model")),
        ("Logical CPUs / NUMA", "%s vCPUs &mdash; %s" % (e.get("logical_cpus"), numa)),
        ("Memory", "%s GiB" % e.get("mem_total_gib")),
        ("CPU governor / steal", "%s / %s%%" % (e.get("cpu_governor"), e.get("steal_pct_sample"))),
        ("Runloom interp", "%s @ %s" % (e.get("python_ft_3_13t"), e.get("runloom_git_sha"))),
        ("Runloom build", esc(rb.get("expected_cflags", "")) + " (RUNLOOM_DEBUG=%s)" % rb.get("RUNLOOM_DEBUG_env")),
        ("Baseline interp", "%s &mdash; uvloop %s, gevent %s, greenlet %s"
         % (e.get("python_gil_3_13"), e.get("uvloop"), e.get("gevent"), e.get("greenlet"))),
        ("Go", e.get("go_version")),
        ("Cython", e.get("cython_version")),
    ]
    body = "".join("<tr><th>%s</th><td>%s</td></tr>" % (k, v) for k, v in rows)
    return ('<h2 id="env">Machine &amp; toolchain</h2>'
            '<table class="kv">%s</table>'
            '<p class="warn">This is a VMware guest &mdash; the CPUs are vCPUs and may '
            'incur hypervisor steal; numbers are valid for relative comparison on '
            'this host, not absolute hardware peaks.</p>' % body)


def sec_constraints(meta):
    m = meta or {}
    items = [
        ("CPU sizing", "hubs = int(cpu&times;0.7) = <b>%s</b>; go GOMAXPROCS = <b>%s</b>; "
         "client = int(cpu&times;0.25) = <b>%s</b>" % (m.get("hubs"), m.get("go_server_cores"), m.get("client_cores"))),
        ("CPU placement", "client cpus <code>%s</code>; server cpus <code>%s</code> "
         "(disjoint, so the loadgen never steals a server core)" % (m.get("client_cpus"), m.get("server_cpus"))),
        ("Network", "veth pair across two netns (10.99.0.1 &harr; 10.99.0.2); spec sysctls "
         "applied inside the server netns; empty firewall ruleset (no host nft tax)"),
        ("Sysctls", "; ".join("%s = %s" % (k, v) for k, v in (m.get("sysctls") or {}).items())),
        ("fd limit", "RLIMIT_NOFILE raised to %s per exec via prlimit (the editor-shell 4096 cap "
         "does not propagate)" % "{:,}".format(m.get("fd_limit", 0))),
        ("Debug", "RUNLOOM_DEBUG unset; as-shipped -O2 -DNDEBUG release build, no sanitizers"),
        ("TCP_NODELAY", "set once per connection at setup (listener + client), never in the "
         "per-request loop"),
        ("req/s payload", "%s B (small &rarr; scheduling/syscall bound, the headline req/s)" % m.get("payload_small_bytes")),
        ("bandwidth payload", "%s B = 1.5 MiB (large &rarr; copy/IO bound, reported as GB/s)" % "{:,}".format(m.get("payload_large_bytes", 0))),
        ("Stop rule", "geometric connection ladder; a rung must beat the incumbent peak's "
         "bootstrap-CI upper bound to count; %s consecutive misses stop the sweep; "
         "%s reps/rung" % (config.PLATEAU_PATIENCE, m.get("reps"))),
        ("Per-core scaling", "throughput metrics (req/s, spawn, http, GB/s) are divided by the "
         "runtime's core count; latency metrics (ctx-switch, RTT) are not divided; single-"
         "threaded runtimes are already 1 core. We do NOT measure run(1) as 'runloom per core' "
         "&mdash; that is the M:1 cooperative scheduler, a different runtime than the M:N work-stealer."),
        ("Saturation honesty", "the 16-core client cannot saturate the fastest servers for a "
         "symmetric echo, so each peak is tagged with the CPU-bound side and, when client-bound, "
         "a server-ceiling estimate (peak / server-CPU-utilisation)."),
    ]
    body = "".join("<tr><th>%s</th><td>%s</td></tr>" % (k, v) for k, v in items)
    return ('<h2 id="constraints">Assumed constraints &amp; methodology</h2>'
            '<table class="kv">%s</table>' % body)


def sec_perf(perf):
    if not perf:
        return '<h2 id="perf">Performance</h2><p class="warn">no perf.json yet</p>'
    servers = perf.get("servers", {})
    # req/s
    rows = []
    for name, s in servers.items():
        mt = s.get("metrics", {}).get("reqps", {})
        pk = mt.get("peak", {})
        if not pk or "rps_median" not in pk:
            continue
        cores = s.get("cores", 1)
        rps = pk.get("rps_median", 0)
        per = rps / cores
        ceil = mt.get("server_ceiling_est")
        rows.append([
            ('<b>%s</b><br><span class="sub">%s</span>' % (esc(name), esc(s.get("label", ""))), name),
            (esc(s.get("interp", "")), s.get("interp", "")),
            (fmt(cores), cores),
            (fmt(rps), rps),
            (fmt(per), per),
            (fmt(pk.get("conns")), pk.get("conns")),
            (fmt(pk.get("p99_us")), pk.get("p99_us")),
            (esc(mt.get("bottleneck_at_peak", "")), mt.get("bottleneck_at_peak", "")),
            (fmt(ceil), ceil or 0),
        ])
    rows.sort(key=lambda r: -(r[3][1] or 0))   # best = highest absolute req/s
    hdr = [("Server", False), ("Interp", False), ("Cores", True), ("Peak req/s", True),
           ("req/s / core", True), ("Conns@peak", True), ("p99 &micro;s", True),
           ("Bottleneck", False), ("Server-ceiling est.", True)]
    reqps_tbl = table("t_reqps", hdr, rows,
                      "Sorted by req/s per core (the spec's scale-to-1-core normalisation). "
                      "Small 1 KiB payload &rarr; measures scheduling + syscall overhead, not "
                      "bandwidth. <b>Two stories, both true:</b> by <i>absolute</i> req/s (click "
                      "'Peak req/s') the 44-hub M:N runtimes (runloom, go) win by ~10&times; &mdash; "
                      "they use the whole machine; by <i>per-core</i> the single-threaded GIL event "
                      "loops (uvloop, asyncio) win, because free-threading pays an atomic-refcount / "
                      "cross-core tax per core that a single-threaded loop avoids. A 'client' "
                      "bottleneck means the 16-core loadgen saturated first, so that row's per-core "
                      "is an <i>under</i>-estimate &mdash; the server-ceiling column is the fairer "
                      "per-core proxy there.")
    # bandwidth
    brows = []
    for name, s in servers.items():
        mt = s.get("metrics", {}).get("bandwidth", {})
        pk = mt.get("peak", {})
        if not pk or "rps_median" not in pk:
            continue
        cores = s.get("cores", 1)
        payload = mt.get("payload", config.PAYLOAD_LARGE)
        gbps = pk.get("rps_median", 0) * payload * 2 / 1e9
        brows.append([
            ('<b>%s</b>' % esc(name), name),
            (fmt(cores), cores),
            (fmt(gbps, 2), gbps),
            (fmt(gbps / cores, 3), gbps / cores),
            (fmt(pk.get("conns")), pk.get("conns")),
            (esc(mt.get("bottleneck_at_peak", "")), mt.get("bottleneck_at_peak", "")),
        ])
    brows.sort(key=lambda r: -(r[2][1] or 0))
    bhdr = [("Server", False), ("Cores", True), ("Peak GB/s", True), ("GB/s / core", True),
            ("Conns@peak", True), ("Bottleneck", False)]
    bw_tbl = table("t_bw", bhdr, brows,
                   "1.5 MiB payload echoed (send + receive counted). Aggregate over the veth pair; "
                   "client-bound at the peak in most rows.")
    # full connection-ladder curves (methodology: raise conns until req/s stops growing)
    curves = ['<h3>Connection-ladder curves (req/s)</h3>'
              '<p class="note">The stop rule walks connections up a geometric ladder until '
              'req/s stops beating the peak\'s CI. Each server\'s full curve:</p>']
    for name, s in servers.items():
        mt = s.get("metrics", {}).get("reqps", {})
        curve = mt.get("curve")
        if not curve:
            continue
        crows = []
        for rung in curve:
            ci = rung.get("rps_ci", [None, None])
            crows.append([
                (fmt(rung.get("conns")), rung.get("conns")),
                (fmt(rung.get("rps_median")), rung.get("rps_median")),
                ("%s&ndash;%s" % (fmt(ci[0]), fmt(ci[1])), ci[0]),
                (fmt((rung.get("server_cpu_util") or 0) * 100, 0) + "%", rung.get("server_cpu_util")),
                (fmt((rung.get("client_cpu_util") or 0) * 100, 0) + "%", rung.get("client_cpu_util")),
                (fmt(rung.get("p99_us")), rung.get("p99_us")),
                (fmt(rung.get("errors")), rung.get("errors")),
            ])
        ch = [("Conns", True), ("req/s", True), ("95% CI", False), ("srv CPU", True),
              ("cli CPU", True), ("p99 &micro;s", True), ("err", True)]
        curves.append('<details class="code"><summary>%s &mdash; %d rungs (peak %s req/s)</summary>%s</details>'
                      % (esc(name), len(curve), fmt(mt.get("peak", {}).get("rps_median")),
                         table("c_%s" % name, ch, crows, mark_best=False)))
    return ('<h2 id="perf">Performance &mdash; requests / second</h2>%s%s'
            '<h3>Performance &mdash; bandwidth (1.5 MB streaming)</h3>%s'
            % (reqps_tbl, "\n".join(curves), bw_tbl))


def sec_speed(speed):
    if not speed:
        return '<h2 id="speed">Speed</h2><p class="warn">no speed.json yet</p>'
    m = speed.get("metrics", {})
    out = ['<h2 id="speed">Speed micro-benchmarks</h2>']

    # spawn
    rows = []
    for rt, d in (m.get("spawn") or {}).items():
        if "rate_per_s" not in d:
            continue
        cores = d.get("cores", 1)
        rows.append([(esc(rt), rt), (fmt(cores), cores), (fmt(d["rate_per_s"]), d["rate_per_s"]),
                     (fmt(d["seconds"] * 1e6 / d["n"], 2), d["seconds"] * 1e6 / d["n"]),
                     (fmt(d["rate_per_s"] / cores), d["rate_per_s"] / cores)])
    rows.sort(key=lambda r: -(r[2][1] or 0))   # best = highest absolute spawn rate
    out.append("<h3>Spawn 1M fibers / goroutines / coroutines</h3>")
    out.append(table("t_spawn", [("Runtime", False), ("Cores", True), ("spawn/s", True),
                                 ("&micro;s/task", True), ("spawn/s / core", True)], rows,
                     "Higher is better. One spawner creates N tasks; drained to completion. "
                     "runloom &amp; greenlet carry real C stacks (heavyweight vs goroutines)."))

    # ctxswitch
    rows = []
    for rt, d in (m.get("ctxswitch") or {}).items():
        if "ns_per_switch" not in d:
            continue
        rows.append([(esc(rt), rt), (fmt(d.get("cores", 1)), d.get("cores", 1)),
                     (fmt(d["ns_per_switch"]), d["ns_per_switch"])])
    rows.sort(key=lambda r: (r[2][1] or 1e18))
    out.append("<h3>Context switch (loaded-yield)</h3>")
    out.append(table("t_ctx", [("Runtime", False), ("Cores", True), ("ns / switch", True)], rows,
                     "Lower is better. G concurrent tasks each yield K times (run queues stay full "
                     "&mdash; same-hub re-dispatch, not a 2-party ping-pong). runloom at 44 hubs hits "
                     "the free-threaded cross-core refcount wall here; at &le;8 hubs it is ~400 ns/switch."))

    # http
    rows = []
    for rt, d in (m.get("http") or {}).items():
        if "rps" not in d:
            continue
        cores = d.get("cores", 1)
        rows.append([(esc(rt), rt), (fmt(cores), cores), (fmt(d["rps"]), d["rps"]),
                     (fmt(d["rps"] / cores), d["rps"] / cores)])
    rows.sort(key=lambda r: -(r[2][1] or 0))   # best = highest absolute req/s
    out.append("<h3>HTTP req/s (client vs a Go httpd)</h3>")
    out.append(table("t_http", [("Runtime", False), ("Cores", True), ("req/s", True),
                                ("req/s / core", True)], rows,
                     "Higher is better. The runtime under test is the HTTP <i>client</i> "
                     "(keepalive GET) against a fixed Go server."))

    # rtt
    rows = []
    for rt, d in (m.get("rtt") or {}).items():
        if "ns_per_rtt" not in d:
            continue
        rows.append([(esc(rt), rt), (fmt(d["ns_per_rtt"]), d["ns_per_rtt"]),
                     (fmt(d["ns_per_rtt"] / 1000, 2), d["ns_per_rtt"] / 1000)])
    rows.sort(key=lambda r: (r[1][1] or 1e18))
    out.append("<h3>TCP round-trip latency (to a Go echo server)</h3>")
    out.append(table("t_rtt", [("Runtime", False), ("ns / RTT", True), ("&micro;s / RTT", True)], rows,
                     "Lower is better. Single connection, sequential. Dominated by the ~70&micro;s "
                     "veth round-trip floor on this VM; runtime overhead is the spread above it."))
    return "\n".join(out)


def sec_iouring(iou):
    if not iou:
        return ""
    pairs = [("cecho_epoll", "cecho_iouring", "8-byte all-C echo (handler=None, tstate-free c_entry)"),
             ("cython_epoll", "cython_iouring_proactor", "1 KiB Cython C handler")]
    rows = []
    for a, b, label in pairs:
        ra, rb = iou.get(a, {}), iou.get(b, {})
        pka, pkb = ra.get("peak", {}), rb.get("peak", {})
        if "rps_median" not in pka or "rps_median" not in pkb:
            continue
        ea, eb = pka["rps_median"], pkb["rps_median"]
        ca = ra.get("server_ceiling_est") or ea
        cb = rb.get("server_ceiling_est") or eb
        rows.append([
            (esc(label), label),
            (fmt(ea) + ' <span class="sub">(%s)</span>' % esc(ra.get("bottleneck_at_peak", "")), ea),
            (fmt(eb) + ' <span class="sub">(%s)</span>' % esc(rb.get("bottleneck_at_peak", "")), eb),
            (fmt(ca), ca), (fmt(cb), cb),
            ('<b>%.2f&times;</b>' % (cb / ca) if ca else "&mdash;", cb / ca if ca else 0),
        ])
    if not rows:
        return ""
    hdr = [("Workload", False), ("epoll peak", True), ("io_uring peak", True),
           ("epoll ceiling", True), ("io_uring ceiling", True), ("uring/epoll ceiling", True)]
    # tstate-bypass comparison: Python-fiber Cython def vs tstate-free cdef c_entry
    trows = []
    for cy, cd, label in [("cython_iouring_8b", "cdef_iouring_8b", "8-byte echo (op-bound)"),
                          ("cython_iouring_proactor", "cdef_iouring_1k", "1 KiB echo (I/O-bound)")]:
        rcy, rcd = iou.get(cy, {}), iou.get(cd, {})
        ccy = rcy.get("server_ceiling_est") or rcy.get("peak", {}).get("rps_median")
        ccd = rcd.get("server_ceiling_est") or rcd.get("peak", {}).get("rps_median")
        if not ccy or not ccd:
            continue
        trows.append([(esc(label), label), (fmt(ccy), ccy), (fmt(ccd), ccd),
                      ("%+.1f%%" % ((ccd / ccy - 1) * 100), ccd / ccy)])
    tstate_tbl = ""
    if trows:
        tstate_tbl = ('<h3>Thread-state bypass: Cython <code>def</code> (tstate) vs '
                      '<code>cdef</code> c_entry (tstate-free)</h3>'
                      + table("t_tstate", [("Workload", False), ("Cython def ceiling", True),
                              ("cdef c_entry ceiling", True), ("cdef vs def", True)], trows,
                              mark_best=False, note=
                              "Both on the io_uring proactor. The tstate-free cdef handler is within noise "
                              "of the Python-fiber Cython handler at BOTH payloads &mdash; the default "
                              "per-hub snapshot tstate is already cheap (a few ints, not a PyThreadState), "
                              "so bypassing it buys ~nothing on throughput (the c_entry path's value is "
                              "per-fiber memory). See IOURING_TSTATE_FINDINGS.md."))
    return ('<h2 id="iouring">io_uring loop backend vs epoll</h2>'
            '<p>Driven through the Stage-2 <b>proactor</b> (<code>loop_recv</code>), '
            'io_uring is a major win for a real handler &mdash; <b>+2.17&times; server '
            'ceiling at 1 KiB</b>, the fastest runloom config in the suite. The earlier '
            '"io_uring loses on loopback" was an artifact of driving it through the '
            'readiness path (recv + an epoll&rarr;ring bridge). Full reasoning, the '
            '"+20%" reconciliation, and the thread-state cost analysis are in '
            '<a href="IOURING_TSTATE_FINDINGS.md">IOURING_TSTATE_FINDINGS.md</a>.</p>'
            + table("t_iou", hdr, rows, mark_best=False, note=
                    "Peaks are often client-bound (the 16-core loadgen), so the "
                    "server-ceiling columns (peak / server-CPU-util) are the fairer "
                    "comparison. At 8 bytes the gain is small because the all-C epoll path "
                    "is already near-optimal; at 1 KiB the proactor cuts server CPU 85%&rarr;55%.")
            + tstate_tbl)


def sec_work(work):
    """The handler work-curve: ONE server, ONE knob (--work N = FNV passes over
    the payload), TWO builds of the identical algorithm (interpreted py_fnv vs
    compiled work_cy.fnv_work). work=0 IS the echo, so it consolidates the echo
    load as the leftmost point. The gap that opens as work grows is the thing
    echo can never show -- a handler optimization needs handler CPU to pay."""
    if not work:
        return ""
    res = work.get("results", {})
    meta = work.get("meta", {})
    py, cy = res.get("py", {}), res.get("cython", {})
    works = meta.get("works", sorted({int(k) for k in py} | {int(k) for k in cy}))
    rows = []
    max_speed = 0.0
    for w in works:
        rpy, rcy = py.get(str(w), {}), cy.get(str(w), {})
        ppy, pcy = rpy.get("peak", {}), rcy.get("peak", {})
        vpy = ppy.get("rps_median")
        vcy = pcy.get("rps_median")
        if vpy is None or vcy is None:
            continue
        spd = (vcy / vpy) if vpy else 0.0
        max_speed = max(max_speed, spd)
        cpu_py = (ppy.get("server_cpu_util") or 0) * 100
        cpu_cy = (pcy.get("server_cpu_util") or 0) * 100
        wlabel = ("%d <span class=\"sub\">(echo)</span>" % w) if w == 0 else str(w)
        rows.append([
            (wlabel, w),
            (fmt(vpy) + ' <span class="sub">%.0f%% CPU</span>' % cpu_py, vpy),
            (fmt(vcy) + ' <span class="sub">%.0f%% CPU</span>' % cpu_cy, vcy),
            ('<b>%.2f&times;</b>' % spd, spd),
            (esc(rpy.get("bottleneck_at_peak", "")), rpy.get("bottleneck_at_peak", "")),
        ])
    if not rows:
        return ""
    hdr = [("FNV passes (--work)", True), ("Python handler req/s", True),
           ("Cython handler req/s", True), ("Cython / Python", True),
           ("Python bottleneck", False)]
    pl = meta.get("payload", 1024)
    return ('<h2 id="work">Handler work curve &mdash; what compiling the handler buys</h2>'
            '<p>Every handler-side optimization <em>ties</em> on echo because a TCP echo '
            'does no CPU work in the handler (the cost is the kernel TCP path). This is the '
            'one experiment that gives the handler something to do: <b>one server</b> '
            '(<code>srv_runloom_work.py</code>), <b>one knob</b> (<code>--work N</code> = an '
            'FNV-1a byte hash over the %d&nbsp;B payload, repeated N times, folded into the '
            'reply so it can\'t be elided), and <b>two builds of the identical algorithm</b>: '
            'the interpreted <code>py_fnv()</code> vs the Cython-compiled '
            '<code>work_cy.fnv_work()</code>. Same runtime, same I/O path &mdash; the '
            '<em>only</em> variable is whether the handler\'s per-byte work is interpreted '
            'or native.</p>'
            '<p><b><code>--work&nbsp;0</code> is the echo</b> (the handler skips the work '
            'call entirely), so it consolidates the echo load as the leftmost point and '
            'should reproduce the echo numbers &mdash; a built-in cross-check. As the knob '
            'grows the Python curve bends down (it goes server-bound) while the Cython curve '
            'holds; the peak <code>Cython / Python</code> ratio here is <b>%.2f&times;</b>. '
            '<em>That</em> gap is what the whole Cython/cdef handler path was built to earn '
            'and echo structurally could not show.</p>'
            % (pl, max_speed)
            + table("t_work", hdr, rows, mark_best=False, note=
                    "Read the <b>Cython / Python ratio</b> column as the robust signal: it is "
                    "monotonic because the Python side collapses (server-bound from the first "
                    "pass). The Cython handler's <em>absolute</em> numbers through ~work&nbsp;4 are "
                    "the 16-core loadgen ceiling (bottleneck <code>client</code>/<code>neither</code>, "
                    "the same wall echo hits), not the server &mdash; so the early Cython curve is "
                    "flat at the client limit, with minor sub-saturation wobble, before it goes "
                    "genuinely server-bound under heavy work. "
                    "The work is PURE inline arithmetic (an FNV xor/mul loop) &mdash; nothing "
                    "runloom routes to the blockpool, so it runs on the fiber's hub and the "
                    "per-core CPU accounting stays valid. <b>Honest framing:</b> if the handler "
                    "delegated this to a C-accelerated library (<code>hashlib</code>, "
                    "<code>json</code>, <code>struct</code>), Python and Cython would converge "
                    "&mdash; both just call the same native code, back to echo-equal. The gap "
                    "only appears for <em>handler-level</em> Python work; that is the actual "
                    "lesson. See WORK_CURVE_EXPERIMENT.md."))


def sec_mem(mem):
    if not mem:
        return '<h2 id="mem">Memory</h2><p class="warn">no mem.json yet</p>'
    cfgs = mem.get("configs", {})
    rows = []
    for name, c in cfgs.items():
        e = c.get("empty", {})
        s = c.get("socket", {})
        mil = c.get("million", {})
        rows.append([
            (esc(name), name),
            (fmt(e.get("bytes_per_fiber_rss")), e.get("bytes_per_fiber_rss") or 0),
            (fmt(s.get("bytes_per_fiber_rss")), s.get("bytes_per_fiber_rss") or 0),
            (fmt((mil.get("rss_total") or 0) / 2**30, 2), mil.get("rss_total") or 0),
            (fmt(mil.get("rss_per_fiber")), mil.get("rss_per_fiber") or 0),
            (fmt(mil.get("n")), mil.get("n") or 0),
        ])
    rows.sort(key=lambda r: (r[1][1] or 1e18))
    hdr = [("Config", False), ("empty B/fiber", True), ("w/socket B/fiber", True),
           ("N&times;fiber total RSS (GiB)", True), ("B/fiber @ scale", True), ("N", True)]
    return ('<h2 id="mem">Memory (used RSS, not virtual)</h2>%s'
            % table("t_mem", hdr, rows,
                    "All figures are resident set size (used physical memory), not virtual address "
                    "space. 'empty' = bare parked fiber; 'w/socket' = fiber holding a socketpair end "
                    "+ its handler buffer (the py handler's 64 KiB bytearray dominates; the Cython "
                    "handler's stack buffer faults similarly, so its win is CPU, not idle RSS). "
                    "optimize(memory) does not shrink idle parked-fiber RSS &mdash; it tunes "
                    "blockpool/prewarm. The scale column is the headline 1M-fiber resident set. "
                    "NB: the default tstate mode is per-hub snapshot (no per-fiber PyThreadState); "
                    "the gated per-g mode adds a full PyThreadState = ~18 KB/fiber (~26.7 KB total, "
                    "vs 8.8 KB snapshot) &mdash; see IOURING_TSTATE_FINDINGS.md."))


def sec_code():
    blocks = []
    files = [
        ("Server tier 1 &mdash; runloom sync wrappers", "suite/servers/srv_runloom_sync.py"),
        ("Server tier 2 &mdash; runloom_c.serve (py handler)", "suite/servers/srv_runloom_c.py"),
        ("Server tiers 4/5 &mdash; runloom_c.serve + Cython handler", "suite/servers/srv_runloom_cython.py"),
        ("Cython zero-PyObject handler", "suite/servers/handler_cy.pyx"),
        ("Work-curve server (--work N, work=0==echo)", "suite/servers/srv_runloom_work.py"),
        ("Work-curve compiled FNV (pure inline arithmetic)", "suite/servers/work_cy.pyx"),
        ("Work-curve sweep driver", "suite/work_sweep.py"),
        ("C-API exposed for the Cython handler", "../src/runloom_c/runloom_tcp_capi.c.inc"),
        ("Cython hot-loop disassembly (zero-PyObject proof)", "suite/servers/handler_cy_hotloop_disasm.txt"),
        ("asyncio / uvloop server", "suite/servers/srv_asyncio.py"),
        ("gevent server", "suite/servers/srv_gevent.py"),
        ("Go server", "suite/servers/srv_go.go"),
        ("Go closed-loop loadgen", "suite/clients/loadgen.go"),
        ("Speed &mdash; runloom", "suite/speed/speed_runloom.py"),
        ("Speed &mdash; asyncio/uvloop", "suite/speed/speed_asyncio.py"),
        ("Speed &mdash; greenlet/gevent", "suite/speed/speed_greenlet.py"),
        ("Speed &mdash; go", "suite/speed/speed_go.go"),
        ("Memory &mdash; runloom probe", "suite/memory/mem_runloom.py"),
        ("Memory &mdash; go probe", "suite/memory/mem_go.go"),
        ("Harness &mdash; config / constraints", "suite/harness/config.py"),
        ("Harness &mdash; topology (veth/netns/pin/fd)", "suite/harness/topo.py"),
        ("Harness &mdash; measurement (ladder/CI/CPU)", "suite/harness/measure.py"),
        ("io_uring vs epoll comparison program", "suite/iouring_compare.py"),
        ("io_uring &amp; thread-state findings (full writeup)", "IOURING_TSTATE_FINDINGS.md"),
        ("Archived original prompt + scoping decisions", "prompt/original_spec.md"),
    ]
    for title, rel in files:
        blocks.append(code_block(title, os.path.join(BENCH, rel)))
    return ('<h2 id="code">Benchmark source &amp; constraints</h2>'
            '<p>Every program and the assumed constraints, embedded for reproducibility.</p>'
            + "\n".join(b for b in blocks if b))


def sec_profiles():
    links = [
        ("big100_syscall_backends.html", "Cross-backend syscall comparison (epoll vs kqueue vs IOCP)"),
        ("big100_syscall_profile_linux.html", "Linux syscall profile (strace, epoll)"),
        ("big100_syscall_profile_mac.html", "macOS syscall profile (ktrace/KDEBUG, kqueue)"),
        ("big100_syscall_profile_win.html", "Windows syscall profile (xperf, IOCP-AFD)"),
    ]
    items = []
    for fn, desc in links:
        if os.path.exists(os.path.join(BENCH, fn)):
            items.append('<li><a href="%s">%s</a> &mdash; %s</li>' % (fn, fn, desc))
    return ('<h2 id="profiles">Cross-platform backend profiling</h2>'
            '<p>Pre-existing syscall-level profiles of the runloom backends on each OS '
            '(how epoll / kqueue / IOCP differ under the big_100 workload):</p>'
            '<ul>%s</ul>' % "".join(items))


CSS = """
:root{--bg:#0f1419;--panel:#171c24;--fg:#d6dde6;--mut:#8b97a6;--acc:#6cb6ff;--good:#6cd97e;--warn:#ffcc66;--line:#2a323d}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif}
a{color:var(--acc)}h1{font-size:24px}h2{font-size:20px;border-bottom:2px solid var(--line);padding-bottom:6px;margin-top:40px}
h3{font-size:16px;color:var(--mut);margin-top:24px}
.wrap{max-width:1100px;margin:0 auto;padding:24px}
nav{position:sticky;top:0;background:var(--panel);border-bottom:1px solid var(--line);padding:10px 24px;z-index:9;font-size:13px}
nav a{margin-right:14px;text-decoration:none}
table{border-collapse:collapse;width:100%;margin:12px 0;background:var(--panel);border:1px solid var(--line)}
th,td{padding:7px 10px;text-align:right;border-bottom:1px solid var(--line)}
th:first-child,td:first-child{text-align:left}
thead th{background:#1d242e;cursor:pointer;user-select:none;position:relative;white-space:nowrap}
thead th:hover{color:var(--acc)}.ar{font-size:10px;color:var(--mut);margin-left:4px}
tbody tr:hover{background:#1d242e}.sub{color:var(--mut);font-size:11px}
tr.best{background:#16301f}tr.best td{font-weight:600;color:#eafaf0}tr.best:hover{background:#1b3a26}
.trophy{margin-right:6px;filter:saturate(1.3)}
table.kv th{text-align:left;width:210px;color:var(--mut);font-weight:600;cursor:default}
table.kv th:hover{color:var(--mut)}
.note{color:var(--mut);font-size:12px;margin:4px 0 18px}
.warn{color:var(--warn);font-size:13px}
details.code{margin:6px 0;background:var(--panel);border:1px solid var(--line);border-radius:4px}
details.code summary{cursor:pointer;padding:8px 12px;font-weight:600;display:flex;justify-content:space-between;align-items:baseline;gap:16px}
details.code summary::-webkit-details-marker{flex:0 0 auto}
details.code .path{color:var(--mut);font-weight:400;font-size:11px;white-space:nowrap;flex:0 0 auto}
pre{margin:0;padding:12px;overflow:auto;max-height:520px;background:#0c1116;font:12px/1.45 ui-monospace,Menlo,monospace}
code{color:#cbd5e1}
"""

JS = """
function sortT(id,col,num){var t=document.getElementById(id);var tb=t.tBodies[0];
var rows=[].slice.call(tb.rows);var dir=t.getAttribute('d'+col)==='1'?-1:1;t.setAttribute('d'+col,dir===1?'1':'0');
rows.sort(function(a,b){var x=a.cells[col],y=b.cells[col];
var xv=num?parseFloat(x.getAttribute('data-v')||x.textContent.replace(/[^0-9.\\-]/g,'')):x.textContent.trim();
var yv=num?parseFloat(y.getAttribute('data-v')||y.textContent.replace(/[^0-9.\\-]/g,'')):y.textContent.trim();
if(num){xv=isNaN(xv)?-Infinity:xv;yv=isNaN(yv)?-Infinity:yv;return (xv-yv)*dir;}
return xv<yv?-dir:xv>yv?dir:0;});
rows.forEach(function(r){tb.appendChild(r);});}
"""


def main():
    envd = load("env.json")
    perf = load("perf.json") or load("perf_quick.json")
    speed = load("speed.json") or load("speed_quick.json")
    mem = load("mem.json") or load("mem_quick.json")
    iou = load("iouring_test.json")
    work = load("work_curve.json")
    meta = (perf or speed or mem or {}).get("meta") or config.summary()
    quick = any(d and d.get("quick") for d in (perf, speed, mem))

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    nav = ('<nav><b>Runloom benchmarks</b> '
           '<a href="#env">machine</a><a href="#constraints">constraints</a>'
           '<a href="#perf">req/s</a><a href="#iouring">io_uring</a>'
           '<a href="#work">work curve</a>'
           '<a href="#speed">speed</a><a href="#mem">memory</a>'
           '<a href="#code">code</a><a href="#profiles">profiles</a></nav>')
    parts = [
        '<!doctype html><html><head><meta charset="utf-8">',
        '<title>Runloom benchmark report</title><style>%s</style></head><body>' % CSS,
        nav, '<div class="wrap">',
        '<h1>Runloom benchmark report</h1>',
        '<p class="note">Generated %s%s. Throughput is shown raw and divided down to one '
        'core; latencies are not divided. Click any column header to sort.</p>'
        % (now, " &mdash; <b>QUICK/SMOKE DATA</b>" if quick else ""),
        sec_header(envd),
        sec_constraints(meta),
        sec_perf(perf),
        sec_iouring(iou),
        sec_work(work),
        sec_speed(speed),
        sec_mem(mem),
        sec_profiles(),
        sec_code(),
        '</div><script>%s</script></body></html>' % JS,
    ]
    out = os.path.join(BENCH, "report.html")
    with open(out, "w") as f:
        f.write("\n".join(parts))
    print("wrote", out, "(%d KiB)" % (os.path.getsize(out) // 1024))


if __name__ == "__main__":
    main()
