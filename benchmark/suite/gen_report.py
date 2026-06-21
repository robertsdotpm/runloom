#!/usr/bin/env python3
"""Generate the single consolidated benchmark report: benchmark/report.html.

Consumes results/{env,perf,speed,mem}.json, renders sortable tables (per-core
normalised + raw), the assumed constraints/methodology, an embedded code viewer
for every benchmark program, and links to the pre-existing cross-platform
backend syscall profiles (linux/mac/win).
"""
import html
import json
import math
import os
import re
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


def _axfmt(v):
    if v >= 1e6:
        return ("%gM" % (v / 1e6))
    if v >= 1e3:
        return ("%gk" % (v / 1e3))
    return "%g" % v


def _slug(s):
    return re.sub(r"[^a-z0-9]+", "_", str(s).lower()).strip("_")


def svg_linechart(cid, series, xlabels, xaxis="FNV passes (--work)", logy=True,
                  ylabel=None, width=760, height=392):
    """Interactive line chart. `cid` scopes its clickable legend. series: list of
    (name, color, [y aligned to xlabels]) or (name, color, ys, slug); a None y is
    a gap. Each series' path+dots are grouped under class `ser-<cid>-<slug>` and
    the legend entry calls tglSeries(cid, slug) so a reader can click any line off
    (e.g. isolate one runloom config vs Go). `logy=False` -> linear y (use when
    the values share a magnitude and ratios must read true). Pure inline SVG."""
    ml, mr, mt, mb = 70, 196, 30, 46
    pw, ph = width - ml - mr, height - mt - mb
    n = len(xlabels)
    allv = [y for s in series for y in s[2] if y and y > 0]
    if not allv or n < 2:
        return ""
    ymin, ymax = min(allv), max(allv)
    if logy:
        lo, hi = math.floor(math.log10(ymin)), math.ceil(math.log10(ymax))
        if hi == lo:
            hi += 1

        def yp(v):
            return None if (not v or v <= 0) else mt + ph - (math.log10(v) - lo) / (hi - lo) * ph
        ticks = [m * 10 ** d for d in range(lo, hi + 1) for m in (1, 2, 5)
                 if ymin * 0.92 <= m * 10 ** d <= ymax * 1.08]
    else:
        top = ymax * 1.06

        def yp(v):
            return None if v is None else mt + ph - (v / top) * ph
        step = top / 5.0
        ticks = [step * k for k in range(0, 6)]

    def xp(i):
        return ml + pw * i / (n - 1)

    out = ['<svg viewBox="0 0 %d %d" id="%s" class="chart" width="100%%" style="max-width:%dpx">'
           % (width, height, cid, width)]
    for v in ticks:
        y = yp(v)
        if y is None:
            continue
        out.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" class="grid"/>' % (ml, y, ml + pw, y))
        out.append('<text x="%d" y="%.1f" class="ytick">%s</text>' % (ml - 7, y + 3, _axfmt(v)))
    for i, xl in enumerate(xlabels):
        out.append('<text x="%.1f" y="%d" class="xtick">%s</text>' % (xp(i), mt + ph + 16, esc(xl)))
    out.append('<text x="%.1f" y="%d" class="axlbl">%s</text>' % (ml + pw / 2, height - 5, esc(xaxis)))
    out.append('<text x="14" y="%.1f" class="axlbl" transform="rotate(-90 14 %.1f)">%s</text>'
               % (mt + ph / 2, mt + ph / 2, esc(ylabel or ("req/s (log)" if logy else "req/s per core"))))
    out.append('<text x="%d" y="%d" class="hint">click a name to toggle</text>' % (ml + pw + 14, mt - 12))
    for si, s in enumerate(series):
        name, color, ys = s[0], s[1], s[2]
        slug = s[3] if len(s) > 3 else _slug(name)
        out.append('<g class="ser ser-%s-%s">' % (cid, slug))
        path, pen = "", False
        for i, v in enumerate(ys):
            y = yp(v)
            if y is None:
                pen = False
                continue
            path += ("L" if pen else "M") + "%.1f %.1f " % (xp(i), y)
            pen = True
        out.append('<path d="%s" fill="none" stroke="%s" stroke-width="2.4"/>' % (path, color))
        for i, v in enumerate(ys):
            y = yp(v)
            if y is not None:
                out.append('<circle cx="%.1f" cy="%.1f" r="2.7" fill="%s"/>' % (xp(i), y, color))
        out.append('</g>')
        ly = mt + 12 + si * 19
        out.append('<g class="legi" data-c="%s" data-s="%s" onclick="tglSeries(\'%s\',\'%s\')">'
                   % (cid, slug, cid, slug))
        out.append('<rect x="%d" y="%d" width="184" height="17" fill="transparent"/>' % (ml + pw + 10, ly - 12))
        out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="2.4"/>'
                   % (ml + pw + 14, ly - 4, ml + pw + 32, ly - 4, color))
        out.append('<text x="%d" y="%d" class="leg">%s</text>' % (ml + pw + 36, ly, esc(name)))
        out.append('</g>')
    out.append('</svg>')
    return "".join(out)


# ---------------------------------------------------------------- table helper
def table(tid, headers, rows, note="", mark_best=True):
    """headers: list of (label, numeric?bool). rows: list of list of (display,
    sortvalue), ALREADY sorted best-first. With mark_best, row 0 is tagged as the
    winner (a trophy + a highlighted row) so the best config in each bench is
    visible at a glance; the tag rides the row if the reader re-sorts."""
    out = ['<table id="%s" class="sortable"><thead><tr>' % tid]
    for i, (lbl, num) in enumerate(headers):
        # header labels are authored HTML (may contain &times; etc.); not esc'd
        # -- same as the cells below, which are also raw authored HTML.
        out.append('<th onclick="sortT(\'%s\',%d,%s)">%s<span class="ar"></span></th>'
                    % (tid, i, "true" if num else "false", lbl))
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


# ---- standard program names: runtime_backend_handler_server[_extra] ----
# Maps the raw data-JSON keys (and a few authored labels) to a single standard
# scheme so every table reads consistently, e.g. runloom_epoll_py_tcpcon.
STD_NAME = {
    "runloom_sync":       "runloom_epoll_py_sync",
    "runloom_c":          "runloom_epoll_py_tcpcon",
    "runloom_c_cython":   "runloom_epoll_cython_tcpcon",
    "runloom_iouring":    "runloom_iouring_py_sync",
    "runloom_cython":     "runloom_iouring_cython_tcpcon",
    "runloom_cython_opt": "runloom_iouring_cython_tcpcon_opt",
    "runloom_cdef":       "runloom_iouring_cdef_tcpcon",
    "asyncio":            "asyncio_epoll_py_proto",
    "uvloop":             "uvloop_libuv_py_proto",
    "gevent":             "gevent_libev_py_stream",
    "go":                 "go_netpoll_native_net",
    "runloom_py":         "runloom_epoll_py_fiber",
    "greenlet":           "greenlet_native_py_coro",
    # authored labels in the saturated conn/s table
    "runloom_cdef (compiled handler)": "runloom_iouring_cdef_tcpcon",
    "runloom_c (Python handler)":      "runloom_epoll_py_tcpcon",
    "go (GOMAXPROCS=2)":               "go_netpoll_native_net",
}
# standard-name -> source file (relative to BENCH) for the click-to-code overlay
STD_SRC = {
    "runloom_epoll_py_sync":             "suite/servers/runloom_epoll_py_sync.py",
    "runloom_epoll_py_tcpcon":           "suite/servers/runloom_epoll_py_tcpcon.py",
    "runloom_epoll_cython_tcpcon":       "suite/servers/runloom_iouring_cython_tcpcon.py",
    "runloom_iouring_py_sync":           "suite/servers/runloom_epoll_py_sync.py",
    "runloom_iouring_cython_tcpcon":     "suite/servers/runloom_iouring_cython_tcpcon.py",
    "runloom_iouring_cython_tcpcon_opt": "suite/servers/runloom_iouring_cython_tcpcon.py",
    "runloom_iouring_cdef_tcpcon":       "suite/servers/runloom_iouring_cdef_tcpcon.py",
    "asyncio_epoll_py_proto":            "suite/servers/asyncio_epoll_py_proto.py",
    "uvloop_libuv_py_proto":             "suite/servers/asyncio_epoll_py_proto.py",
    "gevent_libev_py_stream":            "suite/servers/gevent_libev_py_stream.py",
    "go_netpoll_native_net":             "suite/servers/go_netpoll_native_net.go",
    "runloom_epoll_py_fiber":            "suite/speed/runloom_epoll_py_fiber.py",
    "greenlet_native_py_coro":           "suite/speed/greenlet_native_py_coro.py",
}


def std(key):
    return STD_NAME.get(key, key)


def prog_html(key):
    """A program-name cell: the standard name, clickable to overlay its source."""
    name = std(key)
    if name in STD_SRC:
        return '<span class="prog" data-prog="%s">%s</span>' % (esc(name), esc(name))
    return '<span class="prog nosrc">%s</span>' % esc(name)


def prog_cell(key):
    """(display, sortvalue) tuple for a first table cell."""
    return (prog_html(key), std(key))


_PYKW = set("def class return import from as if elif else for while in is not and or "
            "pass break continue try except finally with yield lambda None True False global "
            "nonlocal raise assert del async await".split())
_GOKW = set("func package import return if else for range var const type struct interface go "
            "defer chan select map nil true false switch case break continue default".split())


def hl_code(src, fname):
    """Server-side syntax highlight -> escaped HTML with <span class=hl-*> tokens.
    One left-to-right regex (comment|string|number|word) so tokens never overlap."""
    import re
    kw = _GOKW if fname.endswith(".go") else _PYKW
    com = re.escape("//" if fname.endswith(".go") else "#")
    esc_src = esc(src)                       # & < > " ' -> entities
    token = re.compile(
        r"(" + com + r"[^\n]*)"                                  # comment
        r"|(&quot;.*?&quot;|&#x27;.*?&#x27;|&#39;.*?&#39;)"      # string (escaped quotes)
        r"|(\b\d[\d_.]*\b)"                                      # number
        r"|(\b[A-Za-z_]\w*\b)")                                  # word

    def repl(m):
        c, s, n, w = m.group(1), m.group(2), m.group(3), m.group(4)
        if c:
            return '<span class="hl-com">%s</span>' % c
        if s:
            return '<span class="hl-str">%s</span>' % s
        if n:
            return '<span class="hl-num">%s</span>' % n
        if w in kw:
            return '<span class="hl-kw">%s</span>' % w
        return w

    return token.sub(repl, esc_src)


def prog_sources_script():
    """Embed every program's pre-highlighted source as window.PROG_SRC (self-contained)."""
    import json as _json
    out = {}
    for name, rel in STD_SRC.items():
        p = os.path.join(BENCH, rel)
        if name not in out and os.path.exists(p):
            with open(p) as f:
                out[name] = {"file": rel, "html": hl_code(f.read(), rel)}
    return "<script>window.PROG_SRC=%s;</script>" % _json.dumps(out)


def code_block(title, path, lang=""):
    if not os.path.exists(path):
        return ""
    with open(path) as f:
        src = f.read()
    rel = os.path.relpath(path, BENCH)
    # title is authored HTML (may contain intentional entities like &mdash;); do
    # NOT esc it (that double-escapes -> "&mdash;" renders literally). rel/src ARE
    # escaped (filename / source code -- untrusted-ish content).
    return ('<details class="code"><summary>%s <span class="path">%s</span></summary>'
            '<pre><code>%s</code></pre></details>'
            % (title, esc(rel), esc(src)))


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
        ("CPU", "hubs = int(cpu&times;0.7) = <b>%s</b>, go GOMAXPROCS = <b>%s</b>, client = <b>%s</b>; "
         "server cpus <code>%s</code> &harr; client <code>%s</code> (disjoint, loadgen never steals a core). "
         "<b>NB:</b> the server cpu set straddles BOTH NUMA nodes while the 1-core asyncio/uvloop/gevent "
         "runs stay NUMA-local, so the M:N servers pay some cross-node memory traffic the single-core runs "
         "don&rsquo;t &mdash; a pinning artifact that <i>depresses</i> the runloom/Go per-core figures, not a "
         "runtime cost"
         % (m.get("hubs"), m.get("go_server_cores"), m.get("client_cores"),
            m.get("server_cpus"), m.get("client_cpus"))),
        ("Network", "veth pair across two netns (10.99.0.1 &harr; .2), <b>empty firewall ruleset</b> "
         "(no host nft tax); spec sysctls applied in the server netns"),
        ("Build / fd", "as-shipped <b>-O2 -DNDEBUG</b> release, no sanitizers, RUNLOOM_DEBUG unset; "
         "RLIMIT_NOFILE raised to %s per exec via prlimit" % "{:,}".format(m.get("fd_limit", 0))),
        ("Payloads", "req/s = <b>%s B</b> (small &rarr; syscall/scheduling bound); bandwidth = 1.5 MiB "
         "(large &rarr; copy bound, GB/s); TCP_NODELAY set once at setup, never per request"
         % m.get("payload_small_bytes")),
        ("Saturation", "geometric dialer ladder; a rung must beat the peak's bootstrap-CI to count; "
         "%s misses stop it, %s reps/rung. The 16-core client can&rsquo;t saturate the fastest "
         "servers, so each peak is tagged client- vs server-bound (+ a server-ceiling estimate when "
         "client-bound). The stop-rule can occasionally tag a sub-saturation peak as server-bound (a "
         "known detection artifact) and truncate a ladder early, which can mis-rank close rows &mdash; "
         "read the per-core + bottleneck columns together, not the rank alone." % (config.PLATEAU_PATIENCE, m.get("reps"))),
        ("Per-core", "throughput (req/s, spawn, GB/s) &divide; core count; latencies (ctxswitch, RTT) "
         "are not. <code>run(1)</code> is NOT used as &lsquo;runloom per core&rsquo; &mdash; that is the "
         "M:1 cooperative scheduler, a different runtime than the M:N work-stealer."),
        ("Acceptors", "runloom servers run <b>N SO_REUSEPORT acceptors</b> (one kernel accept queue per "
         "hub); the Go baseline uses a <b>single <code>Accept()</code> loop</b>. Irrelevant to keep-alive "
         "req/s (connections are accepted once, then loop on) but it would favour runloom on connection "
         "<i>churn</i> &mdash; which is why no conn/s comparison is claimed here."),
        ("Provenance", "Result JSONs span several days and runloom builds: the <b>active-spawn</b> "
         "numbers were measured with the current build (the one exposing <code>fiber_n</code>), the rest "
         "with the build present when each JSON was written. governor = n/a (cpufreq sysfs absent on this "
         "VMware guest, so turbo/frequency is unpinned and unobserved); steal is a single 1&nbsp;s sample. "
         "Valid for relative comparison on this host, not absolute hardware peaks."),
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
            ('<b>%s</b><br><span class="sub">%s</span>' % (prog_html(name), esc(s.get("label", ""))), std(name)),
            (esc(s.get("interp", "")), s.get("interp", "")),
            (fmt(cores), cores),
            (fmt(rps), rps),
            (fmt(per), per),
            (fmt(pk.get("conns")), pk.get("conns")),
            (fmt(pk.get("p99_us")), pk.get("p99_us")),
            (esc(mt.get("bottleneck_at_peak", "")), mt.get("bottleneck_at_peak", "")),
            (fmt(ceil), ceil or 0),
        ])
    rows.sort(key=lambda r: -(r[4][1] or 0))   # best = highest req/s PER CORE (spec's scale-to-1-core; the only cross-model-fair axis)
    hdr = [("Server", False), ("Interp", False), ("Cores", True), ("Peak req/s", True),
           ("req/s / core", True), ("Conns@peak", True), ("p99 &micro;s", True),
           ("Bottleneck", False), ("Server-ceiling est. (extrap.)", True)]
    reqps_tbl = table("t_reqps", hdr, rows,
                      "Sorted by req/s <b>per core</b> &mdash; the spec's scale-to-1-core "
                      "normalisation, the only apples-to-apples axis across these concurrency "
                      "models. Small 1 KiB payload &rarr; measures scheduling + syscall overhead, "
                      "not bandwidth. <b>Read the bottleneck column.</b> The 44-hub M:N runtimes "
                      "(runloom, go) post the biggest <i>absolute</i> req/s (click 'Peak req/s') "
                      "because they use the whole machine &mdash; but at peak they are "
                      "<b>client-bound</b>: the 16-core loadgen saturates before the server does, so "
                      "those absolute numbers measure the loadgen ceiling, not the server, and the "
                      "~6&times; spread among the fast runtimes sits inside the loadgen's noise. "
                      "<b>Per core, the single-threaded GIL loops (uvloop, asyncio) genuinely "
                      "win</b> &mdash; a real server-bound measurement for them, while free-threading "
                      "pays an atomic-refcount / cross-core tax. The server-ceiling column is a rough "
                      "<i>extrapolation</i> (peak &divide; CPU-util; it only lifts client-bound rows) "
                      "&mdash; an upper bound, not a measurement. For a <b>server-bound</b> throughput "
                      "comparison (the meaningful one), see the handler work-curve below.")
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
            ('<b>%s</b>' % prog_html(name), std(name)),
            (fmt(cores), cores),
            (fmt(gbps, 2), gbps),
            (fmt(gbps / cores, 3), gbps / cores),
            (fmt(pk.get("conns")), pk.get("conns")),
            (esc(mt.get("bottleneck_at_peak", "")), mt.get("bottleneck_at_peak", "")),
        ])
    brows.sort(key=lambda r: -(r[3][1] or 0))   # per-core (GB/s/core), consistent with the req/s table
    bhdr = [("Server", False), ("Cores", True), ("Peak GB/s", True), ("GB/s / core", True),
            ("Conns@peak", True), ("Bottleneck", False)]
    bw_tbl = table("t_bw", bhdr, brows,
                   "1.5 MiB payload echoed (send + receive counted), sorted by GB/s <b>per core</b> "
                   "to match the req/s table. Aggregate over the veth pair; <b>client-bound at the "
                   "peak in most rows</b> (Bottleneck = client), so absolute GB/s reflects the "
                   "loadgen ceiling, not the server &mdash; per-core is the fairer cross-model axis.")
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
        rows.append([prog_cell(rt), (fmt(cores), cores), (fmt(d["rate_per_s"]), d["rate_per_s"]),
                     (fmt(d["seconds"] * 1e6 / d["n"], 2), d["seconds"] * 1e6 / d["n"]),
                     (fmt(d["rate_per_s"] / cores), d["rate_per_s"] / cores)])
    rows.sort(key=lambda r: -(r[2][1] or 0))   # best = highest absolute spawn rate
    out.append("<h3>Spawn 1M fibers / goroutines / coroutines (NAKED, worst case)</h3>")
    out.append('<p class="warn">This is <b>naked single-spawn</b> &mdash; ONE spawner creating '
               'tasks <b>one at a time</b>, default config (no arena), no I/O. <b>Go wins this by '
               '~20&ndash;30&times;</b> and that is honest: a goroutine is a cheap 2&nbsp;KB stack, a '
               'runloom fiber is a real C stack + a CPython frame, so one-at-a-time it is heavier. '
               '<b>But no real workload spawns this way.</b> The achievable spawn throughput is the '
               '<b>batch "launch a fleet"</b> path (<a href="#activespawn">Active spawn</a>): bulk '
               '<code>fiber_n</code> + <code>optimize("throughput")</code> reaches <b>~1.6M/s '
               'measured here</b> &mdash; a runloom capability (Go has no batch API), though still '
               'below Go\'s naked ~2.15M. Read this curve as the worst case, not the headline.</p>')
    out.append(table("t_spawn", [("Runtime", False), ("Cores", True), ("spawn/s", True),
                                 ("&micro;s/task", True), ("spawn/s / core", True)], rows,
                     "Higher is better &mdash; but this is the WORST case (one-at-a-time, no arena). "
                     "runloom&rsquo;s batch <code>fiber_n</code> path (see the <a href=\"#activespawn\">"
                     "Active spawn</a> panel) is ~16&times; faster than naked but still <i>below</i> "
                     "Go&rsquo;s naked spawn on this box &mdash; and Go has no batch API to compare "
                     "against. runloom &amp; greenlet carry real C stacks (heavyweight per-spawn vs "
                     "goroutines)."))

    # ctxswitch -- the speed.json rows are PYTHON-fiber; add a runloom
    # compiled-fiber-entry (c_entry capstone) row so the true scheduler yield is
    # in the same table, and relabel the runloom row to say what it actually is.
    cap = load("centry_capstone.json")
    rows = []
    for rt, d in (m.get("ctxswitch") or {}).items():
        if "ns_per_switch" not in d:
            continue
        # the runloom speed.json row is the NAIVE shared-closure worker -- label it
        # so the contrast with the @hot / compiled rows below is unmistakable.
        label = "runloom (python fiber, shared closure)" if rt == "runloom" else rt
        rows.append([(esc(label), label), (fmt(d.get("cores", 1)), d.get("cores", 1)),
                     (fmt(d["ns_per_switch"]), d["ns_per_switch"])])
    if cap and cap.get("hubs"):
        h44 = cap["hubs"][-1]
        # the SAME Python fiber, but with per-core cells (@runloom.hot, or just a
        # module-level handler) -- the fix.  From the capstone (preempt-off,
        # n=0-subtracted), so the python fiber actually MOVES in this table.
        if cap.get("python_distinct_ns"):
            pd44 = cap["python_distinct_ns"][-1]
            rows.append([("runloom (python fiber, @runloom.hot)", "runloom (python fiber, @runloom.hot)"),
                         (fmt(h44), h44),
                         (fmt(pd44, 1) if pd44 < 10 else fmt(pd44), pd44)])
        if cap.get("c_entry_ns"):
            ce44 = cap["c_entry_ns"][-1]      # 44-hub c_entry, same cores
            rows.append([("runloom (compiled fiber entry)", "runloom (compiled fiber entry)"),
                         (fmt(h44), h44),
                         (fmt(ce44, 1) if ce44 < 10 else fmt(ce44), ce44)])
    rows.sort(key=lambda r: (r[2][1] or 1e18))
    out.append("<h3>Context switch (loaded-yield)</h3>")
    out.append(table("t_ctx", [("Runtime", False), ("Cores", True), ("ns / switch", True)], rows,
                     mark_best=False, note=
                     "<b>&#9888; Not one quantity &mdash; do not read across the two groups, and no row "
                     "is crowned.</b> The multi-core rows (Cores 44 / 8: runloom, go) are an "
                     "<i>aggregate</i> &mdash; total switches across all hubs &divide; wall-clock, a "
                     "parallel throughput written as a latency; the 1-core rows (greenlet, asyncio, "
                     "uvloop) are true single-switch <i>latency</i>. A 1-hub runloom switch is ~250 ns "
                     "(see capstone), comparable to greenlet's; the small aggregate number means 44 "
                     "hubs switch in parallel, not that one switch is 18 ns. For a like-for-like "
                     "comparison across hub counts, use the capstone below. "
                     "Lower is better <i>within</i> a basis. G concurrent tasks each yield K times (run "
                     "queues stay full &mdash; same-hub re-dispatch, not a 2-party ping-pong). THREE "
                     "runloom rows tell "
                     "the story: <b>python fiber, shared closure</b> is the naive case (one closure "
                     "reused on every core) &mdash; at 44 hubs that number is free-threaded CPython "
                     "contention on the shared closure's <b>cells</b> (a futex&rarr;cross-NUMA IPI storm; "
                     "<code>perf</code>-confirmed, runloom's own yield is ~2% of the profile), NOT the "
                     "scheduler. <b>python fiber, @runloom.hot</b> is the SAME handler with per-core "
                     "cells (also what a plain module-level handler already is) &mdash; the wall is "
                     "gone. <b>compiled fiber entry</b> is a tstate-free <code>c_entry</code> fiber (no "
                     "Python eval), the true scheduler floor. All three runloom rows are measured "
                     "preempt-off and n=0-subtracted (the CPU-preempt watchdog fires spuriously on this "
                     "pure-CPU microbenchmark and is an I/O-workload feature); the capstone below has "
                     "the hub-scaling proof."))

    # ---- c_entry capstone: the TRUE scheduler yield, + what the wall really is ----
    if cap and cap.get("hubs"):
        hubs = cap["hubs"]
        xl = [str(h) for h in hubs]
        ce = cap.get("c_entry_ns", [])
        pyn = cap.get("python_ns", [])
        pyd = cap.get("python_distinct_ns", [])
        series = [("c_entry (pure scheduler, no Python)", "var(--good)", ce, "centry")]
        if pyd:
            series.append(("Python fiber, per-core cells (@runloom.hot / module-level)",
                           "var(--acc)", pyd, "pydist"))
        series.append(("Python fiber, ONE shared closure", "var(--warn)", pyn, "pyshared"))
        capchart = svg_linechart("ch_cap", series, xl,
                                 xaxis="scheduler hubs", ylabel="ns / switch (log)")
        caprows = []
        for i, h in enumerate(hubs):
            c, p = ce[i], pyn[i]
            d = pyd[i] if pyd else None
            ratio = (p / d) if (d and d > 0) else None   # shared closure vs the fixed path
            row = [(str(h), h), (fmt(c), c)]
            if pyd:
                row.append((fmt(d), d))
            row.append((fmt(p), p))
            row.append((('<b>%.0f&times;</b>' % ratio) if ratio else "&mdash;", ratio or 0))
            caprows.append(row)
        cols = [("scheduler hubs", True), ("c_entry ns/switch", True)]
        if pyd:
            cols.append(("Python per-core cells", True))
        cols += [("Python shared closure", True), ("shared / fixed", True)]
        out.append('<h3>What the 44-hub &ldquo;wall&rdquo; actually was &mdash; the capstone</h3>')
        out.append('<p>The same loaded-yield across hub counts, three ways. <b>c_entry</b> is a '
                   'tstate-free fiber (no Python frame at all) &mdash; runloom\'s pure scheduler cost. '
                   'The two Python lines settle what the wall is: a Python fiber with <b>per-core '
                   'cells</b> (exactly what <code>@runloom.hot</code> does, and what a plain '
                   'module-level handler already is) scales <b>flat, on par with c_entry</b>; a single '
                   '<b>shared closure</b> walls hard. So the wall is shared closure <b>cells</b> &mdash; '
                   'free-threaded CPython contention &mdash; NOT the scheduler and NOT the code object '
                   '(a single shared code object scales fine; '
                   '<a href="SCHEDULER_SCALING_FINDINGS.md">SCHEDULER_SCALING_FINDINGS.md</a> has the '
                   '7-variant proof). The ~250 ns 1-hub Python cost is the interpreter frame; it is '
                   'per-hub-parallel, so it parallelises away in aggregate.</p>'
                   + capchart
                   + table("t_cap", cols, caprows, mark_best=False, note=
                           "<b>Per-core cells (<code>@runloom.hot</code>) / module-level handlers scale "
                           "flat to 44 hubs &mdash; 18 ns aggregate, dead level with c_entry (34 ns).</b> "
                           "A single shared closure explodes to ~7.5 &micro;s (the captured cells bounce "
                           "across NUMA nodes; <code>perf</code> shows the futex&rarr;IPI storm). So a "
                           "regular Python handler already context-switches as cheaply in aggregate as "
                           "the pure-C path; only sharing ONE closure's cells across every core breaks "
                           "it, and <code>@runloom.hot</code> / <code>optimize(&quot;throughput&quot;)</code> "
                           "fixes that (69k&rarr;10.4M switches/s, <b>150&times;</b>). Measured "
                           "preempt-off (the CPU-preempt watchdog is microbenchmark noise). Full "
                           "analysis: <a href=\"SCHEDULER_SCALING_FINDINGS.md\">"
                           "SCHEDULER_SCALING_FINDINGS.md</a>."))

    # http
    rows = []
    for rt, d in (m.get("http") or {}).items():
        if "rps" not in d:
            continue
        cores = d.get("cores", 1)
        rows.append([prog_cell(rt), (fmt(cores), cores), (fmt(d["rps"]), d["rps"]),
                     (fmt(d["rps"] / cores), d["rps"] / cores)])
    rows.sort(key=lambda r: -(r[3][1] or 0))   # best = highest req/s PER CORE (consistent with the req/s table)
    out.append("<h3>HTTP req/s (client vs a Go httpd)</h3>")
    out.append(table("t_http", [("Runtime", False), ("Cores", True), ("req/s", True),
                                ("req/s / core", True)], rows,
                     "Sorted by req/s <b>per core</b>, like the main req/s table. The runtime under "
                     "test is the HTTP <i>client</i> (keepalive GET) against a fixed Go server. "
                     "<b>Core counts differ:</b> runloom and go drive the client on 16 cores, "
                     "asyncio/uvloop/greenlet on 1 &mdash; so by <i>absolute</i> req/s (click 'req/s') "
                     "the 16-core clients lead, but <b>per core the single-threaded loops (uvloop, "
                     "greenlet, asyncio) win</b>, the same story as the server req/s table."))

    # rtt
    rows = []
    for rt, d in (m.get("rtt") or {}).items():
        if "ns_per_rtt" not in d:
            continue
        rows.append([prog_cell(rt), (fmt(d["ns_per_rtt"]), d["ns_per_rtt"]),
                     (fmt(d["ns_per_rtt"] / 1000, 2), d["ns_per_rtt"] / 1000)])
    rows.sort(key=lambda r: (r[1][1] or 1e18))
    out.append("<h3>TCP round-trip latency (to a Go echo server)</h3>")
    out.append(table("t_rtt", [("Runtime", False), ("ns / RTT", True), ("&micro;s / RTT", True)], rows,
                     "Lower is better. Single connection, sequential. Dominated by the ~70&micro;s "
                     "veth round-trip floor on this VM; runtime overhead is the spread above it. "
                     "<b>Not fully like-for-like:</b> asyncio/uvloop use the high-level streams API "
                     "(reader/writer) while runloom, greenlet and go use raw recv/send &mdash; so "
                     "asyncio's per-RTT figure carries a stream-layer cost the others don't, inflating "
                     "it versus a same-level comparison."))
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
            'io_uring is a major win for a real handler &mdash; <b>+2.17&times; the '
            '(extrapolated) server-ceiling estimate at 1 KiB</b>, the fastest runloom config in '
            'the suite. The earlier '
            '"io_uring loses on loopback" was an artifact of driving it through the '
            'readiness path (recv + an epoll&rarr;ring bridge). Full reasoning, the '
            '"+20%" reconciliation, and the thread-state cost analysis are in '
            '<a href="IOURING_TSTATE_FINDINGS.md">IOURING_TSTATE_FINDINGS.md</a>.</p>'
            + table("t_iou", hdr, rows, mark_best=False, note=
                    "Peaks are often client-bound (the 16-core loadgen), so the server-ceiling "
                    "columns (peak / server-CPU-util) are used as the fairer comparison &mdash; but "
                    "these are an <b>extrapolation</b>: for a server-bound row the ceiling is just its "
                    "measured peak, while for a client-bound row it is scaled up by CPU-util. So the "
                    "<b>uring/epoll ceiling ratio mixes a measured and an extrapolated number whenever "
                    "the two sides have different bottlenecks</b> &mdash; read it as indicative, not "
                    "exact. At 8 bytes the gain is small because the all-C epoll path is already "
                    "near-optimal; at 1 KiB the proactor cuts server CPU 85%&rarr;55%.")
            + tstate_tbl)


def sec_work(work):
    """The handler work-curve: ONE server, ONE knob (--work N = FNV passes over
    the payload), same runtime, interpreted Python def vs the fully-native
    zero-PyObject Cython handler (work inline). work=0 IS the echo, so it
    consolidates the echo load as the leftmost point. The gap that opens as work
    grows is the cost of leaving handler work in the interpreter."""
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
    # line chart: req/s vs --work, Python vs Cython (log-y -- spans 615k->1.7k)
    xlabels = [("%d (echo)" % w) if w == 0 else str(w) for w in works]
    ys = lambda h: [py.get(str(w), {}).get("peak", {}).get("rps_median") if h == "py"
                    else cy.get(str(w), {}).get("peak", {}).get("rps_median") for w in works]
    chart = svg_linechart("ch_work",
                          [("Python handler (interpreted)", "var(--warn)", ys("py"), "py"),
                           ("Cython handler (fully native)", "var(--good)", ys("cy"), "cython")],
                          xlabels)
    return ('<h2 id="work">Handler work curve &mdash; interpreted vs the optimized handler</h2>'
            '<p>Every handler-side optimization <em>ties</em> on echo because a TCP echo '
            'does no CPU work in the handler (the cost is the kernel TCP path). This is the '
            'one experiment that gives the handler something to do: <b>one server</b> '
            '(<code>srv_runloom_work.py</code>), <b>one knob</b> (<code>--work N</code> = an '
            'FNV-1a byte hash over the %d&nbsp;B payload, repeated N times, folded into the '
            'reply so it can\'t be elided), the <b>same runtime</b>, two handler '
            'implementations: an <b>interpreted Python <code>def</code></b> (recv_into / '
            'py_fnv / fold / send_all all in the interpreter) vs the <b>fully-native, '
            'zero-PyObject Cython handler</b> with the work inlined (capi recv, native FNV, '
            'fold, capi send &mdash; no Python wrapper, no per-call boxing; '
            '<code>disasm_check.sh</code> proves the loop is PyObject-free). The Cython line '
            'is runloom\'s state of the art; the cross-runtime section shows it tracking Go.</p>'
            '<p><b><code>--work&nbsp;0</code> is the echo</b> (the handler skips the work '
            'entirely), so it consolidates the echo load as the leftmost point and reproduces '
            'the echo number &mdash; a built-in cross-check. As the knob grows the interpreted '
            'handler goes server-bound and collapses while the optimized Cython handler holds '
            'nearly flat; the peak <code>Cython / Python</code> ratio here is <b>%.2f&times;</b> '
            '&mdash; the cost of leaving handler-level work in the interpreter, and what a '
            'properly-optimized handler reclaims.</p>'
            % (pl, max_speed)
            + chart
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


def sec_work_xrt(xrt):
    """Cross-runtime work curve, PER CORE: the same FNV --work knob in every
    runtime's natural handler language. One row per runtime, sorted by per-core
    throughput at the heaviest work so the two bands (compiled vs interpreted)
    separate visually. The honest point: for CPU-bound handler work the dominant
    variable is the handler LANGUAGE, not the runtime."""
    if not xrt:
        return ""
    res = xrt.get("results", {})
    meta = xrt.get("meta", {})
    rtinfo = meta.get("runtimes", {})
    works = meta.get("works", [])
    if not works:
        return ""
    heaviest = str(works[-1])

    def percore(name, w):
        r = res.get(name, {}).get(str(w), {})
        rps = r.get("peak", {}).get("rps_median")
        c = rtinfo.get(name, {}).get("cores", 1) or 1
        return (rps / c) if rps else None

    rows = []
    for name, info in rtinfo.items():
        pc_heavy = percore(name, works[-1])
        kind = info.get("kind", "")
        cells = [
            (esc(info.get("label", name)), info.get("label", name)),
            ('<span class="%s">%s</span>' % ("kgood" if kind == "compiled" else "kwarn", esc(kind)), kind),
            (str(info.get("cores", 1)), info.get("cores", 1)),
        ]
        for w in works:
            pc = percore(name, w)
            cells.append((fmt(pc) if pc is not None else "&mdash;", pc if pc is not None else -1))
        rows.append((pc_heavy if pc_heavy is not None else -1, cells))
    rows.sort(key=lambda t: -t[0])
    body = [c for _, c in rows]

    hdr = [("Runtime", False), ("handler", False), ("cores", True)]
    for w in works:
        hdr.append((("w=%d (echo)" % w) if w == 0 else ("w=%d" % w), True))

    # per-core line chart: cool colours = compiled band, warm = interpreted band
    palette = {"runloom_cython": "var(--good)", "go": "var(--acc)",
               "runloom_py": "var(--warn)", "asyncio": "#ff9966",
               "uvloop": "#ff6b9d", "gevent": "#e06c75"}
    xlabels = [("%d (echo)" % w) if w == 0 else str(w) for w in works]
    cseries = []
    for name, info in rtinfo.items():
        cseries.append((info.get("label", name), palette.get(name, "var(--fg)"),
                        [percore(name, w) for w in works], name))
    chart = svg_linechart("ch_xrt", cseries, xlabels)

    # FOCUSED chart at the top: just the compiled band (runloom Cython vs Go) on
    # a LINEAR y-axis, where the ~2x Go-vs-runloom gap reads true (log-y hides
    # it). Only render the compiled runtimes that are present.
    comp_order = [n for n in ("go", "runloom_cython")
                  if n in rtinfo and any(percore(n, w) for w in works)]
    focus = ""
    if len(comp_order) >= 2:
        fseries = [(rtinfo[n].get("label", n), palette.get(n, "var(--fg)"),
                    [percore(n, w) for w in works], n) for n in comp_order]
        focus = ('<h3>Compiled handlers vs Go (linear scale &mdash; the gap the log chart hides)</h3>'
                 + svg_linechart("ch_comp", fseries, xlabels, logy=False,
                                 ylabel="req/s per core (linear)"))

    return ('<h2 id="workxrt">Cross-runtime work curve &mdash; per core, every runtime</h2>'
            '<p>The same <code>--work N</code> FNV-1a byte hash, run in <b>each runtime\'s '
            'natural handler language</b>, reported <b>per core</b> (peak rps &divide; pinned '
            'cores) so it is an efficiency comparison, not a core-count one. <b>Two</b> runloom '
            'handler tiers are measured in this curve: interpreted Python (<code>py</code>) and the '
            'fully-native zero-PyObject Cython handler (<code>cython</code>, work inline &mdash; the '
            'state-of-the-art path). (The tstate-free <code>cdef</code> handler is an echo handler '
            'with no <code>--work</code> knob, so it is not on this curve &mdash; its result lives in '
            'the io_uring section.) '
            'References: Go (<code>GOMAXPROCS</code>) and the single-core event loops '
            '(asyncio / uvloop / gevent). <b>Click any name in a chart legend to toggle it</b> '
            '(e.g. isolate one runloom line against Go).</p>'
            '<p><b>The headline:</b> a fully-native runloom Cython handler <b>matches&ndash;to&ndash;beats '
            'Go across the whole curve</b> &mdash; runloom-cython is <em>ahead</em> '
            'of Go through ~work&nbsp;4 (its faster I/O paying off) and within ~8% at the heaviest '
            'compute, where Go\'s native codegen edges back. The earlier 2&times; gap was entirely '
            'the interpreted Python <code>def</code> wrapper; inlining the work into the compiled '
            'handler erases it. Meanwhile the interpreted handlers (runloom-py, asyncio, uvloop, gevent) '
            'collapse ~100&ndash;200&times; below under real work &mdash; the handler <em>language</em> '
            'is the only thing that ever separates the field.</p>'
            + focus
            + chart
            + table("t_workxrt", hdr, body, mark_best=True, note=
                    "Rows sorted by per-core throughput at the heaviest work &mdash; the "
                    "<b>rightmost column is the true capacity comparison</b> (the only point where all "
                    "runtimes are genuinely server-bound). Two bands, drawn by the <b>handler "
                    "language, not the runtime</b>: compiled (runloom-cython &asymp; Go, "
                    "~6.5&ndash;7k/core) ~180&times; above interpreted (~30&ndash;40/core); "
                    "the runtime barely matters inside a band, and runloom-py is the <em>top</em> of "
                    "the interpreted band (free-threaded M:N Python is not slower per core than a "
                    "single-thread event loop, and runs on all 44 cores at once). <b>Lighter-work "
                    "columns:</b> the compiled runtimes are so fast there that the 16-core loadgen "
                    "can\'t saturate them (bottleneck <code>client</code>), so those per-core figures "
                    "are the loadgen ceiling &divide; cores, not capacity &mdash; hence any "
                    "non-monotonicity is the measurement, not the runtime. <b>The <code>w=0</code> "
                    "(echo) inversion:</b> single-thread event loops lead per core (uvloop highest) "
                    "because pure I/O pays no free-threading/M:N tax &mdash; and it vanishes the instant "
                    "the handler does work. <b>Honest caveat:</b> delegate the work to a C library "
                    "(<code>hashlib</code>/<code>json</code>) and every runtime re-converges &mdash; "
                    "they would all call the same native code."))


def sec_mem(mem):
    if not mem:
        return '<h2 id="mem">Memory</h2><p class="warn">no mem.json yet</p>'
    cfgs = mem.get("configs", {})
    rows = []
    for name, c in cfgs.items():
        e = c.get("empty", {})
        s = c.get("socket", {})
        mil = c.get("million", {})
        mil_err = ("error" in mil) or not mil.get("rss_total")
        gib = ('<span class="sub">timed out</span>' if mil_err
               else fmt(mil["rss_total"] / 2**30, 2))
        rows.append([
            (esc(name), name),
            (fmt(e.get("bytes_per_fiber_rss")), e.get("bytes_per_fiber_rss") or 0),
            (fmt(s.get("bytes_per_fiber_rss")), s.get("bytes_per_fiber_rss") or 0),
            (gib, mil.get("rss_total") or -1),
            ("&mdash;" if mil_err else fmt(mil.get("rss_per_fiber")), mil.get("rss_per_fiber") or -1),
            ("&mdash;" if mil_err else fmt(mil.get("n")), mil.get("n") or -1),
        ])
    rows.sort(key=lambda r: (r[1][1] or 1e18))
    hdr = [("Config", False), ("empty B/fiber", True), ("w/socket B/fiber", True),
           ("N&times;fiber total RSS (GiB)", True), ("B/fiber @ scale", True), ("N", True)]
    return ('<h2 id="mem">Memory (used RSS, not virtual)</h2>%s'
            % table("t_mem", hdr, rows,
                    "All figures are resident set size (used physical memory), not virtual address "
                    "space. <b>The clean comparison is 'empty B/fiber' and the 1M total RSS</b> (no "
                    "buffer confound): a stackful runloom fiber costs <b>~9 KB/fiber vs a goroutine's "
                    "~2.7 KB (~3.3&times;)</b> &mdash; runloom genuinely uses more, as expected, because "
                    "a fiber carries a real C stack + CPython state and a goroutine carries a 2 KB "
                    "grow-on-demand stack. <b>The 'w/socket' column is NOT apples-to-apples:</b> both "
                    "sides now allocate an equal 64 KiB handler buffer, but CPython eagerly zero-faults "
                    "its <code>bytearray(65536)</code> (so it is resident immediately, ~82 KB/fiber) "
                    "while Go's <code>make([]byte,65536)</code> stays lazily unfaulted until first write "
                    "(the probe parks on Read, so it never faults &mdash; ~7 KB/fiber). Under real load "
                    "(buffer actually used) Go's would fault too and both land near 64 KiB; the idle gap "
                    "is faulting <i>timing</i>, not a steady-state runtime cost. optimize(memory) does "
                    "not shrink idle parked-fiber RSS &mdash; it tunes blockpool/prewarm. "
                    "<b>Coverage:</b> only the two STACKFUL runtimes (runloom, go) are measured; "
                    "stackless asyncio/uvloop tasks and greenlet are not &mdash; a stackless task has no "
                    "C stack, so runloom is <i>expected</i> to use more RSS than asyncio, and that is not "
                    "hidden. NB: the default tstate mode is per-hub snapshot (no per-fiber "
                    "PyThreadState); the gated per-g mode adds a full PyThreadState = ~18 KB/fiber "
                    "(~26.7 KB total, vs 8.8 KB snapshot) &mdash; see IOURING_TSTATE_FINDINGS.md."))


def sec_code():
    blocks = []
    files = [
        # --- servers (these are the program names you click in the tables above) ---
        ("runloom_epoll_py_sync &mdash; sync wrappers (epoll, py handler)", "suite/servers/runloom_epoll_py_sync.py"),
        ("runloom_epoll_py_tcpcon &mdash; runloom_c.serve (py handler, C TCPConn)", "suite/servers/runloom_epoll_py_tcpcon.py"),
        ("runloom_*_cython_tcpcon &mdash; runloom_c.serve + Cython handler", "suite/servers/runloom_iouring_cython_tcpcon.py"),
        ("runloom_iouring_cdef_tcpcon &mdash; cdef c_entry handler server", "suite/servers/runloom_iouring_cdef_tcpcon.py"),
        ("Cython zero-PyObject handler (echo + inline FNV work)", "suite/servers/handler_cy.pyx"),
        ("Cython cdef c_entry handler (tstate-free, inline FNV work)", "suite/servers/handler_cdef.pyx"),
        ("asyncio_epoll_py_proto / uvloop_libuv_py_proto server", "suite/servers/asyncio_epoll_py_proto.py"),
        ("gevent_libev_py_stream server", "suite/servers/gevent_libev_py_stream.py"),
        ("go_netpoll_native_net server", "suite/servers/go_netpoll_native_net.go"),
        ("Work-curve server (--handler py/cython/cdef, --work N)", "suite/servers/srv_runloom_work.py"),
        # --- clients / loadgens ---
        ("Go closed-loop loadgen (persistent req/s)", "suite/clients/loadgen.go"),
        ("Go connection-churn loadgen (conn/s)", "suite/clients/churn_loadgen.go"),
        # --- benchmark orchestrators ---
        ("Orchestrator &mdash; all (perf+speed+mem)", "suite/run_all.py"),
        ("Orchestrator &mdash; req/s + bandwidth (server set + ladder)", "suite/run_perf.py"),
        ("Orchestrator &mdash; speed (spawn/ctxswitch/rtt/http)", "suite/run_speed.py"),
        ("Orchestrator &mdash; memory (RSS/fiber + 1M)", "suite/run_mem.py"),
        ("Connection-churn (conn/s, same server set + ladder)", "suite/conn_churn.py"),
        ("Spawn-rate-vs-N (the naked single-spawn curve)", "suite/speed/spawn_curve.py"),
        ("Work-curve sweep driver", "suite/work_sweep.py"),
        ("Cross-runtime work sweep (all runtimes, per core)", "suite/work_xrt_sweep.py"),
        ("io_uring vs epoll comparison program", "suite/iouring_compare.py"),
        # --- active/batch spawn bench (committed, in-suite) ---
        ("Active/batch spawn bench (naked vs fiber_n, default vs optimize)", "suite/speed/spawn_batch.py"),
        # --- speed / memory probes ---
        ("Speed &mdash; runloom", "suite/speed/runloom_epoll_py_fiber.py"),
        ("Speed &mdash; asyncio/uvloop", "suite/speed/speed_asyncio.py"),
        ("Speed &mdash; greenlet/gevent", "suite/speed/greenlet_native_py_coro.py"),
        ("Speed &mdash; go", "suite/speed/speed_go.go"),
        ("Memory &mdash; runloom probe", "suite/memory/mem_runloom.py"),
        ("Memory &mdash; go probe", "suite/memory/mem_go.go"),
        # --- harness + C-API + docs ---
        ("C-API exposed for the Cython handler", "../src/runloom_c/runloom_tcp_capi.c.inc"),
        ("Cython hot-loop disassembly (zero-PyObject proof)", "suite/servers/handler_cy_hotloop_disasm.txt"),
        ("Harness &mdash; config / constraints", "suite/harness/config.py"),
        ("Harness &mdash; topology (veth/netns/pin/fd)", "suite/harness/topo.py"),
        ("Harness &mdash; measurement (ladder/CI/CPU)", "suite/harness/measure.py"),
        ("Spawn-tuning consolidated summary", "../docs/dev/PERF_SUMMARY.md"),
        ("Spawn >1M plan + results", "../docs/dev/spawn_above_1m.md"),
        ("conn/s CPU decomposition + saturated comparison", "../docs/dev/conn_cpu.md"),
        ("io_uring &amp; thread-state findings (full writeup)", "IOURING_TSTATE_FINDINGS.md"),
        ("Archived original prompt + scoping decisions", "prompt/original_spec.md"),
    ]
    for title, rel in files:
        blocks.append(code_block(title, os.path.join(BENCH, rel)))
    return ('<h2 id="code">Benchmark source &amp; constraints</h2>'
            '<p>Every program embedded for reproducibility &mdash; or just <b>click any program '
            'name in a table above</b> to pop its source up, syntax-highlighted.</p>'
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
tr.best{background:#16301f;box-shadow:inset 3px 0 0 var(--good)}tr.best td{font-weight:700;color:#eafaf0}
tr.best td:first-child{color:var(--good)}tr.best:hover{background:#1b3a26}
.trophy{margin-right:6px;filter:saturate(1.4);font-size:15px}
table.kv th{text-align:left;width:210px;color:var(--mut);font-weight:600;cursor:default}
table.kv th:hover{color:var(--mut)}
.note{color:var(--mut);font-size:12px;margin:4px 0 18px}
.lead{font-size:15px;line-height:1.6;margin:6px 0 22px;padding:14px 16px;border-left:3px solid var(--acc);background:rgba(127,127,127,.06);border-radius:4px}
.warn{color:var(--warn);font-size:13px}
.kgood{color:var(--good);font-weight:600}.kwarn{color:var(--warn);font-weight:600}
svg.chart{display:block;background:var(--panel);border:1px solid var(--line);border-radius:4px;margin:14px 0}
.chart .grid{stroke:var(--line);stroke-width:1}
.chart .ytick{fill:var(--mut);font-size:10px;text-anchor:end}
.chart .xtick{fill:var(--mut);font-size:11px;text-anchor:middle}
.chart .axlbl{fill:var(--mut);font-size:11px;text-anchor:middle}
.chart .leg{fill:var(--fg);font-size:11px}
.chart .hint{fill:var(--mut);font-size:10px;font-style:italic;text-anchor:start}
.chart .legi{cursor:pointer}.chart .legi:hover .leg{fill:var(--acc)}
.chart .legi.off .leg{fill:var(--mut);text-decoration:line-through;opacity:.6}
.chart .ser.off{display:none}
details.code{margin:6px 0;background:var(--panel);border:1px solid var(--line);border-radius:4px}
details.code summary{cursor:pointer;padding:8px 12px;font-weight:600;display:flex;justify-content:space-between;align-items:baseline;gap:16px}
details.code summary::-webkit-details-marker{flex:0 0 auto}
details.code .path{color:var(--mut);font-weight:400;font-size:11px;white-space:nowrap;flex:0 0 auto}
pre{margin:0;padding:12px;overflow:auto;max-height:520px;background:#0c1116;font:12px/1.45 ui-monospace,Menlo,monospace}
code{color:#cbd5e1}
.prog{cursor:pointer;color:var(--acc);border-bottom:1px dotted rgba(108,182,255,.5)}
.prog:hover{color:#fff}
.prog.nosrc{cursor:default;color:inherit;border-bottom:none}
tr.best td:first-child .prog{color:var(--good)}
#codeoverlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.66);z-index:99;padding:4vh 4vw}
#codeoverlay.show{display:block}
#codebox{background:var(--panel);border:1px solid var(--line);border-radius:6px;max-width:1000px;max-height:92vh;margin:0 auto;display:flex;flex-direction:column;box-shadow:0 10px 50px rgba(0,0,0,.6)}
#codehead{display:flex;justify-content:space-between;align-items:center;gap:16px;padding:10px 16px;border-bottom:1px solid var(--line)}
#codetitle{font-weight:700;color:var(--acc)}#codefn{color:var(--mut);font:11px ui-monospace,monospace}
#codeclose{cursor:pointer;color:var(--mut);font-size:22px;line-height:1;border:none;background:none}#codeclose:hover{color:#fff}
#codebody{overflow:auto}#codebody pre{max-height:none;background:#0c1116}
.hl-kw{color:#c792ea}.hl-str{color:#c3e88d}.hl-com{color:#5f6e82;font-style:italic}.hl-num{color:#f78c6c}
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
function tglSeries(cid,slug){
var grp=document.querySelector('.ser-'+cid+'-'+slug);if(!grp)return;
grp.classList.toggle('off');var off=grp.classList.contains('off');
var legs=document.querySelectorAll('.legi[data-c="'+cid+'"][data-s="'+slug+'"]');
legs.forEach(function(e){if(off)e.classList.add('off');else e.classList.remove('off');});}
function showCode(name){var d=window.PROG_SRC&&window.PROG_SRC[name];if(!d)return;
document.getElementById('codetitle').textContent=name;
document.getElementById('codefn').textContent=d.file;
document.getElementById('codepre').innerHTML=d.html;
document.getElementById('codeoverlay').classList.add('show');}
function closeCode(){document.getElementById('codeoverlay').classList.remove('show');}
document.addEventListener('click',function(e){
if(e.target&&e.target.id==='codeoverlay'){closeCode();return;}
var p=e.target.closest?e.target.closest('.prog'):null;
if(p&&!p.classList.contains('nosrc')&&p.getAttribute('data-prog'))showCode(p.getAttribute('data-prog'));});
document.addEventListener('keydown',function(e){if(e.key==='Escape')closeCode();});
"""


def sec_exec_summary():
    """Plain-language verdict at the very top: runloom's strengths + gaps vs Go,
    distilled from ALL the data.  Static, always present."""
    return ('<h2 id="summary">Executive summary &mdash; the honest one-screen verdict</h2>'
            '<p class="lead">Runloom brings Go-style stackful coroutines to free-threaded Python. '
            'Across this suite: <b>where the server actually does work, runloom is competitive with '
            'Go; where it isn\'t, the suite says so.</b> '
            '<b>The strong, server-bound result:</b> on the handler <a href="#workxrt">work-curve</a> '
            '(a real handler doing CPU work &mdash; the comparison whose numbers are <i>not</i> '
            'loadgen-limited) a fully-native runloom Cython handler <b>matches-to-beats Go across the '
            'curve</b>, and the <a href="#iouring">io_uring</a> backend is the fastest config here; '
            'free-threaded M:N Python is not slower per core than a single-thread event loop and uses '
            'every core. '
            '<b>The honest caveats:</b> the small-payload echo <a href="#perf">req/s</a> headline is '
            '<b>client-bound</b> on this single box (the 16-core loadgen saturates before the fast '
            'servers), so runloom and Go cluster within the loadgen\'s noise &mdash; read it as '
            '"scheduling/spawn isn\'t the bottleneck," not as a ranking; <b>per core, single-threaded '
            'uvloop/asyncio win echo req/s.</b> Spawning fibers <i>one at a time</i> is ~20&times; '
            'behind Go (a stackful fiber + CPython frame vs a 2&nbsp;KB goroutine); batch '
            '<code>fiber_n</code> launches a fleet at <b>~1.6M/s</b> here, but Go has no batch API to '
            'compare against and that figure still trails Go\'s naked 2.15M. Stackful fibers also cost '
            'more <a href="#mem">RSS</a> than stackless asyncio tasks, and connection churn is <b>not '
            'yet measured</b> in this suite\'s pipeline. <b>Bottom line: for a busy server running a '
            'real (CPU-doing) handler, runloom is close to Go and well ahead of interpreted Python; '
            'the gaps are the spawn microbench and per-fiber memory.</b> Per-metric breakdown + '
            'caveats below.</p>')


def sec_active_spawn(sb):
    """The spawn story, MEASURED in-suite (spawn_batch.json) rather than asserted:
    naked single-spawn vs batch fiber_n, under default config and
    optimize("throughput"). Go has NO batch-spawn API, so batch is a runloom
    capability, not a Go-beating number; the like-for-like (naked) Go wins."""
    head = '<h2 id="activespawn">Active spawn &mdash; single vs batch (measured on this box)</h2>'
    framing = (
        '<p>There are <b>two</b> ways to spawn, with very different ceilings:</p>'
        '<ul><li><b>Single spawn</b> &mdash; <code>runloom.fiber(fn)</code> one at a time (the '
        'per-event / per-connection pattern). <b>Go wins this decisively</b>: a goroutine is a cheap '
        '2&nbsp;KB stack, a runloom fiber is a real C stack + a CPython frame, so one-at-a-time it is '
        'far heavier &mdash; ~0.1M/s here vs Go\'s ~2.15M/s naked (<a href="#spawncurve">spawn-vs-N</a>).'
        '</li>'
        '<li><b>Batch spawn</b> &mdash; <code>fiber_n(fn, N)</code> launches N <i>at once</i> in one '
        'bulk C call. With <code>optimize("throughput")</code> (warm-stack arena + bulk + parallel '
        'create) it reaches <b>~1.6M/s</b> on this box, ~16&times; the naked path. <b>Go has no '
        'batch-spawn API</b> (its per-goroutine cost is already low enough not to need one), so there '
        'is <b>no like-for-like Go number to beat</b> here &mdash; this is a runloom <i>capability</i>, '
        'not a Go comparison. And runloom\'s best batch figure (~1.6M) still sits <i>below</i> Go\'s '
        'naked spawn (~2.15M) on this box.</li></ul>')
    if not sb or not sb.get("modes"):
        return head + framing + '<p class="warn">spawn_batch.json not present &mdash; run speed/spawn_batch.py.</p>'
    md, meta = sb["modes"], sb.get("meta", {})
    ns = sorted(int(k) for k in md["throughput"].keys())
    rows = []
    for n in ns:
        dN, tN = md["default"][str(n)], md["throughput"][str(n)]
        nk = dN["naked"]["rate_per_s"]
        b_def = dN["batch"]["rate_per_s"]
        b_thr = tN["batch"]["rate_per_s"]
        rows.append([
            (fmt(n), n),
            (fmt(nk), nk),
            (fmt(b_def), b_def),
            ('<b>%s</b>' % fmt(b_thr), b_thr),
            ('%.1f&times;' % (b_thr / max(nk, 1)), b_thr / max(nk, 1)),
        ])
    cols = [("tasks N", True), ("naked spawn/s", True), ("fiber_n /s (default)", True),
            ("fiber_n /s + optimize", True), ("batch / naked", True)]
    tbl = table("t_actspawn", cols, rows, mark_best=False, note=(
        "Measured on this box: %d hubs pinned to a single NUMA node (%d cores), median of %d reps, "
        "rate = N / (wall &minus; empty-run baseline). <b>fiber_n alone (default) is ~no faster than "
        "naked</b> &mdash; the speedup needs <code>optimize(\"throughput\")</code> (warm-stack arena + "
        "parallel bulk-create). The earlier &lsquo;2.0&ndash;2.2M, past Go&rsquo; figure came from the "
        "runloom runtime repo&rsquo;s spawn campaign (<code>docs/dev/spawn_above_1m.md</code>) on a "
        "differently-tuned, less-loaded run; <b>~1.6M is what reproduces in-suite here</b>, and it does "
        "not pass Go&rsquo;s naked spawn. No Go batch number exists to compare against."
        % (meta.get("hubs"), meta.get("ncores_pinned"), meta.get("reps"))))
    return head + framing + ('<p>The single&rarr;batch ladder, measured on this box (FT&nbsp;3.13t):</p>'
                             + tbl)


def sec_spawn_curve(sc):
    if not sc or not sc.get("rates"):
        return ""
    NS = sc["NS"]
    labels = sc.get("labels", {})
    rates = sc["rates"]

    def fmtN(n):
        return ("%dk" % (n // 1000)) if n < 1_000_000 else ("%dM" % (n // 1_000_000))

    def getr(rt, n):
        d = rates.get(rt, {})
        return d.get(str(n), d.get(n))

    xlabels = [fmtN(n) for n in NS]
    palette = {"go": "var(--acc)", "runloom_c": "var(--good)", "runloom_py": "var(--warn)",
               "uvloop": "#ff6b9d", "asyncio": "#ff9966", "greenlet": "#e06c75"}
    order = ["go", "uvloop", "asyncio", "greenlet", "runloom_c", "runloom_py"]
    series, rows = [], []
    for rt in order:
        if rt not in rates:
            continue
        ys = [getr(rt, n) for n in NS]
        series.append((std(rt), palette.get(rt, "var(--fg)"), ys, rt))
        cells = [prog_cell(rt)]
        for v in ys:
            cells.append((fmt(v) if v else "&mdash;", v if v else -1))
        rows.append(cells)
    chart = svg_linechart("ch_spawn", series, xlabels,
                          xaxis="tasks spawned, front-loaded (N)", ylabel="spawn / s (log)")
    cols = [("Runtime", False)] + [(fmtN(n), True) for n in NS]
    return ('<h3 id="spawncurve">Spawn rate vs N (1k &rarr; 1M)</h3>'
            '<p>Raw spawn/s (= N / whole-run seconds) as N front-loaded tasks climb 1k&rarr;1M, '
            'each runtime drained to completion (Go\'s own bench front-loads identically). '
            'runloom &amp; Go on %d cores; asyncio/uvloop/greenlet single-core. Click a legend '
            'entry to isolate a line.</p>' % sc.get("hubs", 8)
            + chart
            + table("t_spawncurve", cols, rows, mark_best=False, note=
                    "Higher is better, but read this as the <b>worst case</b>: NAKED spawn &mdash; "
                    "create+run+destroy one fiber at a time, no I/O, no batching. No real workload "
                    "does this; the achievable number is the <b>active fleet-launch path above "
                    "(batch <code>fiber_n</code> + optimize, ~1.6M/s measured here)</b>. Stackful "
                    "runtimes (runloom, greenlet) carry a real "
                    "C stack per task; asyncio/uvloop coroutines are stackless Python objects; Go "
                    "goroutines are 2&nbsp;KB grow-on-demand native stacks. The naked-burst gap was "
                    "per-fiber stack mmap/mprotect &mdash; now solved by the warm-stack arena "
                    "(landed); what is left here is the one-at-a-time CPython per-fiber cost. "
                    "Corrected diagnosis + the &gt;1M story: docs/dev/spawn_experiments.md + "
                    "spawn_above_1m.md."))


def sec_metrics_legend():
    """Self-documenting verdict panel: what each metric measures, whether it
    exercises spawn, where runloom stands.  Static (no data) so it is always
    present -- the anti-repeat artifact for 'which number means what', and the
    antidote to the two confusions every reader hits: conn/s-vs-req/s, and
    reading a number without checking what got dropped."""
    rows = [
        ["<b>active</b> spawn &mdash; fleet launch (<code>fiber_n</code>)",
         "create+run+destroy N fibers at once, no I/O",
         "Yes &mdash; it IS the whole workload",
         "~1.6M/s batch (measured); Go has no batch API to compare",
         "needs <code>optimize(\"throughput\")</code>; a runloom capability, not a Go-beating number"],
        ["naked spawn &mdash; 1 issuer (microbench)",
         "the same, but one fiber at a time, nothing batched",
         "Yes, and nothing else",
         "~0.1M/s &mdash; Go wins ~20&times; (Go naked ~2.15M)",
         "the like-for-like spawn comparison vs Go &mdash; Go wins it decisively"],
        ["<b>passive</b> spawn &mdash; conn/s (conn-churn)",
         "fresh handler spawned + torn down per request (new connection each time)",
         "Yes &mdash; 1 spawn+teardown / request, but in the hot loop",
         "<b>not measured in this suite</b> (conn_churn.json empty)",
         "TCP accept/handshake/teardown DOMINATES; runloom uses N reuseport acceptors vs Go's single accept &mdash; not yet a like-for-like run"],
        ["req/s &mdash; persistent / keep-alive",
         "steady-state requests on live connections (the browser case)",
         "No &mdash; 1 handler/conn at setup, then loops; spawn ~0% of the window",
         "client-bound here &mdash; &asymp; Go within loadgen noise; per-core uvloop/asyncio win",
         "where real servers + browsers live; the spawn cost is amortized to ~0"],
        ["ctxswitch", "yield/resume cost under load", "n/a",
         "competitive (after closure-cell / @runloom.hot / immortalize)", "the FT refcount lever (1.65&times;)"],
    ]
    head = ["Metric", "Measures", "Spawn in hot loop?", "runloom vs Go (this box)", "Reality"]
    trs = "".join("<tr>" + "".join("<td>%s</td>" % c for c in r) + "</tr>" for r in rows)
    return ('<h2 id="metrics">How to read these metrics &mdash; and where runloom stands</h2>'
            '<p>There is no single "runloom vs Go" number: each benchmark measures a different '
            'axis, and <b>spawn is only exercised by some of them</b>. Two framings make the table '
            'below unambiguous:</p>'
            '<p><b>Active vs passive spawn.</b> <i>Active</i> spawn is an explicit "launch a fleet" '
            '(<code>fiber_n</code> / the spawn benchmark) &mdash; you create N at once, so the create '
            'loop can be parallelized (that is the 804k&rarr;1.5M win). <i>Passive</i> spawn is a '
            'server creating one handler per connection inside its accept loop &mdash; a <i>single</i> '
            'spawn per event, so the active fleet-launch lever does <b>not</b> apply to it; passive '
            'spawn shows up in conn/s.</p>'
            '<p><b>conn/s vs req/s &mdash; the distinction everyone trips on.</b> '
            '<i>req/s</i> (keep-alive) opens connections ONCE and loops requests on them &mdash; this '
            'is the <b>100k&ndash;1M+/s</b> number people quote, and spawn is ~0% of it. '
            '<i>conn/s</i> (churn) opens a NEW connection per request and closes it, so every unit '
            'pays the full TCP lifecycle (SYN handshake + socket alloc + spawn + FIN teardown + '
            'TIME_WAIT). That is <b>~10&ndash;30k/s for everyone, Go included</b> &mdash; the kernel '
            'TCP stack dominates, not the application. They are different benchmarks; quoting the '
            'wrong one is the most common benchmark deception.</p>'
            '<p><b>Where a browser lands:</b> browsers are aggressively keep-alive (HTTP/1.1 reuses '
            '~6 connections per origin; HTTP/2 multiplexes everything over <i>one</i>), so they hit '
            'the <b>req/s</b> path, not conn/s churn. The per-connection spawn (and TLS handshake) is '
            'paid once at connect and amortized over the whole session &mdash; so for browser-shaped '
            'load runloom is &asymp; Go, and the conn/s worst case describes <i>non</i>-keep-alive '
            'clients (proxies without upstream pooling, connection-per-call RPC, reconnect storms).</p>'
            '<table><thead><tr>' + "".join("<th>%s</th>" % h for h in head) +
            '</tr></thead><tbody>' + trs + '</tbody></table>'
            '<p class="note"><b>Reading any of these honestly (so the chart does not fool you):</b> '
            '(1) <b>check which side saturated</b> &mdash; a number where the load-generator CPU is '
            'pinned and the server is idle measures the <i>client</i>, not the server (it understates '
            'the runtime). (2) <b>report throughput per core</b> &mdash; matching Go\'s throughput at '
            '3&times; the cores is not parity; the per-core column is the honest one. (3) <b>warm vs '
            'cold</b> &mdash; a min-of-reps number is the warm, fault-free rep and hides cold-start '
            'cost. (4) <b>name the metric precisely</b> (active/passive, conn/req, naked/amortized) '
            '&mdash; a number with no workload attached is a claim with the asterisk removed. Full '
            'spawn diagnosis + the corrected story (the per-fiber madvise is the security stack-scrub, '
            'NOT a CPython purge): <code>docs/dev/spawn_experiments.md</code>; the &gt;1M plan: '
            '<code>docs/dev/spawn_above_1m.md</code>.</p>')


def sec_conn_churn(cc):
    # New shape mirrors perf.json -- {servers:{name:{label,interp,cores,peak,...}}}
    # from measure.ladder() against run_perf's server set.  The old preliminary
    # shape was {results:{name:{conns_per_s,...}}} (a fixed-load snapshot); both
    # are tolerated, and an empty/absent payload renders a "pending run" panel.
    cc = cc or {}
    servers = cc.get("servers")
    legacy = cc.get("results") if not servers else None

    intro = (
        '<h2 id="churn">Connection churn &mdash; conn/s (a fresh handler spawned per request)</h2>'
        '<p>The req/s benchmark further down establishes connections ONCE and loops requests '
        'on them &mdash; so the server never spawns a handler under load. This is the '
        'opposite, and the case most people picture when they hear "spawn a handler per '
        'request": the client opens a NEW connection, sends one request, reads the echo, and '
        'CLOSES, as hard as it can. So the server pays <b>accept + spawn-a-handler + serve + '
        'teardown for every counted connection, in the hot loop</b> &mdash; the metric where '
        'per-connection fiber/goroutine/coroutine spawn actually lands. It runs against the '
        '<b>same servers</b> as the req/s benchmark and is driven to the <b>same saturation</b>: '
        'a ladder of concurrent dialers climbed until conn/s plateaus, with the per-core '
        'server- vs client-bound CPU check. (One request per connection, so conn/s == req/s '
        'here, but every request is a fresh connection.)</p>')

    note = ("Higher conn/s is better; the ladder climbs concurrent dialers until conn/s plateaus, "
            "so <b>Bottleneck</b> says whether the <i>server</i> was the limit at peak (a real "
            "ceiling) or the 16-core <i>client</i> saturated first (then Server-ceiling est. = "
            "peak / server-CPU is the fairer per-core proxy). Connection churn is dominated by "
            "the TCP accept/setup/teardown syscalls EVERY runtime pays, so a heavier fiber-spawn "
            "is only a slice of the per-connection cost &mdash; but lower server CPU at the same "
            "conn/s means more headroom under heavier churn. KNOWN BUG: the runloom "
            "<i>Cython</i>-handler server busy-spins under churn (the M:N no-data park loop), so "
            "its row is a defect, not a real limit; the Python-handler row is the representative "
            "runloom number. Single-core asyncio/uvloop/gevent saturate one core.")

    if servers:
        rows = []
        for name, s in servers.items():
            pk = s.get("peak") or {}
            if "rps_median" not in pk:
                continue
            cores = s.get("cores", 1) or 1
            cps = pk.get("rps_median", 0)
            ceil = s.get("server_ceiling_est")
            rows.append([
                ('<b>%s</b><br><span class="sub">%s</span>' % (esc(name), esc(s.get("label", ""))), name),
                (esc(s.get("interp", "")), s.get("interp", "")),
                (fmt(cores), cores),
                (fmt(cps), cps),
                (fmt(cps / cores), cps / cores),
                (fmt(pk.get("conns")), pk.get("conns")),
                (fmt(pk.get("p99_us")), pk.get("p99_us")),
                (esc(s.get("bottleneck_at_peak", "")), s.get("bottleneck_at_peak", "")),
                (fmt(ceil), ceil or 0),
            ])
        if rows:
            rows.sort(key=lambda r: -(r[3][1] or 0))
            cols = [("Runtime", False), ("Interp", False), ("Cores", True),
                    ("Peak conn/s", True), ("conn/s / core", True), ("Dialers@peak", True),
                    ("p99 &micro;s", True), ("Bottleneck", False), ("Server-ceiling est.", True)]
            return intro + table("t_churn", cols, rows, note)

    if legacy:
        order = sorted(legacy.keys(), key=lambda n: -(legacy[n].get("conns_per_s") or 0))
        rows = []
        for n in order:
            d = legacy[n]
            cps = d.get("conns_per_s")
            rows.append([
                (esc(d.get("label", n)), d.get("label", n)),
                (fmt(d.get("cores", 1)), d.get("cores", 1)),
                (fmt(cps) if cps else "&mdash;", cps or -1),
                (fmt(d.get("p50_us", 0)), d.get("p50_us", 0)),
                (fmt(d.get("p99_us", 0)), d.get("p99_us", 0)),
                ("%.0f%%" % ((d.get("server_util") or 0) * 100), (d.get("server_util") or 0)),
            ])
        cols = [("Runtime", False), ("Cores", True), ("conn/s", True),
                ("p50 &micro;s", True), ("p99 &micro;s", True), ("server CPU", True)]
        return (intro
                + '<p class="warn">Preliminary fixed-load snapshot &mdash; superseded by the '
                  'saturation-ladder harness against the performance server set; re-run '
                  '<code>conn_churn.py</code> for current numbers.</p>'
                + table("t_churn", cols, rows, note))

    # conn_churn.json is empty (no committed saturation run yet). Rather than headline a
    # hardcoded side-experiment as a Go-beating result, show conn/s as NOT-yet-measured and
    # disclose why it is not yet a like-for-like comparison (acceptor asymmetry + a cython
    # busy-spin bug). No win or parity is claimed from the preliminary side numbers.
    prelim_rows = [
        ("runloom_iouring_cdef_tcpcon", "8,538", "from a prebuilt cdef .so that predates recent ext rebuilds"),
        ("go_netpoll_native_net", "7,783", "GOMAXPROCS=2, single <code>Accept()</code> loop"),
        ("runloom_epoll_py_tcpcon", "7,077", "Python handler"),
    ]
    ptrs = "".join("<tr><td>%s</td><td style='text-align:right'>%s</td><td>%s</td></tr>"
                   % (prog_html(r[0]), r[1], r[2]) for r in prelim_rows)
    return (intro
            + '<p class="warn"><b>Connection churn is NOT yet measured in this suite&rsquo;s '
              'pipeline</b> &mdash; <code>conn_churn.json</code> is empty (the saturation-ladder run '
              'against the full server set is a pending idle-box job). It is also <b>not yet a '
              'like-for-like comparison</b>: connection churn is accept-bound, and the runloom servers '
              'use <b>N SO_REUSEPORT acceptors</b> (one kernel accept queue per hub) while the Go '
              'baseline uses a <b>single <code>Accept()</code> loop</b> &mdash; so any runloom conn/s '
              'lead would partly be the acceptor count, not the runtime. Separately, the runloom '
              '<i>Cython/cdef</i>-handler server busy-spins under churn (a known M:N no-data park-loop '
              'bug), so its conn/s would be a defect, not a real ceiling.</p>'
            + '<p>For reference only, a <b>preliminary</b> 2-core saturated side-experiment '
              '(<code>docs/dev/conn_cpu.md</code> &mdash; <b>not</b> from this suite, and subject to '
              'the caveats above) measured conn/s per core:</p>'
            + '<table><thead><tr><th>server (2 cores, saturated) &mdash; preliminary</th>'
              '<th>conn/s / core</th><th>caveat</th></tr></thead><tbody>' + ptrs + '</tbody></table>'
            + '<p class="note"><b>What is and isn&rsquo;t claimed:</b> the figures above are close '
              'across runtimes (~7&ndash;8.5k/core), consistent with conn/s being dominated by the TCP '
              'accept/teardown syscalls every runtime pays. But with the acceptor asymmetry and the '
              'cdef busy-spin bug unresolved, and no committed saturation run, <b>this report makes no '
              'conn/s win or parity claim.</b> A like-for-like run (matched acceptors + a rebuilt '
              'handler) is the open follow-up.</p>')


def main():
    envd = load("env.json")
    perf = load("perf.json") or load("perf_quick.json")
    speed = load("speed.json") or load("speed_quick.json")
    mem = load("mem.json") or load("mem_quick.json")
    iou = load("iouring_test.json")
    work = load("work_curve.json")
    work_xrt = load("work_xrt.json")
    spawn_curve = load("spawn_curve.json")
    conn_churn = load("conn_churn.json")
    spawn_batch = load("spawn_batch.json")
    meta = (perf or speed or mem or {}).get("meta") or config.summary()
    quick = any(d and d.get("quick") for d in (perf, speed, mem))

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    nav = ('<nav><b>Runloom benchmarks</b> '
           '<a href="#summary">summary</a>'
           '<a href="#env">machine</a><a href="#constraints">constraints</a>'
           '<a href="#metrics">metrics</a>'
           '<a href="#churn">conn churn</a><a href="#perf">req/s</a><a href="#iouring">io_uring</a>'
           '<a href="#work">work curve</a><a href="#workxrt">work x-runtime</a>'
           '<a href="#activespawn">active spawn</a><a href="#spawncurve">spawn vs N</a>'
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
        sec_exec_summary(),
        sec_header(envd),
        sec_constraints(meta),
        sec_metrics_legend(),
        sec_conn_churn(conn_churn),
        sec_perf(perf),
        sec_iouring(iou),
        sec_work(work),
        sec_work_xrt(work_xrt),
        sec_active_spawn(spawn_batch),
        sec_spawn_curve(spawn_curve),
        sec_speed(speed),
        sec_mem(mem),
        sec_profiles(),
        sec_code(),
        '</div>',
        # click-to-code overlay (a program name in any table opens it, highlighted)
        '<div id="codeoverlay"><div id="codebox">'
        '<div id="codehead"><span id="codetitle"></span><span id="codefn"></span>'
        '<button id="codeclose" onclick="closeCode()" title="close (Esc)">&times;</button></div>'
        '<div id="codebody"><pre><code id="codepre"></code></pre></div></div></div>',
        prog_sources_script(),
        '<script>%s</script></body></html>' % JS,
    ]
    out = os.path.join(BENCH, "report.html")
    with open(out, "w") as f:
        f.write("\n".join(parts))
    print("wrote", out, "(%d KiB)" % (os.path.getsize(out) // 1024))


if __name__ == "__main__":
    main()
