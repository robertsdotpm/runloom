#!/usr/bin/env python3
"""Per-platform big_100 syscall profile -- one standalone report per netpoll
backend, in the style of the original Linux big100_syscall_profile_v2.html but
single-snapshot (no before/after).  Generates, from the committed data dirs:

    big100_syscall_profile_linux.html    (epoll,    strace -c)
    big100_syscall_profile_mac.html      (kqueue,   ktrace / KDEBUG)
    big100_syscall_profile_win.html      (iocp-afd, xperf NT-kernel SYSCALL + symbols)

Each: a summary table + a per-program syscall table (syscall, category, count,
%-of-program), category-coloured, sorted by count.  Run with no args.
"""
import glob, html, os, re

HERE = os.path.dirname(os.path.abspath(__file__))
E = html.escape

# --- categorisation (shared with gen_syscall_backends_html.py); specific-first ---
CATRULES = [
    ("poll/wait", r"epoll|kevent|kqueue|removeiocompletion|waitforworkvia|associatewaitcompletion|iocompletion"),
    ("net",       r"recvmsg|recvfrom|recvmmsg|\brecv|sendmsg|sendto|sendmmsg|\bsend|accept|connect|socket|bind|listen|shutdown|sockopt|getpeername|getsockname|deviceiocontrol|afd|wsa"),
    ("registry",  r"valuekey|openkey|querykey|enumeratekey|createkey|notifychangekey|deletekey|setvaluekey"),
    ("sync",      r"futex|psynch|ulock|semwait|waitforalert|alertthread|keyedevent|waitforsingleobject|waitformultiple|signalandwait|releasemutant|releasesemaphore|setevent|resetevent|clearevent|pulseevent|mutant|waitforgate|releasekeyed"),
    ("yield",     r"sched_yield|yieldexecution|thread_switch"),
    ("time",      r"nanosleep|clock_|gettimeofday|delayexecution|querysystemtime|queryperformance|setitimer|getitimer|timer_|settimer|waitabletimer"),
    ("proc/thr",  r"clone|fork|vfork|execve|posix_spawn|wait4|waitid|^exit|exit_group|bsdthread|workq|createthread|createprocess|createuserprocess|terminateprocess|terminatethread|openprocess|openthread|resumethread|suspendthread|setinformationthread|queryinformationthread|queryinformationprocess|setinformationprocess|informationtoken|impersonate|processortoken|thread_selfid"),
    ("mem",       r"mmap|munmap|mprotect|madvise|\bbrk|mremap|mlock|virtualmemory|mapviewofsection|unmapview|sharedregion|flushvirtual"),
    ("file",      r"read|write|open|close|stat|lseek|fcntl|ioctl|getdents|getdirentries|fsync|fdatasync|access|pread|pwrite|\bdup|pipe|createfile|queryinformationfile|queryattributes|openfile|querydirectory|flushbuffers|queryvolume|fstatat|readlink|unlink|rename|mkdir|ftruncate|getattrlist|fsetattr|createnamedpipe|fscontrol|setinformationfile"),
    ("signal",    r"sigaction|sigprocmask|sigreturn|sigaltstack|sigsuspend|sigpending|pthread_kill|raiseexception|raisehard|\bkill|tgkill|exceptionhandler"),
]
COLOR = {"poll/wait": "#17becf", "net": "#1f77b4", "sync": "#ff7f0e", "yield": "#bcbd22",
         "time": "#7f7f7f", "registry": "#9edae5", "file": "#2ca02c", "mem": "#8c564b",
         "proc/thr": "#9467bd", "signal": "#d62728", "other": "#999999"}
CATS = list(COLOR)


def categorize(name):
    low = name.lower()
    bare = re.sub(r"^(bsc_|msc_|sys_|nt|zw)", "", low)
    for cat, pat in CATRULES:
        if re.search(pat, low) or re.search(pat, bare):
            return cat
    return "other"


def disp(name):
    return re.sub(r"^(BSC_|MSC_|sys_)", "", name)


def parse_strace(path):
    out = {}
    for line in open(path, errors="replace"):
        t = line.split()
        if len(t) >= 5 and re.match(r"^\d", t[0]) and t[-1] != "total":
            try:
                out[t[-1]] = int(t[3])
            except ValueError:
                pass
    return out


def parse_counts(path):
    out = {}
    for line in open(path, errors="replace"):
        m = re.match(r"\s*(\d+)\s+(\S+)", line)
        if m:
            out[m.group(2)] = int(m.group(1))
    return out


CLASS = {
    "p01_tcp_echo": "TCP", "p04_tcp_proxy": "TCP-duplex", "p07_udp_storm": "UDP",
    "p09_dns_stampede": "DNS-offload", "p11_tls_swarm": "TLS", "p16_file_copier": "file-IO",
    "p17_tiny_file_storm": "fs-meta", "p21_sqlite": "C-ext-offload", "p23_jsonl_gzip": "compress",
    "p26_subprocess_echo": "subprocess", "p33_pty": "PTY", "p36_million_sleepers": "timers",
    "p38_cpu_hog_isolation": "CPU", "p40_work_stealing": "scheduler", "p46_immortal": "immortal-g",
    "p54_select_channels": "chan-select", "p56_cancellation_storm": "cancel",
    "p69_traceback_integrity": "interp", "p78_gc_pressure": "GC", "p99_migration_fuzzer": "migration",
    "p111_fork_while_scheduler_active": "fork", "p114_setitimer_storm": "signals",
    "p116_sigchld_storm": "sigchld",
}

PLATFORMS = [
    ("Linux",   "epoll",    "lin_sys", "*.strace", parse_strace,
     "strace -f -c (per-process)",
     "big100_syscall_profile_linux.html"),
    ("macOS",   "kqueue",   "mac_sys", "*.counts", parse_counts,
     "ktrace / KDEBUG (per-process; dtrace's syscall provider is SIP-blocked, KDEBUG is not)",
     "big100_syscall_profile_mac.html"),
    ("Windows", "iocp-afd", "win_sys", "*.counts", parse_counts,
     "xperf NT-kernel SYSCALL flag + MS symbols (system-wide minus an idle baseline; "
     "NtTraceControl, the tracer's own syscall, excluded)",
     "big100_syscall_profile_win.html"),
]
HEAD = {"Linux": "#3fb950", "macOS": "#58a6ff", "Windows": "#d29922"}


def load(ddir, gl, parser):
    out = {}
    for p in glob.glob(os.path.join(HERE, ddir, gl)):
        m = re.match(r"(p\d+\w*?)\.", os.path.basename(p))
        if m:
            out[m.group(1)] = parser(p)
    return out


def pkey(n):
    return int(re.match(r"p(\d+)", n).group(1))


for plat, backend, ddir, gl, parser, method, outfile in PLATFORMS:
    data = load(ddir, gl, parser)
    if plat == "Windows":   # subtract idle baseline
        bpath = os.path.join(HERE, ddir, "BASELINE.counts")
        base = parse_counts(bpath) if os.path.exists(bpath) else {}
        for d in data.values():
            for k in list(d):
                d[k] = max(0, d[k] - base.get(k, 0))
                if d[k] == 0:
                    del d[k]
    progs = sorted(data, key=pkey)
    hc = HEAD[plat]

    P = ["""<!doctype html><meta charset=utf-8><title>big_100 syscall profile -- %s/%s</title>
<style>
 body{font:14px/1.5 system-ui,sans-serif;margin:0;background:#0e1116;color:#e6e6e6}
 .wrap{max-width:1150px;margin:0 auto;padding:24px} h1{font-size:24px}
 h2{font-size:18px;margin-top:32px;border-bottom:1px solid #2a3038;padding-bottom:6px}
 a{color:#58a6ff;text-decoration:none} a:hover{text-decoration:underline}
 .note{background:#161b22;border:1px solid #2a3038;border-radius:8px;padding:12px 16px;margin:14px 0;color:#c9d1d9}
 table{border-collapse:collapse;width:100%%;margin:10px 0;font-size:13px}
 th,td{padding:4px 8px;text-align:right;border-bottom:1px solid #20262e}
 th{color:#8b949e} td.s,th.s{text-align:left} .muted{color:#8b949e}
 .pill{display:inline-block;padding:1px 7px;border-radius:10px;font-size:11px;color:#06121f}
 .bar{height:8px;border-radius:2px;display:inline-block;vertical-align:middle}
 .leg span{margin-right:12px} code{background:#161b22;padding:1px 5px;border-radius:4px}
</style><div class=wrap>
<h1>big_100 &mdash; syscall profile <span class=muted style="font-size:15px">(%s / %s backend)</span></h1>
<div class=note>
Per-program syscall counts for the <b>%s</b> netpoll backend on the latest
origin/main (<code>--hubs 2 --seed 1234</code>).  Captured with <b>%s</b>.
Tracing perturbs timing, so read the <i>mix</i>, not absolute magnitudes.  See
<a href="big100_syscall_backends.html">big100_syscall_backends.html</a> for the
cross-backend (epoll vs kqueue vs iocp-afd) comparison.
</div>
<p class=leg>""" % (plat, backend, plat, backend, backend, method)]
    P.append(" ".join('<span><span class=pill style="background:%s">&nbsp;</span> %s</span>'
                      % (COLOR[c], c) for c in CATS) + "</p>")

    # summary
    P.append('<h2>Summary</h2><table><tr><th class=s>program</th><th class=s>class</th>'
             '<th>total syscalls</th><th>distinct</th><th class=s>dominant category</th>'
             '<th class=s>top syscall</th></tr>')
    for n in progs:
        d = data[n]
        tot = sum(d.values())
        roll = {}
        for k, v in d.items():
            roll[categorize(k)] = roll.get(categorize(k), 0) + v
        domcat = max(roll, key=roll.get) if roll else "-"
        top = max(d, key=d.get) if d else "-"
        P.append('<tr><td class=s><a href="#%s">%s</a></td><td class=s>%s</td>'
                 '<td>%s</td><td>%d</td>'
                 '<td class=s><span class=pill style="background:%s">%s</span> %d%%</td>'
                 '<td class=s>%s <span class=muted>%s</span></td></tr>'
                 % (n, E(n), E(CLASS.get(n, "?")), "{:,}".format(tot), len(d),
                    COLOR.get(domcat, "#999"), domcat,
                    (100 * roll[domcat] // tot) if tot else 0,
                    E(disp(top)), "{:,}".format(d.get(top, 0))))
    P.append("</table>")

    # per-program detail
    for n in progs:
        d = data[n]
        tot = sum(d.values()) or 1
        P.append('<h2 id="%s">%s <span class=muted style="font-size:13px">&mdash; %s</span></h2>'
                 % (n, E(n), E(CLASS.get(n, ""))))
        P.append('<table><tr><th class=s>syscall</th><th class=s>category</th>'
                 '<th>count</th><th>% of prog</th><th></th></tr>')
        items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)
        CAP = 60
        for k, v in items[:CAP]:
            cat = categorize(k)
            pct = 100.0 * v / tot
            P.append('<tr><td class=s>%s</td>'
                     '<td class=s><span class=pill style="background:%s">%s</span></td>'
                     '<td>%s</td><td class=muted>%.1f</td>'
                     '<td class=s><span class=bar style="background:%s;width:%dpx"></span></td></tr>'
                     % (E(disp(k)), COLOR[cat], cat, "{:,}".format(v), pct,
                        COLOR[cat], min(240, int(pct * 2.4) + 1)))
        if len(items) > CAP:
            tail = items[CAP:]
            tc = sum(v for _, v in tail)
            P.append('<tr><td class=s muted>&plus; %d more (long tail)</td><td></td>'
                     '<td class=muted>%s</td><td class=muted>%.1f</td><td></td></tr>'
                     % (len(tail), "{:,}".format(tc), 100.0 * tc / tot))
        P.append("</table>")

    P.append('<p class=muted style="margin-top:28px">gen_syscall_profile.py &mdash; %s/%s, %d programs.</p>'
             % (plat, backend, len(progs)))
    P.append("</div>")
    open(os.path.join(HERE, outfile), "w").write("\n".join(P))
    print("wrote", outfile, "(", len(progs), "programs )")
