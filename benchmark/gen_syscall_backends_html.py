#!/usr/bin/env python3
"""big_100 per-BACKEND syscall profile -- epoll (Linux) vs kqueue (macOS) vs
iocp-afd (Windows), on the latest origin/main, same workload.

Each platform is traced with its native facility (Linux strace -c, macOS ktrace
KDEBUG, Windows xperf NT-kernel SYSCALL + symbols), then every syscall is mapped
into common CATEGORIES so the three netpoll backends are directly comparable --
you can see how epoll_wait vs kevent vs the IOCP completion port show up, where
the locking/wake traffic goes, etc.
"""
import glob, html, os, re

HERE = os.path.dirname(os.path.abspath(__file__))
LIN = os.path.join(HERE, "lin_sys")   # *.strace  (strace -c)
MAC = os.path.join(HERE, "mac_sys")   # *.counts  ("<n> BSC_name")
WIN = os.path.join(HERE, "win_sys")   # *.counts  ("<n> NtName")
OUT = os.path.join(HERE, "big100_syscall_backends.html")
COL = {"Linux": "#3fb950", "macOS": "#58a6ff", "Windows": "#d29922"}

# Ordered category rules: (category, regex) -- first match wins.  Patterns are
# matched case-insensitively against the bare syscall name (prefixes stripped).
# Ordered MORE-SPECIFIC-FIRST so e.g. NtOpenProcess hits proc/thr before file's
# generic "open", and registry keys aren't miscounted as files.
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


def categorize(name):
    low = name.lower()
    bare = re.sub(r"^(bsc_|msc_|sys_|nt|zw)", "", low)   # strip BSC_/Nt/etc.
    for cat, pat in CATRULES:
        if re.search(pat, low) or re.search(pat, bare):
            return cat
    return "other"


def parse_strace(path):
    out = {}
    for line in open(path, errors="replace"):
        t = line.split()
        # % time, seconds, usecs/call, calls, [errors], syscall
        if len(t) >= 5 and re.match(r"^\d", t[0]) and t[-1] != "total":
            try:
                calls = int(t[3]); out[t[-1]] = calls
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


def load(directory, pat, parser):
    out = {}
    for p in glob.glob(os.path.join(directory, pat)):
        base = os.path.basename(p)
        stem = re.match(r"(p\d+\w*?)\.", base)
        nm = stem.group(1) if stem else base
        out[nm] = parser(p)
    return out


def disp(name):
    return re.sub(r"^(BSC_|MSC_|sys_)", "", name)


data = {
    "Linux":   load(LIN, "*.strace", parse_strace),
    "macOS":   load(MAC, "*.counts", parse_counts),
    "Windows": load(WIN, "*.counts", parse_counts),
}

# Windows syscall ETW events carry no process id, so the capture is system-wide;
# subtract an idle BASELINE (captured with nothing running) to strip background
# (registry, idle waits) and leave ~the program's contribution. Floor at 0.
winbase = None
for k in list(data["Windows"].keys()):
    if "BASELINE" in k.upper():
        winbase = data["Windows"].pop(k); break
if winbase:
    for prog, d in data["Windows"].items():
        for k in list(d.keys()):
            d[k] = max(0, d[k] - winbase.get(k, 0))
            if d[k] == 0:
                del d[k]
plats = ["Linux", "macOS", "Windows"]
CATS = ["poll/wait", "net", "sync", "yield", "time", "registry", "file", "mem", "proc/thr", "signal", "other"]
backend = {"Linux": "epoll", "macOS": "kqueue", "Windows": "iocp-afd"}

progs = set()
for pl in plats:
    progs |= set(data[pl])
progs = sorted(progs, key=lambda n: int(re.match(r"p(\d+)", n).group(1)))

E = html.escape
P = []
P.append("""<!doctype html><meta charset=utf-8><title>big_100 syscall profile by backend</title>
<style>
 body{font:14px/1.5 system-ui,sans-serif;margin:0;background:#0e1116;color:#e6e6e6}
 .wrap{max-width:1300px;margin:0 auto;padding:24px} h1{font-size:24px}
 h2{font-size:18px;margin-top:34px;border-bottom:1px solid #2a3038;padding-bottom:6px}
 a{color:#58a6ff;text-decoration:none} a:hover{text-decoration:underline}
 .note{background:#161b22;border:1px solid #2a3038;border-radius:8px;padding:12px 16px;margin:14px 0;color:#c9d1d9}
 table{border-collapse:collapse;font-size:12.5px} th,td{padding:3px 8px;text-align:right;border-bottom:1px solid #20262e}
 th{color:#8b949e} td.s,th.s{text-align:left} .muted{color:#8b949e} .na{color:#6e7681}
 .cols{display:flex;gap:18px;flex-wrap:wrap} .colbox{flex:1;min-width:330px}
 .pill{display:inline-block;padding:1px 8px;border-radius:10px;font-size:11px;color:#06121f;font-weight:600}
 .bar{height:9px;border-radius:3px;display:inline-block;vertical-align:middle}
 code{background:#161b22;padding:1px 5px;border-radius:4px}
</style><div class=wrap>
<h1>big_100 &mdash; syscall profile by netpoll backend</h1>
<div class=note>
Same workload (<code>--hubs 2 --funcs 150 --duration 3</code>) on the latest
origin/main, each platform traced with its native facility:
<b>Linux</b> epoll via <code>strace -c</code>,
<b>macOS</b> kqueue via <code>ktrace</code> (KDEBUG; SIP blocks dtrace, not this),
<b>Windows</b> iocp-afd via <code>xperf</code> (NT-kernel SYSCALL + MS symbols).
Syscalls are bucketed into shared categories so the backends line up; counts are
the per-syscall call totals (Linux excludes parked time).  Each tracer perturbs
timing differently, so read the <i>mix</i> across backends, not raw cross-OS magnitudes.
</div>""")

# legend
P.append('<p>')
for pl in plats:
    n = len(data[pl])
    P.append('<span style="margin-right:16px"><span class=pill style="background:{0}">{1}</span> {2} &middot; {3} progs</span>'
             .format(COL[pl], pl, backend[pl], n))
P.append('</p>')

def catrollup(d):
    roll = {c: 0 for c in CATS}
    for k, v in d.items():
        roll[categorize(k)] += v
    return roll

# ---- category summary across all programs (the headline backend signature) ----
P.append('<h2>Category mix &mdash; summed over all programs</h2>')
P.append('<table><tr><th class=s>category</th>')
for pl in plats:
    P.append('<th><span class=pill style="background:{0}">{1}</span><br>{2}</th>'.format(COL[pl], pl, backend[pl]))
P.append('</tr>')
totals = {pl: {c: 0 for c in CATS} for pl in plats}
for pl in plats:
    for prog in data[pl].values():
        r = catrollup(prog)
        for c in CATS:
            totals[pl][c] += r[c]
for c in CATS:
    P.append('<tr><td class=s>{0}</td>'.format(c))
    for pl in plats:
        tot = sum(totals[pl].values()) or 1
        v = totals[pl][c]
        pct = 100.0 * v / tot
        P.append('<td>{0:,}<span class=muted style="font-size:11px"> {1:.0f}%</span></td>'.format(v, pct))
    P.append('</tr>')
P.append('</table>')

# ---- per-program: 3 columns, category rollup + top syscalls ----
for prog in progs:
    P.append('<h2 id="{0}">{0}</h2>'.format(E(prog)))
    P.append('<div class=cols>')
    for pl in plats:
        d = data[pl].get(prog)
        P.append('<div class=colbox><div><span class=pill style="background:{0}">{1}</span> '
                 '<span class=muted>{2}</span></div>'.format(COL[pl], pl, backend[pl]))
        if not d:
            P.append('<p class=na>&mdash; not captured</p></div>'); continue
        roll = catrollup(d); tot = sum(d.values()) or 1
        P.append('<table style="width:100%;margin-top:6px"><tr><th class=s>category</th><th>calls</th><th>%</th></tr>')
        for c in CATS:
            if roll[c] == 0:
                continue
            P.append('<tr><td class=s>{0}</td><td>{1:,}</td><td class=muted>{2:.0f}</td></tr>'
                     .format(c, roll[c], 100.0 * roll[c] / tot))
        P.append('<tr><td class=s muted>total</td><td>{0:,}</td><td></td></tr></table>'.format(tot))
        # top syscalls
        top = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:8]
        P.append('<table style="width:100%;margin-top:4px"><tr><th class=s>top syscall</th><th>cat</th><th>calls</th></tr>')
        for k, v in top:
            P.append('<tr><td class=s>{0}</td><td class=muted>{1}</td><td>{2:,}</td></tr>'
                     .format(E(disp(k)), categorize(k), v))
        P.append('</table></div>')
    P.append('</div>')

P.append('<p class=muted style="margin-top:30px">gen_syscall_backends_html.py &mdash; '
         + ", ".join("{0} {1}p".format(pl, len(data[pl])) for pl in plats) + '</p>')
P.append("</div>")
open(OUT, "w").write("\n".join(P))
print("wrote", OUT, "(", len(progs), "programs )")
print("category totals:")
for pl in plats:
    tt = {c: totals[pl][c] for c in CATS if totals[pl][c]}
    print(" ", pl, backend[pl], "->", tt)
