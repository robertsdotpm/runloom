#!/usr/bin/env python3
"""fault_sweep_counted.py -- SQLite-style counted-exhaustive anomaly sweep.

For each runloom fault site, fail the Nth reach of that site (the runtime's
RUNLOOM_FAULT_<SITE>="nth:N:CODE" mode, netpoll_init.c.inc) for N = 1, 2, 3...
and STOP when a clean run reports _fault_count(site) == 0: the workload reached
the site fewer than N times, so EVERY reachable failure point in this workload
has now been exercised -- the fixpoint that makes the sweep exhaustive rather
than sampled.  Injection counting is scoped to runloom's OWN sites, so CPython's
allocator churn never skews N (the reason an Nth-libc-malloc sweep can't work).

Verdicts per run (mirrors tools/fault_sweep.py):
  ok        -- workload completed, injection handled gracefully (or not reached)
  graceful  -- workload exited nonzero but ORDERLY (an exception surfaced --
               acceptable iff the site's contract is to raise; still reported)
  CRASH     -- killed by a signal (SIGSEGV/SIGABRT...): a real bug
  HANG      -- exceeded the per-run timeout: a real bug (lost wake / stuck loop)

Exit nonzero iff any CRASH or HANG.  Run it:
  PYTHON_GIL=0 python3 tools/fault_sweep_counted.py            # all Linux sites
  PYTHON_GIL=0 python3 tools/fault_sweep_counted.py SPAWN_G    # one site

House style: .format()/%% only -- no f-strings.
"""
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

# Default sites = the ones actually WIRED on the Linux epoll backend:
#   - SPAWN_G / SPAWN_STACK: goroutine-spawn allocation OOM (every platform).
#   - FD_READ / FD_WRITE: module_fdio's cooperative read/write loops.
# Deliberately absent (pass explicitly if you know better):
#   - TCP_*: RUNLOOM_TCP_FINJ compiles to a no-op outside Windows/kqueue -- the
#     Linux TCP error-path campaign is driven by strace -e inject= instead
#     (see netpoll_init.c.inc's fault-injection header).
#   - SPAWN_TSTATE: declared in the site enum but currently unwired (no call
#     site) -- exhausts at N=1 by construction.
#   - pump sites (WSAPOLL/IOCP_*/KQUEUE_*/SELECT): other backends only.
LINUX_SITES = [
    "SPAWN_G", "SPAWN_STACK",
    "FD_READ", "FD_WRITE",
]

# The child workload: touches every sweepable site class, CATCHES the errors an
# injected failure is CONTRACTED to surface (OSError/MemoryError), and prints
# the site's fired count last.  Completing the script (exit 0) == the runtime
# degraded gracefully; anything else is the sweep's finding.
WORKLOAD = r"""
import errno, os, socket, sys
sys.path.insert(0, "src")
import runloom_c
SITE = os.environ["SWEEP_SITE"]
def eat(fn):
    try:
        fn()
    except (OSError, MemoryError):
        pass
# --- spawn sites (single-thread + M:N so SPAWN_TSTATE is reachable) ---
done = [0]
def child(): done[0] += 1
for _ in range(24):
    eat(lambda: runloom_c.fiber(child))
runloom_c.run()
def mn_body():
    for _ in range(8):
        eat(lambda: runloom_c.mn_fiber(child))
eat(lambda: (runloom_c.mn_init(2), runloom_c.mn_fiber(mn_body),
             runloom_c.mn_run(), runloom_c.mn_fini()))
# --- TCP sites (TCPConn: socket/connect/accept/recv/send) ---
# INJECTION-SAFE round: a failed spawn must never strand its already-queued
# sibling (a stranded srv parks in accept() forever -> the NEXT round's run()
# hangs -- a workload artifact the sweep would misreport as a runtime HANG).
# If either spawn fails, close the listener BEFORE run(): the surviving fiber
# then fails fast (accept -> closed; connect -> refused / recv -> reset).
def tcp_round():
    L = runloom_c.TCPConn.listen("127.0.0.1", 0)
    fd = L.fileno(); sk = socket.socket(fileno=socket.dup(fd))
    port = sk.getsockname()[1]; sk.close()
    def srv():
        try:
            c = L.accept(); d = c.recv(64)
            if d: c.send(d)
            c.close()
        except (OSError, MemoryError): pass
    def cli():
        try:
            c = runloom_c.TCPConn.connect("127.0.0.1", port)
            c.send(b"x"); c.recv(64); c.close()
        except (OSError, MemoryError): pass
    both = True
    for fn in (srv, cli):
        try:
            runloom_c.fiber(fn)
        except (OSError, MemoryError):
            both = False
    if not both:
        L.close()
    runloom_c.run()
    L.close()          # idempotent (close() no-ops when already closed)
for _ in range(4):
    eat(tcp_round)
# --- FD sites (fd_read / fd_write on a pipe) ---
# Injection-safe pipeline discipline: the reader must terminate no matter WHAT
# happens to the writer.  Two failure paths strand it otherwise:
#   - writer never SPAWNED         -> close w before run()
#   - writer's WRITE was injected  -> writer closes w in its finally (real
#     producers do this): the reader then reads the byte or EOF, never parks
#     forever on a pipe whose write end nobody will use again.
def fd_round():
    r, w = os.pipe()
    wopen = [True]
    def close_w():
        if wopen[0]:
            wopen[0] = False
            os.close(w)
    def wr():
        try: runloom_c.fd_write(w, b"y")
        except (OSError, MemoryError): pass
        finally: close_w()
    def rd():
        try: runloom_c.fd_read(r, bytearray(1), 1)
        except (OSError, MemoryError): pass
    wr_ok = True
    try: runloom_c.fiber(wr)
    except (OSError, MemoryError): wr_ok = False
    try: runloom_c.fiber(rd)
    except (OSError, MemoryError): pass
    if not wr_ok:
        close_w()
    runloom_c.run()
    close_w()
    try: os.close(r)
    except OSError: pass
for _ in range(4):
    eat(fd_round)
print("FIRED=%d" % runloom_c._fault_count(SITE), flush=True)
"""


def run_one(site, nth, code, timeout):
    env = dict(os.environ,
               PYTHON_GIL="0", PYTHONPATH="src", SWEEP_SITE=site)
    env["RUNLOOM_FAULT_" + site] = "nth:%d:%d" % (nth, code)
    try:
        p = subprocess.run([PY, "-c", WORKLOAD], cwd=ROOT, env=env,
                           capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return "HANG", None
    fired = None
    for line in (p.stdout or "").splitlines():
        if line.startswith("FIRED="):
            fired = int(line.split("=", 1)[1])
    if p.returncode < 0:
        return "CRASH(sig %d)" % -p.returncode, fired
    if p.returncode != 0:
        return "graceful", fired
    return "ok", fired


def sweep_site(site, code=12, maxn=400, timeout=30):
    findings = []
    n = 0
    while n < maxn:
        n += 1
        verdict, fired = run_one(site, n, code, timeout)
        if verdict.startswith("CRASH") or verdict == "HANG":
            findings.append((n, verdict))
            print("  %-12s N=%-4d %s   <-- FINDING" % (site, n, verdict), flush=True)
            continue          # keep sweeping: later Ns are independent points
        if verdict == "graceful":
            print("  %-12s N=%-4d graceful (orderly nonzero exit)" % (site, n), flush=True)
            continue
        if fired == 0:
            # clean run, site reached fewer than N times: EXHAUSTED.
            print("  %-12s exhausted at N=%d (%d injection points exercised)"
                  % (site, n, n - 1), flush=True)
            return n - 1, findings
    print("  %-12s hit maxn=%d without exhausting (raise maxn?)" % (site, maxn),
          flush=True)
    return maxn, findings


def main(argv):
    sites = argv[1:] or LINUX_SITES
    t0 = time.time()
    total_points = 0
    all_findings = []
    print("== counted-exhaustive fault sweep: %s ==" % " ".join(sites), flush=True)
    for site in sites:
        points, findings = sweep_site(site)
        total_points += points
        all_findings += [(site,) + f for f in findings]
    print("== done in %.0fs: %d injection points exercised, %d findings =="
          % (time.time() - t0, total_points, len(all_findings)), flush=True)
    for site, n, verdict in all_findings:
        print("  FINDING %s N=%d %s" % (site, n, verdict))
    return 1 if all_findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
