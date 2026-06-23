"""sweep_win.py -- run the first 100 big_100 programs on Windows, sequentially.

Usage:  python3.13t big_100/sweep_win.py [funcs] [hubs] [duration] [timeout]

Windows analogue of sweep_mac.sh.  No bash, so this is pure Python:
  * runs p01..p100 (the first 100 programs by number) in subprocesses;
  * VERDICT-first classification (a program that printed VERDICT: PASS but then
    lingered in mn_fini teardown still counts as PASS);
  * per-program log files under big_100/win_logs/;
  * kills the child tree on timeout (taskkill /T) so leaked grandchildren can't
    wedge the sweep.
"""
import glob
import os
import re
import subprocess
import sys

PY = os.environ.get("PYFT", r"C:\Users\matth\py313t\python3.13t.exe")
FUNCS = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
HUBS = int(sys.argv[2]) if len(sys.argv) > 2 else 8
DUR = float(sys.argv[3]) if len(sys.argv) > 3 else 10.0
TMO = float(sys.argv[4]) if len(sys.argv) > 4 else 120.0

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
LOGD = os.path.join(HERE, "win_logs")
os.makedirs(LOGD, exist_ok=True)


def prog_num(path):
    m = re.search(r"p(\d+)", os.path.basename(path))
    return int(m.group(1)) if m else 9999


progs = sorted(glob.glob(os.path.join(HERE, "p[0-9]*.py")), key=prog_num)
progs = [p for p in progs if prog_num(p) <= 100]   # first 100 only

# Optional targeted subset: RUNLOOM_SWEEP_PROGS="7 8 11 26" (space/comma list of
# program numbers) restricts the run to those, for fast fix-and-recheck cycles
# without re-running the whole first-100.  Unset -> full first-100.
_only = os.environ.get("RUNLOOM_SWEEP_PROGS", "").replace(",", " ").split()
if _only:
    want = {int(x) for x in _only}
    progs = [p for p in progs if prog_num(p) in want]

env = dict(os.environ)
env["PYTHON_GIL"] = "0"
env["PYTHONPATH"] = os.path.join(ROOT, "src")

VERDICT_RE = re.compile(r"VERDICT\s*:\s*([A-Z]+)")
EXIT_RE = re.compile(r"worker_exits\s*:\s*(\d+/\d+)|exited=(\d+/\d+)")

results = []
npass = 0
for prog in progs:
    name = os.path.splitext(os.path.basename(prog))[0]
    logpath = os.path.join(LOGD, name + ".log")
    cmd = [PY, prog, "--funcs", str(FUNCS), "--hubs", str(HUBS),
           "--duration", str(DUR), "--rounds", "1",
           "--hang-timeout", "50", "--drain-timeout", "50"]
    with open(logpath, "w") as lf:
        proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=lf,
                                stderr=subprocess.STDOUT)
        try:
            rc = proc.wait(timeout=TMO)
        except subprocess.TimeoutExpired:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            rc = "TMO"
    try:
        with open(logpath, "r", errors="replace") as lf:
            out = lf.read()
    except OSError:
        out = ""
    vm = VERDICT_RE.findall(out)
    verdict = vm[-1] if vm else None
    em = EXIT_RE.findall(out)
    exited = ("".join(em[-1]) if em else "")
    if verdict == "PASS":
        cls = "PASS"; npass += 1
    elif rc == "TMO":
        cls = "TIMEOUT"
    elif isinstance(rc, int) and rc < 0:
        cls = "CRASH(sig%d)" % (-rc)
    elif isinstance(rc, int) and rc >= 128:
        cls = "CRASH(rc=%d)" % rc
    elif verdict:
        cls = "VFAIL(%s)" % verdict
    else:
        cls = "FAIL(rc=%s)" % rc
    results.append((name, cls, exited))
    print("%-28s %-15s %s" % (name, cls, exited), flush=True)

print("==== big_100 first-100 @%d: %d/%d PASS ====" % (FUNCS, npass, len(progs)))
bad = [n for n, c, e in results if c != "PASS"]
if bad:
    print("NON-PASS: " + " ".join(bad))
