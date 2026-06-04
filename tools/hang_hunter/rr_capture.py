"""Determinism tooling #5: capture a runloom repro under rr (record-and-replay).

rr (https://rr-project.org) records a multi-threaded execution and replays it
DETERMINISTICALLY, bit-for-bit -- the gold standard for turning a rare crash
into a reverse-debuggable recording.  Used here to re-run a hang-hunter repro
under `rr record` so the resulting trace can be `rr replay`ed and stepped
*backwards* from the crash to the corruption, instead of forensic-ing a core.

TWO HARD CONSTRAINTS, both honored here:

  1. rr does NOT support io_uring.  We force the epoll netpoll backend
     (RUNLOOM_NETPOLL=epoll) for rr runs.

  2. rr needs a hardware performance counter (the retired-conditional-branch
     counter).  Many VMs -- including this VMware box -- do not expose a usable
     PMU, and rr aborts at startup with
       [FATAL PerfCounters.cc] ioctl(PERF_EVENT_IOC_PERIOD) failed EINVAL
     rr_available() probes for exactly this and returns False with the reason,
     so the daemon/tool cleanly SKIPS rr where it can't run (this box) and uses
     it where it can (bare metal / a PMU-passthrough VM).

Usage:
    python rr_capture.py probe
    python rr_capture.py run  HH_NCOLL=2 HH_NWORK=48 ... -- <workload.py>
"""
import os
import shutil
import subprocess
import sys
import time

_PROBE = None   # cached (ok: bool, reason: str)


def rr_available():
    """(ok, reason).  Cached.  Probes that rr is installed AND can actually
    record on this hardware (the PMU check that fails on PMU-less VMs)."""
    global _PROBE
    if _PROBE is not None:
        return _PROBE
    if shutil.which("rr") is None:
        _PROBE = (False, "rr not installed (apt-get install rr)")
        return _PROBE
    trace = "/tmp/rr_probe_%d" % os.getpid()
    try:
        p = subprocess.run(
            ["rr", "record", "-o", trace, "/bin/true"],
            capture_output=True, text=True, timeout=30,
        )
    except Exception as e:                       # noqa: BLE001
        _PROBE = (False, "rr probe raised: %r" % (e,))
        return _PROBE
    finally:
        shutil.rmtree(trace, ignore_errors=True)
    out = p.stdout + p.stderr
    if "PERF_EVENT_IOC_PERIOD" in out or "PMU" in out or "perf_event" in out:
        _PROBE = (False, "no usable hardware PMU on this host "
                         "(rr ioctl(PERF_EVENT_IOC_PERIOD) failed) -- "
                         "needs bare metal or PMU passthrough")
    elif p.returncode != 0:
        _PROBE = (False, "rr probe exited %d: %s" % (p.returncode, out.strip()[:200]))
    else:
        _PROBE = (True, "rr %s, recording works" % _rr_version())
    return _PROBE


def _rr_version():
    try:
        return subprocess.run(["rr", "--version"], capture_output=True,
                              text=True, timeout=10).stdout.strip()
    except Exception:                            # noqa: BLE001
        return "?"


def capture(workload, env_overlay, out_dir, py=None, timeout=180):
    """Run `workload` under `rr record` (epoll backend).  Returns
    (returncode, trace_path, output).  Caller decides whether to keep the
    trace (e.g. only on a crash).  Raises RuntimeError if rr is unavailable."""
    ok, reason = rr_available()
    if not ok:
        raise RuntimeError("rr unavailable: " + reason)
    py = py or sys.executable
    os.makedirs(out_dir, exist_ok=True)
    trace = os.path.join(out_dir, "trace_%d_%d" % (os.getpid(), int(time.time())))
    env = dict(os.environ)
    env.update(env_overlay)
    env["RUNLOOM_NETPOLL"] = "epoll"             # rr can't record io_uring
    env["PYTHON_GIL"] = "0"
    argv = ["rr", "record", "-o", trace, py, workload]
    try:
        p = subprocess.run(argv, capture_output=True, text=True,
                           env=env, timeout=timeout)
        return p.returncode, trace, p.stdout + p.stderr
    except subprocess.TimeoutExpired as e:
        return 124, trace, (e.output or "") + "\n[rr_capture: TIMEOUT]"


def replay_hint(trace):
    return "rr replay %s    # then `continue`, `reverse-cont`, `when`, etc." % trace


def _main(argv):
    if not argv or argv[0] == "probe":
        ok, reason = rr_available()
        print("rr_available:", ok, "--", reason)
        return 0 if ok else 1
    if argv[0] == "run":
        rest = argv[1:]
        sep = rest.index("--") if "--" in rest else None
        if sep is None:
            print("usage: rr_capture.py run KEY=VAL ... -- <workload.py>")
            return 2
        env_overlay = dict(kv.split("=", 1) for kv in rest[:sep])
        workload = rest[sep + 1]
        out_dir = os.environ.get("RR_TRACE_DIR", "/tmp/rr_traces")
        try:
            rc, trace, out = capture(workload, env_overlay, out_dir)
        except RuntimeError as e:
            print(e)
            return 1
        print("rc=%d trace=%s" % (rc, trace))
        if rc < 0 or rc >= 128 or rc == 124:
            print("CAPTURED a failure under rr. Replay with:")
            print("   ", replay_hint(trace))
        else:
            print("clean run (rc=%d) -- trace kept at %s" % (rc, trace))
        return 0
    print("usage: rr_capture.py {probe|run ...}")
    return 2


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
