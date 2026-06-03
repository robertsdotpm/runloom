"""Auto-triage for the hang-hunter: turn a wedged or crashed runloom process into
a deduplicated, human-readable report.

On a HANG we attach gdb to the still-live process and dump, for the free-threaded
M:N scheduler specifically:
  * every thread's backtrace,
  * the interpreter's stop-the-world state (requested / world_stopped / countdown /
    requester) and every PyThreadState's attach state (0 DETACHED, 1 ATTACHED,
    2 SUSPENDED), and
  * each hub's queue snapshot (deque depth, ready-ring depth, pending, tstate
    attach state).
That trio is what distinguishes the scheduler failure modes (stop-the-world
monopoly, lost wake, snap-dance corruption, ...) at a glance.

On a CRASH we open the core (if one was written) and dump all-thread backtraces.

A SIGNATURE -- a hash of the top frames of the "interesting" thread (the one in the
GC / scheduler / coroutine machinery) -- lets the daemon collapse thousands of
repeats of one bug into a single report with a count, so distinct bugs stand out.

Needs ptrace permission for live attach: /proc/sys/kernel/yama/ptrace_scope = 0
(or run as root).  No-op-degrades cleanly if gdb is missing.
"""
import hashlib
import os
import re
import subprocess


GDB = None


def gdb_path():
    global GDB
    if GDB is None:
        try:
            GDB = subprocess.check_output(["bash", "-lc", "command -v gdb"],
                                          text=True).strip()
        except Exception:
            GDB = ""
    return GDB


# gdb batch script that prints the per-interp stop-the-world state and every
# tstate's attach state, then each hub's queue snapshot.  Pure reads; safe on a
# live process.  Field names track src/runloom_c/mn_sched.c + runloom_sched.h.
STATE_GDB = r"""
set pagination off
printf "=== stoptheworld ===\n"
set $i = _PyRuntime.interpreters.head
printf "requested=%d world_stopped=%d countdown=%d requester=%p\n", \
  $i->stoptheworld.requested, $i->stoptheworld.world_stopped, \
  $i->stoptheworld.thread_countdown, $i->stoptheworld.requester
printf "=== tstates (addr / state: 0=DETACHED 1=ATTACHED 2=SUSPENDED) ===\n"
set $t = $i->threads.head
while $t != 0
  printf "tstate=%p state=%d\n", $t, $t->state
  set $t = $t->next
end
printf "=== hubs ===\n"
printf "pending_global=%ld hub_count=%d\n", runloom_mn_pending_global, runloom_hub_count
set $n = runloom_hub_count
set $k = 0
while $k < $n
  set $h = &runloom_hubs[$k]
  printf "hub %d: pending=%ld stopping=%d tstate_state=%d deque=%ld ready=%lu sleep=%ld sub_head=%p\n", \
    $h->id, $h->pending, $h->stopping, $h->tstate->state, \
    ($h->deque.bottom - $h->deque.top), \
    ($h->sched.ready_tail - $h->sched.ready_head), \
    $h->sched.sleep_size, $h->sub_head
  set $k = $k + 1
end
"""

NOISE = re.compile(r"debuginfod|auto-load|safe-path|info \"|For more|Enable deb|"
                   r"Debuginfod|libthread_db|This GDB|add-auto|^\[New LWP|"
                   r"^\[Thread debugging|^Using host")

# Frames worth keying a signature on: the scheduler / GC / coroutine machinery.
INTEREST = re.compile(r"runloom_|gc_collect|deduce_unreachable|update_refs|"
                      r"mark_heap|stop_the_world|start_the_world|tstate_wait|"
                      r"_PyThreadState|_Py_HandlePending|mi_heap|_PyEval_EvalFrame")


def run_gdb(args, timeout=30):
    g = gdb_path()
    if not g:
        return "(gdb not available)"
    try:
        out = subprocess.run([g, "-batch", "-nx"] + args,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             timeout=timeout, text=True).stdout
    except Exception as e:
        return "(gdb failed: {0})".format(e)
    return "\n".join(ln for ln in out.splitlines() if not NOISE.search(ln))


def signature(all_bt):
    """Hash the top ~6 'interesting' frames so the same bug dedups to one key."""
    frames = []
    for ln in all_bt.splitlines():
        m = re.search(r"#\d+\s+(?:0x[0-9a-f]+ in )?([A-Za-z_][\w.]*)", ln)
        if m and INTEREST.search(ln):
            frames.append(m.group(1))
        if len(frames) >= 6:
            break
    if not frames:
        # fall back to the first few frames of any kind
        for ln in all_bt.splitlines():
            m = re.search(r"#\d+\s+(?:0x[0-9a-f]+ in )?([A-Za-z_][\w.]*)", ln)
            if m:
                frames.append(m.group(1))
            if len(frames) >= 6:
                break
    key = "|".join(frames) if frames else "unknown"
    return hashlib.sha1(key.encode()).hexdigest()[:12], key


def triage_hang(pid, write):
    """Attach to a live hung pid; return (sig, key, report_text)."""
    bt = run_gdb(["-p", str(pid), "-ex", "set pagination off",
                  "-ex", "thread apply all bt"])
    tmp = os.path.join("/tmp", "hh_state_{0}.gdb".format(os.getpid()))
    with open(tmp, "w") as fh:
        fh.write(STATE_GDB)
    state = run_gdb(["-p", str(pid), "-x", tmp])
    try:
        os.unlink(tmp)
    except OSError:
        pass
    sig, key = signature(bt)
    report = ("KIND: HANG\nPID: {0}\nSIGNATURE: {1}\nKEY: {2}\n\n"
              "===== STOP-THE-WORLD / HUB STATE =====\n{3}\n\n"
              "===== ALL-THREAD BACKTRACE =====\n{4}\n").format(
                  pid, sig, key, state, bt)
    write(report)
    return sig, key


def triage_crash(py, corefile, write):
    """Open a core (may be None); return (sig, key)."""
    if corefile and os.path.exists(corefile):
        bt = run_gdb([py, corefile, "-ex", "set pagination off",
                      "-ex", "thread apply all bt"])
    else:
        bt = "(no core file; core_pattern may pipe elsewhere -- see README)"
    sig, key = signature(bt)
    write("KIND: CRASH\nCORE: {0}\nSIGNATURE: {1}\nKEY: {2}\n\n"
          "===== ALL-THREAD BACKTRACE =====\n{3}\n".format(
              corefile, sig, key, bt))
    return sig, key
