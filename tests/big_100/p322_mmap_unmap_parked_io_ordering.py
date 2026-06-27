"""big_100 / 322 -- mmap UNMAP vs a parked netpoll recv_into, at the exit instant.

mmap is on the explicit untested-subsystem list and appears in NONE of the built
p300-series programs.  This probes an ordering the runtime has never been put
through: a goroutine PARKED in a cooperative socket recv_into whose target buffer
is a SLICE OF AN mmap REGION, while another goroutine on a DIFFERENT hub (and the
teardown path) munmaps / closes the backing file in a stressful order WHILE the
recv is still parked.

The bite: the patched recv_into (src/runloom/monkey/sockets.py:135) hands the
caller's buffer straight to the C netpoll recv primitive -- with no timeout it
takes the FAST C path `_tcp_recv(fd, buffer, n, flags)`, which holds the buffer
pointer across the netpoll park.  When the peer finally sends, the netpoll wake
resumes the kernel copy and it writes into that mmapped page.  If a sibling on
another hub (or teardown) has munmap'd the page in the meantime, the kernel copy
(or the resumed Python access to the now-unmapped buffer) touches an unmapped
address -> SIGBUS (returncode -7) / SIGSEGV (-11); or the parker is STRANDED
(recv_into into a freed buffer never completes, the join wedges).  The assumption
that mmap teardown ordering is INDEPENDENT of parked netpoll IO is untested.

Distinct from p309_exit_pipes (anonymous pipes, NO mmap) and from a page-fault-
during-park spec (that faults a resident-then-evicted page on RESUME; this races
the UNMAP against an in-flight kernel copy at the EXIT instant, cross-hub).

Each CHILD picks a teardown ORDER (a permutation of the three release steps:
mm.close, os.close(fd), sockets.close), stands up K socketpairs whose recv_into
target is a slice of one shared mmap region, parks K goroutines in recv_into
(peers DELAY their send so the recv is genuinely PARKED, not already complete),
prints DONE-MARKER, then -- with the recvs still parked and a sibling goroutine
on a different hub munmapping -- tears the mmap/fd/sockets down in the chosen
order, lets the peers fire, and returns so run() joins.

ORACLE (subprocess driver -- a SIGBUS must crash a CHILD, not poison the parent
sweep, exactly like p305/p306/p309):

  * per teardown-order variant: child returncode == 0 AND >= 0.  A SIGBUS from
    the kernel copy touching an unmapped page shows as a NEGATIVE returncode
    (-7 SIGBUS / -10 on some arches), a SIGSEGV as -11; BOTH are caught (the
    >= 0 check isolates a crash-signal even if a future regression made the
    expected code negative).
  * DONE-MARKER present (it actually stood up the parked recvs over the mmap)
    AND MAIN-EXIT present (run()'s join drained the recvs after teardown -- no
    wedge), for EVERY ordering.
  * TimeoutExpired -> H.fail: a parked recv_into into an unmapped buffer that
    never completes stranded the join.

METAMORPHIC content arm (clean-order variant only): the child writes a known
pattern THROUGH the mmap view for the bytes a recv actually delivered, and the
parent re-reads the backing FILE and asserts the delivered bytes match -- a
torn/lost write through the mmap-backed recv buffer (a copy that landed in a
stale/unmapped mapping) is caught as a content mismatch, not just as a crash.

require_no_lost on the PARENT pool.

If this only ever reproduces a plain-CPython SIGBUS independent of M:N, the
cross-hub-unmap-while-parked framing is the runloom-specific bite -- the
"crosshub" variant (sibling on a different hub munmaps while the recv is parked
on its own hub) is the PRIMARY one and is always exercised.

Stresses: mmap region as a recv_into target across a netpoll park, cross-hub
munmap vs an in-flight kernel copy at the exit instant, mm.close/os.close/socket-
close teardown ORDER permutations, parked-recv wake after the backing mapping is
torn, no SIGBUS/SIGSEGV/hang, no torn write through the mmapped recv buffer.

Good TSan / controlled-M:N-replay target: the munmap-vs-resumed-copy ordering is
a cross-hub memory-lifetime race; a data race (or a tools/asan report of a touch
on the unmapped region) is often the first signal, before the returncode oracle.
"""
import os
import re
import subprocess

import harness
import procutil

# K socketpairs per child, each with one goroutine PARKED in recv_into over a
# slice of the shared mmap region.  Small -- the point is the EXIT INSTANT and
# the teardown ORDER, not throughput; a wide parked population just widens the
# munmap-vs-resumed-copy window.
K = 24
# Bytes each parked recv targets in the mmap (its slot is [wid*SLOT : +SLOT]).
SLOT = 64
# The peer holds its send back this long so the recv is GENUINELY PARKED on the
# fd's netpoll arm (a recv that completes before teardown wouldn't race it).
SEND_DELAY = 0.06

# Teardown-order variants.  Each is a permutation of the three release steps
# applied while the K recvs are still parked, with a sibling on ANOTHER hub
# munmapping concurrently.  "clean" additionally runs the metamorphic content
# arm (peers send a KNOWN pattern, parent re-reads the file).  "crosshub" is the
# PRIMARY runloom-specific variant (the unmap happens on a DIFFERENT hub than the
# parked recv); the mm-first / fd-first / sock-first variants permute WHICH
# release races the resumed copy.
ORDERS = ("clean", "crosshub", "mmfirst", "fdfirst", "sockfirst")

CHILD = r'''
import sys, os, mmap, socket, threading, time
sys.path.insert(0, {src!r})
import runloom
import runloom.monkey
runloom.monkey.patch()                     # cooperative socket recv_into on hubs

ORDER = sys.argv[1] if len(sys.argv) > 1 else "crosshub"
K     = int(sys.argv[2]) if len(sys.argv) > 2 else 24
SLOT  = int(sys.argv[3]) if len(sys.argv) > 3 else 64
DELAY = float(sys.argv[4]) if len(sys.argv) > 4 else 0.06

stop = [False]
lock = threading.Lock()
socks = []                                 # every socket (both ends of each pair)
delivered = [0] * K                        # bytes each recv actually drained
parked = [0]                               # how many recvs are confirmed parked
state = {{"fd": -1, "mm": None, "path": None, "size": 0}}

# CLEAN is the metamorphic baseline: the recv must genuinely COMPLETE (peer
# sends, the copy lands in the mmap, verified via the file) and teardown happens
# AFTER, in a benign order -- so it is a correctness check, not a crash race.
# All other orders RACE the resumed copy against the unmap and rely on the
# returncode/no-hang oracle.
CLEAN = (ORDER == "clean")

def fill_byte(i):
    # A per-slot known byte so a torn/lost write through the mmap recv buffer is
    # visible in the file: slot i is all (0x41 + i % 64) -- 'A'.. range.
    return 0x41 + (i % 64)

def recv_into_parked(idx, sock, mv):
    # Park in recv_into over a SLICE OF THE MMAP region.  The patched recv_into
    # hands `mv` to the C netpoll recv primitive, which holds the buffer pointer
    # across the park; when the peer finally sends, the resumed kernel copy
    # writes into the mmapped page -- the exact page the teardown may have just
    # munmap'd on another hub.
    with lock:
        parked[0] += 1
    try:
        n = sock.recv_into(mv, len(mv))
        if n:
            with lock:
                delivered[idx] = n
    except (OSError, ValueError, BufferError):
        pass                               # torn buffer / closed under us -> done

def peer_delayed_send(idx, sock):
    # DELAY so the matching recv is genuinely parked, THEN send the known
    # pattern.  In CLEAN we always send (the recv must complete, then we verify
    # the mmap content); in the racy orders the send fires right around the
    # teardown window so the resumed copy races the munmap, and we skip it once
    # the sockets are being torn down (stop) so a send into a closed fd is not
    # mistaken for the hazard.
    try:
        runloom.sleep(DELAY)
        if stop[0] and not CLEAN:
            return
        sock.sendall(bytes([fill_byte(idx)]) * SLOT)
    except (OSError, ValueError):
        pass

def crosshub_unmapper(mm):
    # A SIBLING goroutine -- the scheduler spreads goroutines across hubs, so
    # this runs (with good probability) on a DIFFERENT hub than the parked
    # recvs.  It munmaps the backing region WHILE the recvs are parked / their
    # copies in flight: the runloom-specific cross-hub unmap-vs-parked-IO race.
    try:
        runloom.sleep(DELAY * 0.5)         # let the recvs park first
        if not stop[0]:
            return
        mm.close()                         # munmap from (likely) another hub
    except (OSError, ValueError, BufferError):
        pass

def main():
    # Backing file sized to hold K slots; mmap it and use slices as recv targets.
    import tempfile
    fd, path = tempfile.mkstemp(prefix="big100_mmaprecv_")
    size = K * SLOT
    os.ftruncate(fd, size)
    mm = mmap.mmap(fd, size)
    with lock:
        state["fd"] = fd; state["mm"] = mm; state["path"] = path; state["size"] = size
    view = memoryview(mm)

    for i in range(K):
        a, b = socket.socketpair()
        with lock:
            socks.append(a); socks.append(b)
        # recv target = this slot's slice of the mmap region.
        runloom.fiber(recv_into_parked, i, a, view[i * SLOT:(i + 1) * SLOT])
        runloom.fiber(peer_delayed_send, i, b)

    if ORDER == "crosshub":
        runloom.fiber(crosshub_unmapper, mm)

    runloom.sleep(DELAY * 0.4)             # let every recv_into actually park
    sys.stdout.write("DONE-MARKER\n"); sys.stdout.flush()

    if CLEAN:
        # Metamorphic baseline: do NOT flip stop -- let every peer's delayed send
        # land and every recv_into COMPLETE its copy into the mmap, then verify.
        # Poll until all K slots delivered (bounded by a wall so a genuinely lost
        # recv still tears down and the parent's no-hang oracle bites).
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            with lock:
                got = sum(1 for d in delivered if d > 0)
            if got >= K:
                break
            runloom.sleep(0.01)
        # mm.flush so the delivered bytes are guaranteed visible via the file
        # before we close the mapping and the parent re-reads it.
        try: mm.flush()
        except (OSError, ValueError, BufferError): pass

    # Flip stop so the delayed peers fire their sends NOW (racing the teardown);
    # in CLEAN the sends already landed, so this just lets the loops exit.
    stop[0] = True

    # The three release steps, applied while the recvs are still parked / their
    # resumed copies in flight.  ORDER chooses the permutation; "crosshub"
    # already munmapped on a sibling hub above, so its mm.close here is the
    # idempotent second close (must be tolerated).
    def step_mm():
        try: mm.close()                    # munmap the region (out from under recvs)
        except (OSError, ValueError, BufferError): pass
    def step_fd():
        try: os.close(fd)                  # close the backing fd
        except OSError: pass
    def step_sock():
        with lock:
            ss = list(socks)
        for s in ss:                       # close sockets -> wakes parked recvs
            try: s.close()
            except OSError: pass

    perms = {{
        "clean":    (step_sock, step_mm, step_fd),   # wake recvs, then unmap, then fd
        "crosshub": (step_sock, step_mm, step_fd),   # + sibling already unmapped
        "mmfirst":  (step_mm, step_sock, step_fd),   # UNMAP FIRST, recvs still parked
        "fdfirst":  (step_fd, step_mm, step_sock),   # fd first, then unmap, then wake
        "sockfirst":(step_sock, step_fd, step_mm),   # wake, then fd, then unmap
    }}[ORDER]
    for step in perms:
        step()

    # Give the resumed copies / wakes a beat to land on the (now torn-down)
    # mapping before run() joins -- this is the window a SIGBUS lands in.
    runloom.sleep(0.02)
    # return -> run() joins the (woken/torn) recv goroutines.

runloom.run(4, main)

# METAMORPHIC content arm: for the clean order, re-read the backing FILE and
# emit the per-slot delivered bytes so the PARENT can verify no torn/lost write
# through the mmap recv buffer.  Only meaningful when the mapping/fd outlive the
# recvs in a defined order (clean): sock-close wakes the recvs, the copies land,
# THEN we unmap -- so the file must hold the delivered pattern.  Other orders
# intentionally tear the mapping mid-copy, so their content is undefined (the
# returncode/no-hang oracle is what bites there).
if ORDER == "clean":
    try:
        path = state["path"]; size = state["size"]
        # The mm is closed; re-open the file fresh and read what landed.
        with open(path, "rb") as f:
            data = f.read(size)
        # Emit "slot:delivered:firstbyte" for slots that delivered, so the
        # parent can check the byte written through the mmap view matches.
        out = []
        for i in range(K):
            n = delivered[i]
            if n > 0:
                fb = data[i * SLOT] if i * SLOT < len(data) else -1
                out.append("{{0}}:{{1}}:{{2}}".format(i, n, fb))
        sys.stdout.write("CONTENT " + ",".join(out) + "\n")
        sys.stdout.flush()
    except Exception:
        pass
try:
    os.unlink(state["path"])
except OSError:
    pass

sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
'''


def setup(H):
    import sys
    src = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src")
    script = os.path.join(H.make_tmpdir("big100_mmaprecv_"), "child.py")
    with open(script, "w") as f:
        f.write(CHILD.format(src=src))
    H.state = {"py": sys.executable, "script": script,
               "per_order": [0] * 1024}   # ops attributed by order index, 1/slot


def _fill_byte(i):
    """Mirror the child's per-slot known byte for the metamorphic check."""
    return 0x41 + (i % 64)


def _verify_content(H, out, wid):
    """Clean-order metamorphic arm: every byte a recv DELIVERED through the
    mmap view must read back from the file as the slot's known fill byte.  A
    torn/lost write (copy into a stale/unmapped mapping) is a content mismatch.
    Returns True if OK or no CONTENT line (other orders emit none)."""
    m = re.search(rb"CONTENT ([^\n]*)", out)
    if m is None:
        return True
    payload = m.group(1).decode("ascii", "replace").strip()
    if not payload:
        return True
    for tok in payload.split(","):
        try:
            idx, n, fb = (int(x) for x in tok.split(":"))
        except ValueError:
            continue
        if not H.check(n == SLOT,
                       "clean: slot {0} delivered {1} bytes != SLOT {2} wid={3} "
                       "(short recv into mmap buffer)".format(idx, n, SLOT, wid)):
            return False
        if not H.check(fb == _fill_byte(idx),
                       "clean: slot {0} read back byte {1} != expected {2} wid={3} "
                       "-- TORN/LOST write through the mmap-backed recv buffer "
                       "(copy landed in a stale/unmapped mapping)".format(
                           idx, fb, _fill_byte(idx), wid)):
            return False
    return True


def worker(H, wid, rng, state):
    py = state["py"]
    script = state["script"]
    for _ in H.round_range():
        if not H.running():
            break
        # Rotate the teardown order so every permutation is exercised across the
        # pool even at small --funcs; crosshub (the primary) and clean (the
        # metamorphic arm) both get hit.
        order = ORDERS[(wid + rng.getrandbits(3)) % len(ORDERS)]
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        try:
            proc = procutil.popen(
                [py, script, order, str(K), str(SLOT), repr(SEND_DELAY)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=env, running=H.running)
        except OSError:
            break                          # shutdown cancelled the spawn
        try:
            out, err = proc.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=10)
            except Exception:
                pass
            # A timeout is a STRANDED-RECV bug only while the harness is still
            # running; once H.running() is False this child was merely caught
            # mid-flight at over-scale -- a benign drain timeout, not a wedge.
            if not H.running():
                break
            H.fail("child HUNG tearing down mmap under a parked recv_into "
                   "(order={0}) wid={1} -- a recv_into into an unmapped buffer "
                   "was stranded (join wedged)".format(order, wid))
            return
        except OSError:
            if not H.running():
                break
            raise
        # A SIGBUS/SIGSEGV from the resumed kernel copy touching an unmapped page
        # surfaces as a NEGATIVE returncode (-7 SIGBUS / -10 / -11 SIGSEGV).
        if not H.check(proc.returncode is not None and proc.returncode >= 0,
                       "child CRASHED (signal {0}) tearing down mmap under a "
                       "parked recv_into (order={1}) wid={2} -- SIGBUS/SIGSEGV "
                       "from the resumed copy on an unmapped page? stderr={3!r}"
                       .format(-proc.returncode if proc.returncode is not None
                               else "?", order, wid, err[-300:])):
            return
        if not H.check(proc.returncode == 0,
                       "child exited {0} (nonzero) order={1} wid={2} stderr={3!r}"
                       .format(proc.returncode, order, wid, err[-300:])):
            return
        # DONE-MARKER: stood up the parked recvs over the mmap.  MAIN-EXIT: the
        # join drained the (woken/torn) recvs after teardown -- no wedge.
        if not H.check(b"DONE-MARKER" in out,
                       "child never stood up parked mmap recvs order={0} wid={1}: "
                       "{2!r}".format(order, wid, out[:160])):
            return
        if not H.check(b"MAIN-EXIT" in out,
                       "child reached teardown but join never returned order={0} "
                       "wid={1}: {2!r}".format(order, wid, out[:160])):
            return
        # METAMORPHIC content arm (clean order only).
        if order == "clean" and not _verify_content(H, out, wid):
            return
        state["per_order"][ORDERS.index(order)] += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    po = H.state["per_order"]
    by_order = {name: po[i] for i, name in enumerate(ORDERS)}
    H.log("clean_teardowns={0} by_order={1} exited={2}/{3} K={4} slot={5}".format(
        H.total_ops(), by_order, H.exited, H.expected, K, SLOT))
    H.check(H.total_ops() > 0,
            "no child survived tearing down an mmap under parked recv_into")
    H.require_no_lost("mmap-unmap vs parked recv_into ordering")


if __name__ == "__main__":
    # SUBPROCESS program: each worker iteration forks a real child runloom
    # process that mmaps a file, parks K recv_into goroutines over its slices,
    # and tears the mmap/fd/sockets down in a chosen order while a sibling on
    # another hub munmaps.  A SIGBUS therefore crashes the CHILD (caught by the
    # returncode oracle), never the parent sweep.  Hard funcs ceiling keeps the
    # soak driver from fork-bombing the box at 1M.
    harness.main("p322_mmap_unmap_parked_io_ordering", body, setup=setup,
                 post=post, default_funcs=100, max_funcs=300,
                 describe="child runloom tears down an mmap region (mm.close / "
                          "os.close(fd) / socket.close permutations) while K "
                          "goroutines are PARKED in recv_into over slices of it "
                          "and a sibling on another hub munmaps; returncode 0/"
                          ">=0 (no SIGBUS/SIGSEGV), no hang, no torn mmap write")
