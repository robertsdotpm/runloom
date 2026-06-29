"""big_100 / 497 -- shutil file operations under M:N.

shutil provides high-level file operations (copy, rmtree, move, etc.) that wrap
low-level filesystem syscalls. Most shutil functions are pure and stateless,
operating on distinct file paths with no module-level mutable state -- BUT shutil
internally uses linecache (for reading source files) and tempfile (for atomic
renames on some platforms), both of which have thread-affine state.  Under M:N
many fibers share one hub OS-thread, so if shutil operations corrupted linecache
state or tempfile bookkeeping across a yield, sibling fibers' subsequent file ops
would observe the corruption.

PROBE DESIGN: This program is a MOSTLY STATELESS NEGATIVE CONTROL / EXPECTED
PASS (no runloom-specific hazard expected for the most basic operations).  Each
fiber performs shutil operations (copy, move, rmtree) on DISTINCT temporary files
and directories (no sharing), constructed from fiber-local paths derived from
(wid, iteration).  Under a correct runtime the operations should succeed 100% of
the time:
  - Plain threads (GIL on/off): each fiber operates on its own disjoint paths
  - Runloom M:N: each fiber still operates on its own disjoint paths, and even
    though siblings share the hub thread they never race on the same file

The LOAD-BEARING oracle asserts that shutil operations on a fiber's DISTINCT
temp files succeed and produce the expected results (file copied/moved/deleted)
after yields and migrations.  A failure (unable to copy, wrong file size,
directory didn't delete) indicates either a real shutil bug (platform-specific,
0 under plain threads if it's M:N-triggered) or a deeper file-descriptor leak /
resource exhaustion under M:N (all fibers' disjoint FDs accumulate).

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  Each fiber creates temporary file/directory paths derived from its wid and
  iteration counter, ensuring NO TWO FIBERS EVER TOUCH THE SAME FILE.  Siblings
  may be mid-shutil operation on their own disjoint files when a fiber yields
  (and potentially migrates hubs), but the files they operate on are disjoint so
  there is no concurrent mutation of a shared file.  Under this constraint:

  CORRECT: shutil.copy(src, dst) MUST succeed (the src file is fiber-local, the
           dst file is fiber-local, no other fiber reads either).  The copied
           file MUST have the same size/content as the source.  On re-read after
           yield + potential hub migration, the file MUST still exist and be
           unchanged.
  FAIL:    shutil.copy raises OSError (file disappeared, FD leaked, or tempfile
           bookkeeping corrupted).  Or file size / content is wrong (the bytes
           wrote didn't make it to disk, or were mangled mid-copy).  Or post-
           yield re-read shows a vanished file or wrong content (a sibling's
           tempfile cleanup / linecache purge corrupted this fiber's file).

  The NON-SHARED-FILE invariant (each fiber owns disjoint files) means a failure
  is NOT a documented M:N "shared-object" behavior (like threading.local leak or
  decimal Context sharing) -- it is a genuine file-system or resource bug.  We
  verify against plain threads (0 failures under GIL on/off) so the oracle is
  non-vacuous.

ARMS:
  * LOAD-BEARING -- DISTINCT-FILE SHUTIL OPERATIONS (worker, HARD, fail-fast).
    Each fiber constructs paths for its temp src file (fiber-local content),
    copies/moves/deletes via shutil on those paths, and verifies the operations
    succeed and produce expected results:
      - copy: file copied, size matches, content matches post-yield
      - move: file moved, old path gone, new path exists, content matches
      - rmtree: directory removed, no longer exists
    A single failed operation -> H.fail "shutil operation failed".  All paths are
    fiber-local, so NO cross-fiber interference (unless a leak/corruption bug).

  * COMPLETENESS (post, HARD): require_no_lost -- no fiber vanished mid-copy
    (stranded in an open file handle, or blocked on an FD).

  * SECONDARY (report-only, NEVER fails): operation success rates by fiber.
    Measured to confirm the hazard (distinct, disjoint file operations per fiber)
    is exercised at scale.

FAIL ON: shutil operation raised OSError, file size/content mismatch after copy,
moved file doesn't exist at new path, rmtree failed to delete, or post-yield
re-read shows wrong content (a sibling corrupted this fiber's temp file).
NEVER fail on operation counts (this is mostly stateless; 100% success is
expected).

EXPECTED RESULT: this NEGATIVE CONTROL is expected to PASS (exit 0) under both
plain threads (GIL on/off) and runloom M:N.  If it FAILS, it indicates a real
file-descriptor leak, tempfile bookkeeping corruption, or linecache desync
under M:N (unlikely; shutil is pure Python and most of its state is disjoint
per fiber).  If the SECONDARY arm reports high failure rates, it could signal
resource exhaustion (too many FDs open / temp files not cleaned up).

Stresses: shutil.copy / shutil.move / shutil.rmtree across hub fiber yields and
potential hub migrations, per-fiber distinct temporary file paths (NO sharing),
linecache usage inside shutil (if it reads source files for any reason),
tempfile state if shutil uses it for atomic renames, file descriptor lifecycle
and cleanup under concurrent fiber operations, distinct file I/O per fiber with
no cross-fiber file mutation.

Good TSan / controlled-M:N-replay target: shutil operations call low-level
syscalls (read, write, unlink, rename) on fiber-local file descriptors; a
data-race on any FD table entry across hubs, or a replay that migrates a hub
during an open() / close() / stat() sequence inside shutil, isolates the issue
before the post-yield re-read oracle fires.
"""
import os
import shutil
import tempfile

import harness
import runloom


def make_test_content(wid, idx):
    """Generate deterministic, fiber-local test content for file operations."""
    return "wid={0} idx={1} content_marker_{0}_{1}\n".format(wid, idx).encode("utf-8")


def make_src_path(tmpdir, wid, idx):
    """Fiber-local source file path (unique per wid + idx)."""
    return os.path.join(tmpdir, "src_{0}_{1}.txt".format(wid, idx))


def make_dst_path(tmpdir, wid, idx):
    """Fiber-local destination file path (unique per wid + idx)."""
    return os.path.join(tmpdir, "dst_{0}_{1}.txt".format(wid, idx))


def make_moved_path(tmpdir, wid, idx):
    """Fiber-local moved file path (unique per wid + idx)."""
    return os.path.join(tmpdir, "moved_{0}_{1}.txt".format(wid, idx))


def make_subdir_path(tmpdir, wid, idx):
    """Fiber-local subdirectory path (unique per wid + idx)."""
    return os.path.join(tmpdir, "subdir_{0}_{1}".format(wid, idx))


def setup(H):
    tmpdir = H.make_tmpdir(prefix="p497_shutil_")
    H.state = {
        "tmpdir": tmpdir,
        "copy_checks": [0] * 1024,      # shutil.copy operations attempted
        "copy_fails": [0] * 1024,       # copy raised or content mismatch
        "move_checks": [0] * 1024,      # shutil.move operations attempted
        "move_fails": [0] * 1024,       # move raised or post-move mismatch
        "rmtree_checks": [0] * 1024,    # shutil.rmtree operations attempted
        "rmtree_fails": [0] * 1024,     # rmtree raised or dir still exists
        "sample": [None],               # first observed failure
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: DISTINCT-FILE SHUTIL OPERATIONS.  Each fiber constructs
# fiber-local paths and performs copy/move/rmtree on those paths, verifying
# operations succeed and produce expected results.  All paths are disjoint
# (fiber-local, derived from wid + idx), so failures indicate a leak/corruption
# bug, not expected M:N shared-object behavior.
# --------------------------------------------------------------------------
def copy_check(H, wid, idx, state):
    """Test shutil.copy on fiber-local files.

    Create a source file with fiber-local content, copy it to a destination,
    verify the copy succeeded, re-read after a yield to confirm post-migration
    stability, and verify content matches.
    """
    tmpdir = state["tmpdir"]
    src = make_src_path(tmpdir, wid, idx)
    dst = make_dst_path(tmpdir, wid, idx)

    try:
        # Create source file with fiber-local content.
        content = make_test_content(wid, idx)
        with open(src, "wb") as f:
            f.write(content)

        # Copy the file.
        shutil.copy(src, dst)

        # Verify destination exists and has correct size.
        if not os.path.exists(dst):
            H.fail("shutil.copy({0}, {1}): destination does not exist after copy "
                   "(wid {2} idx {3})".format(src, dst, wid, idx))
            state["copy_fails"][wid & 1023] += 1
            return

        # Verify size matches.
        src_size = os.path.getsize(src)
        dst_size = os.path.getsize(dst)
        if dst_size != src_size:
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "copy_size", src_size, dst_size)
            H.fail("shutil.copy size mismatch: src {0} bytes, dst {1} bytes "
                   "(wid {2} idx {3}) -- copied file has wrong size".format(
                       src_size, dst_size, wid, idx))
            state["copy_fails"][wid & 1023] += 1
            return

        # YIELD + SLEEP: migrate/deschedule, allowing sibling operations.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0002)

        # Re-read after yield: destination must still exist and match.
        if not os.path.exists(dst):
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "copy_post_yield_vanished", dst)
            H.fail("shutil.copy destination VANISHED after yield: {0} "
                   "(wid {1} idx {2}) -- post-migration, copy'd file is gone "
                   "(possible linecache corruption or tempfile cleanup bug)".format(
                       dst, wid, idx))
            state["copy_fails"][wid & 1023] += 1
            return

        # Verify post-yield content and size still match.
        reread_size = os.path.getsize(dst)
        if reread_size != src_size:
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "copy_post_yield_size", src_size, reread_size)
            H.fail("shutil.copy size CHANGED after yield: was {0}, now {1} "
                   "(wid {2} idx {3}) -- post-migration, copied file's size "
                   "changed (sibling operation corrupted it?)".format(
                       src_size, reread_size, wid, idx))
            state["copy_fails"][wid & 1023] += 1
            return

        with open(dst, "rb") as f:
            reread_content = f.read()
        if reread_content != content:
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "copy_post_yield_content", len(content), len(reread_content))
            H.fail("shutil.copy content CHANGED after yield (wid {0} idx {1}) "
                   "-- post-migration, copied file's content was corrupted "
                   "(expected {2} bytes, got {3})".format(
                       wid, idx, len(content), len(reread_content)))
            state["copy_fails"][wid & 1023] += 1
            return

        state["copy_checks"][wid & 1023] += 1

    except OSError as exc:
        if state["sample"][0] is None:
            state["sample"][0] = (wid, idx, "copy_oserror", str(exc))
        H.fail("shutil.copy raised OSError (wid {0} idx {1}): {2} -- "
               "file operation failed (src {3}, dst {4})".format(
                   wid, idx, exc, src, dst))
        state["copy_fails"][wid & 1023] += 1
    except Exception as exc:
        if state["sample"][0] is None:
            state["sample"][0] = (wid, idx, "copy_exception", type(exc).__name__)
        H.fail("shutil.copy raised {0} (wid {1} idx {2}): {3}".format(
            type(exc).__name__, wid, idx, exc))
        state["copy_fails"][wid & 1023] += 1
    finally:
        # Cleanup: remove src if it still exists (fiber-local, safe).
        try:
            os.unlink(src)
        except OSError:
            pass
        try:
            os.unlink(dst)
        except OSError:
            pass


def move_check(H, wid, idx, state):
    """Test shutil.move on fiber-local files.

    Create a source file, move it to a new location, verify the move succeeded
    (old path gone, new path exists), yield and re-check, and verify content
    matches post-migration.
    """
    tmpdir = state["tmpdir"]
    src = make_src_path(tmpdir, wid, idx)
    dst = make_moved_path(tmpdir, wid, idx)

    try:
        # Create source file with fiber-local content.
        content = make_test_content(wid, idx)
        with open(src, "wb") as f:
            f.write(content)

        # Move the file.
        shutil.move(src, dst)

        # Verify source is gone and destination exists.
        if os.path.exists(src):
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "move_src_still_exists", src)
            H.fail("shutil.move: source file still exists after move (wid {0} idx {1}) "
                   "-- {2} should be gone".format(wid, idx, src))
            state["move_fails"][wid & 1023] += 1
            return

        if not os.path.exists(dst):
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "move_dst_missing", dst)
            H.fail("shutil.move destination does not exist (wid {0} idx {1}) "
                   "-- move to {2} failed".format(wid, idx, dst))
            state["move_fails"][wid & 1023] += 1
            return

        src_size = os.path.getsize(dst)  # Check the new location's size.

        # YIELD + SLEEP: migrate/deschedule.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0002)

        # Re-read after yield: moved file must still be at dst, not src.
        if os.path.exists(src):
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "move_src_reappeared", src)
            H.fail("shutil.move source REAPPEARED after yield (wid {0} idx {1}) "
                   "-- moved file shouldn't be at original location after "
                   "migration".format(wid, idx))
            state["move_fails"][wid & 1023] += 1
            return

        if not os.path.exists(dst):
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "move_dst_vanished", dst)
            H.fail("shutil.move destination VANISHED after yield (wid {0} idx {1}) "
                   "-- moved file is gone post-migration (possible tempfile "
                   "cleanup bug)".format(wid, idx))
            state["move_fails"][wid & 1023] += 1
            return

        # Verify post-yield content matches.
        reread_size = os.path.getsize(dst)
        if reread_size != src_size:
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "move_post_yield_size", src_size, reread_size)
            H.fail("shutil.move size CHANGED after yield (wid {0} idx {1}) "
                   "-- was {2} bytes, now {3}".format(wid, idx, src_size, reread_size))
            state["move_fails"][wid & 1023] += 1
            return

        with open(dst, "rb") as f:
            reread_content = f.read()
        if reread_content != content:
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "move_post_yield_content", len(content), len(reread_content))
            H.fail("shutil.move content CHANGED after yield (wid {0} idx {1})".format(wid, idx))
            state["move_fails"][wid & 1023] += 1
            return

        state["move_checks"][wid & 1023] += 1

    except OSError as exc:
        if state["sample"][0] is None:
            state["sample"][0] = (wid, idx, "move_oserror", str(exc))
        H.fail("shutil.move raised OSError (wid {0} idx {1}): {2}".format(wid, idx, exc))
        state["move_fails"][wid & 1023] += 1
    except Exception as exc:
        if state["sample"][0] is None:
            state["sample"][0] = (wid, idx, "move_exception", type(exc).__name__)
        H.fail("shutil.move raised {0} (wid {1} idx {2}): {3}".format(
            type(exc).__name__, wid, idx, exc))
        state["move_fails"][wid & 1023] += 1
    finally:
        # Cleanup: remove moved file if it exists.
        try:
            os.unlink(src)
        except OSError:
            pass
        try:
            os.unlink(dst)
        except OSError:
            pass


def rmtree_check(H, wid, idx, state):
    """Test shutil.rmtree on fiber-local directories.

    Create a temporary subdirectory with some files, remove it via rmtree,
    verify it's gone, yield, and re-check that it stays gone.
    """
    tmpdir = state["tmpdir"]
    subdir = make_subdir_path(tmpdir, wid, idx)

    try:
        # Create subdirectory with a few files.
        os.makedirs(subdir, exist_ok=True)
        for i in range(3):
            file_path = os.path.join(subdir, "file_{0}_{1}.txt".format(wid, i))
            with open(file_path, "wb") as f:
                f.write("wid={0} file={1}\n".format(wid, i).encode("utf-8"))

        # Remove the directory tree.
        shutil.rmtree(subdir)

        # Verify the directory is gone.
        if os.path.exists(subdir):
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "rmtree_still_exists", subdir)
            H.fail("shutil.rmtree directory still exists after removal (wid {0} idx {1}) "
                   "-- {2} should be gone".format(wid, idx, subdir))
            state["rmtree_fails"][wid & 1023] += 1
            return

        # YIELD + SLEEP: migrate/deschedule.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0002)

        # Re-check after yield: directory must still be gone.
        if os.path.exists(subdir):
            if state["sample"][0] is None:
                state["sample"][0] = (wid, idx, "rmtree_reappeared", subdir)
            H.fail("shutil.rmtree directory REAPPEARED after yield (wid {0} idx {1}) "
                   "-- deleted directory came back post-migration (cleanup bug?)".format(
                       wid, idx))
            state["rmtree_fails"][wid & 1023] += 1
            return

        state["rmtree_checks"][wid & 1023] += 1

    except OSError as exc:
        if state["sample"][0] is None:
            state["sample"][0] = (wid, idx, "rmtree_oserror", str(exc))
        H.fail("shutil.rmtree raised OSError (wid {0} idx {1}): {2}".format(wid, idx, exc))
        state["rmtree_fails"][wid & 1023] += 1
    except Exception as exc:
        if state["sample"][0] is None:
            state["sample"][0] = (wid, idx, "rmtree_exception", type(exc).__name__)
        H.fail("shutil.rmtree raised {0} (wid {1} idx {2}): {3}".format(
            type(exc).__name__, wid, idx, exc))
        state["rmtree_fails"][wid & 1023] += 1
    finally:
        # Cleanup: force-remove subdir if it still exists.
        try:
            if os.path.exists(subdir):
                shutil.rmtree(subdir, ignore_errors=True)
        except Exception:
            pass


# Bounded inner loop: each worker runs multiple checks until H.running() or
# INNER_CAP is hit.  This allows the oracle to fire at --rounds 1 (default).
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs LOAD-BEARING copy/move/rmtree checks on distinct temp
    files (derived from wid + iteration counter).  All file paths are fiber-
    local, so failures indicate a bug, not expected M:N shared-object behavior.
    """
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            copy_check(H, wid, idx, state)
            if H.failed:
                return
            move_check(H, wid, idx, state)
            if H.failed:
                return
            rmtree_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    copy_checks = sum(H.state["copy_checks"])
    copy_fails = sum(H.state["copy_fails"])
    move_checks = sum(H.state["move_checks"])
    move_fails = sum(H.state["move_fails"])
    rmtree_checks = sum(H.state["rmtree_checks"])
    rmtree_fails = sum(H.state["rmtree_fails"])
    total_ops = copy_checks + move_checks + rmtree_checks
    total_fails = copy_fails + move_fails + rmtree_fails
    fail_pct = (100.0 * total_fails / total_ops) if total_ops else 0.0
    sample = H.state["sample"][0]

    H.log("shutil[LOAD-BEARING]: copy={0} fail={1}  move={2} fail={3}  "
          "rmtree={4} fail={5}  total_ops={6} total_fails={7} ({8:.2f}%) "
          "sample={9}".format(
              copy_checks, copy_fails, move_checks, move_fails,
              rmtree_checks, rmtree_fails, total_ops, total_fails,
              fail_pct, sample))

    # NON-VACUITY: the load-bearing hazard was exercised (operations ran).
    H.check(total_ops > 0,
            "no shutil operations ran -- the load-bearing distinct-file "
            "operations hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber vanished mid-operation (stranded on an FD).
    H.require_no_lost("shutil file operations")

    # NEGATIVE CONTROL: no failures expected (all operations are on disjoint
    # fiber-local files; failures indicate a real bug, not expected behavior).
    if total_fails > 0:
        H.log("note: observed {0} shutil operation failures ({1:.2f}% of {2} ops) "
              "-- expected 0 for disjoint fiber-local files. This may indicate "
              "file-descriptor leaks, tempfile cleanup bugs, or resource "
              "exhaustion under M:N.".format(total_fails, fail_pct, total_ops))


if __name__ == "__main__":
    harness.main(
        "p497_shutil", body, setup=setup, post=post,
        default_funcs=8000,
        describe="shutil.copy / .move / .rmtree on disjoint fiber-local temp files. "
                 "Each fiber constructs unique paths (no sharing) and performs file "
                 "operations across scheduler yields + hub migrations. All paths are "
                 "fiber-local (derived from wid + idx), so operations should never "
                 "race or corrupt each other. LOAD-BEARING: copy/move/rmtree must "
                 "succeed (0 OSError, correct size/content post-yield, moved/deleted "
                 "files stay at their final state). Expected to PASS on correct "
                 "runtime (plain threads GIL on/off AND runloom M:N) -- failures "
                 "indicate file-descriptor leaks or tempfile corruption under M:N, "
                 "not documented shared-object behavior (all files are disjoint)."
    )
