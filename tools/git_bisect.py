#!/usr/bin/env python3
"""bisect.py -- auto-blame a repro to its introducing commit (Chromium Findit /
OSS-Fuzz + ClusterFuzz bisection; QA-steal rank 12).

Wraps `git bisect run`: for each candidate commit it rebuilds the ext and runs
YOUR repro command, so a minimized finding (a tests/bughunt_repros case, a
lifefuzz/DST seed, a regression) becomes a blamed commit with no manual bisecting
-- fitting the no-hosted-CI local-gate model.

  tools/bisect.py --good <sha> --bad <sha> [--timeout S] -- <repro command...>

  # e.g. blame a lifefuzz seed:
  tools/bisect.py --good v4.0.0 --bad HEAD --timeout 30 -- \
      python tools/lifefuzz/lifefuzz.py repro 12345 --mn-seed 1

The repro command must EXIT 0 when the bug is ABSENT (good) and NONZERO when it is
PRESENT (bad).  A signal/timeout counts as bad.  A commit that fails to BUILD is
`git bisect skip`ped.  The working branch/HEAD is restored on exit.

--no-build skips the rebuild (repro doesn't need the ext rebuilt / faster).
--python picks the interpreter for the build (default 3.14.4t).
"""
import argparse
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_PY = os.path.expanduser("~/.pyenv/versions/3.14.4t/bin/python3")


def _run(cmd, **kw):
    return subprocess.run(cmd, cwd=ROOT, **kw)


def current_ref():
    r = _run(["git", "symbolic-ref", "--quiet", "--short", "HEAD"],
             capture_output=True, text=True)
    if r.returncode == 0 and r.stdout.strip():
        return r.stdout.strip()
    return _run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--good", required=True, help="a commit where the bug is ABSENT")
    ap.add_argument("--bad", default="HEAD", help="a commit where the bug is PRESENT")
    ap.add_argument("--timeout", type=float, default=60.0, help="per-repro seconds (hang=bad)")
    ap.add_argument("--python", default=DEFAULT_PY)
    ap.add_argument("--no-build", action="store_true", help="skip the ext rebuild")
    ap.add_argument("repro", nargs=argparse.REMAINDER,
                    help="-- <command...> : exits 0 when the bug is absent")
    args = ap.parse_args()

    repro = args.repro
    if repro and repro[0] == "--":
        repro = repro[1:]
    if not repro:
        ap.error("give the repro command after --")

    # The per-commit runner: build (skip=125 on failure), then run the repro under
    # a timeout, mapping the result to git bisect's contract (0 good / 1 bad /
    # 125 skip).  Written as a script git bisect run invokes at each commit.
    build = "true" if args.no_build else (
        "%s setup.py build_ext --inplace --force >/tmp/bisect_build.log 2>&1"
        % _sh(args.python))
    runner = os.path.join("/tmp", "runloom_bisect_run.sh")
    with open(runner, "w") as f:
        f.write("#!/usr/bin/env bash\nset +e\ncd %s\n" % _sh(ROOT))
        f.write("PYTHON_GIL=0 PYTHONPATH=src\nexport PYTHON_GIL PYTHONPATH\n")
        f.write("%s || exit 125   # un-buildable commit -> skip\n" % build)
        f.write("timeout %s %s\nrc=$?\n" % (args.timeout, " ".join(_sh(a) for a in repro)))
        f.write("if [ $rc -ge 128 ]; then exit 1; fi   # signal/timeout -> bad\n")
        f.write("exit $rc\n")
    os.chmod(runner, 0o755)

    restore = current_ref()
    print("bisect: good=%s bad=%s repro=%r (restore -> %s)"
          % (args.good, args.bad, " ".join(repro), restore))
    # Stash uncommitted TRACKED changes so bisect's per-commit checkouts are clean
    # (this repo's soak daemons continuously touch docs/dev/soak/*).  Restored in
    # finally.  NOTE: if a daemon re-dirties the tree DURING a slow (building)
    # bisect, a checkout can still fail -- pause the daemons for a build bisect.
    dirty = (_run(["git", "diff", "--quiet"]).returncode != 0
             or _run(["git", "diff", "--cached", "--quiet"]).returncode != 0)
    stashed = dirty and _run(["git", "stash", "push", "-q", "-m", "bisect-autostash"],
                             capture_output=True).returncode == 0
    rc = 1
    try:
        if _run(["git", "bisect", "start", args.bad, args.good]).returncode != 0:
            print("bisect: `git bisect start` failed (dirty tree? bad refs?)")
            return 2
        r = _run(["git", "bisect", "run", runner])
        rc = r.returncode
    finally:
        _run(["git", "bisect", "reset", restore])
        if stashed:
            _run(["git", "stash", "pop", "-q"], capture_output=True)
    print("bisect: done (git bisect run rc=%d) -- the first-bad commit is printed above" % rc)
    return 0 if rc == 0 else 1


def _sh(s):
    """Minimal shell-quote."""
    if s and all(c.isalnum() or c in "-_./=:" for c in s):
        return s
    return "'" + s.replace("'", "'\\''") + "'"


if __name__ == "__main__":
    sys.exit(main())
