"""big_100 / 195 -- audit-hook reentrancy stress.

A `sys.addaudithook` callback (installed at IMPORT time) fires on every audited
operation in the interpreter.  Under M:N, audited operations happen
concurrently across hubs, so the hook fires REENTRANTLY from many goroutines at
once.  The callback here is deliberately MINIMAL -- it buckets events by a
coarse prefix into per-bucket counters and returns -- because a hook cannot be
removed once added and any heavy/recursive work in it would be ruinous.

Goroutines perform audited operations (open files in a tmpdir, open sockets,
import modules round-robin) and the hook must keep counting without crashing,
deadlocking, or recursing.

Stresses: sys.addaudithook reentrancy under M:N, the audit subsystem's
thread-safety with the GIL off.
"""
import importlib
import os
import socket as _socket
import sys

import harness
import runloom

# --- audit hook: installed at IMPORT time, ONE cheap counting callback. ------
# Buckets are fixed up front so the hook never allocates / never recurses.
BUCKETS = ("open", "socket", "import", "subprocess", "other")
BUCKET_INDEX = {name: i for i, name in enumerate(BUCKETS)}
# A plain list of ints.  The hook does `lst[i] += 1` -- this is a
# read-modify-write that can lose increments with the GIL off, so this count is
# a LOWER-BOUND "the hook fired" signal, never a conservation quantity.  We only
# assert it is > 0, which is robust to lost increments.
AUDIT_COUNTS = [0] * len(BUCKETS)
AUDIT_TOTAL = [0]


def audit_hook(event, args):
    # Keep this as cheap as possible: a couple of prefix checks, one increment.
    if event.startswith("open"):
        i = 0
    elif event.startswith("socket."):
        i = 1
    elif event.startswith("import"):
        i = 2
    elif event.startswith("subprocess."):
        i = 3
    else:
        i = 4
    AUDIT_COUNTS[i] += 1
    AUDIT_TOTAL[0] += 1


sys.addaudithook(audit_hook)

# Modules the workers re-import (already loaded; re-importing still fires the
# `import` audit event and exercises the import machinery under the hook).
IMPORT_TARGETS = ["json", "base64", "hashlib", "struct", "math", "zlib",
                  "binascii", "random"]


def worker(H, wid, rng, state):
    tmpdir = state["tmpdir"]
    host = state["host"]
    path = os.path.join(tmpdir, "f{0}.bin".format(wid & 1023))
    for _ in H.round_range():
        action = rng.randrange(3)
        try:
            if action == 0:
                # open() fires the `open` audit event.
                with open(path, "wb") as f:
                    f.write(b"x" * 16)
                with open(path, "rb") as f:
                    f.read()
            elif action == 1:
                # socket creation fires the `socket.__new__` / `socket.bind` etc.
                s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
                try:
                    s.bind((host, 0))
                finally:
                    s.close()
            else:
                # import fires the `import` audit event.
                name = IMPORT_TARGETS[wid % len(IMPORT_TARGETS)]
                mod = importlib.import_module(name)
                # Use it so the import isn't optimised away.
                if not H.check(mod is not None, "import returned None"):
                    return
        except OSError:
            if not H.running():
                break
            continue
        H.op(wid)
        if rng.random() < 0.2:
            runloom.yield_now()
        H.task_done(wid)


def setup(H):
    H.state = {"tmpdir": H.make_tmpdir("p195_"), "host": H.net_ip(0)}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    total = AUDIT_TOTAL[0]
    # The hook must have fired (the whole point); we assert >0 not an exact
    # count because the cross-goroutine increments race (lower-bound signal).
    H.check(total > 0, "the audit hook never fired (count stuck at 0)")
    # At least one event bucket beyond 'other' should have registered.
    H.check(sum(AUDIT_COUNTS[:4]) > 0,
            "no recognised audited operation (open/socket/import) was seen")
    H.log("audit_total={0} buckets={1}".format(
        total, dict(zip(BUCKETS, AUDIT_COUNTS))))


if __name__ == "__main__":
    harness.main("p195_audit_hook_stress", body, setup=setup, post=post,
                 default_funcs=1000,
                 describe="sys.addaudithook fires reentrantly under M:N; cheap "
                          "counting hook never crashes/recurses/deadlocks")
