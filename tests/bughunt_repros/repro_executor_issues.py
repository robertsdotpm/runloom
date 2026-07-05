"""CoThreadPoolExecutor divergences:

(1) shutdown(cancel_futures=True) is accepted but IGNORED: queued futures are
    never cancelled and their tasks all run (stdlib cancels not-yet-started
    futures and result() raises CancelledError).
(2) _pending grows without bound: one CoEvent per submit(), never pruned until
    shutdown() -- a long-lived executor leaks memory proportional to submits.
(3) submit() from a non-fiber context with no scheduler running never resolves
    (stdlib ThreadPoolExecutor works anywhere).
"""
import runloom.monkey as monkey
monkey.patch()

import time
import concurrent.futures as cf
import runloom_c as rc

out = {}


def main():
    # ---- (1) cancel_futures ignored ----
    ex = cf.ThreadPoolExecutor(max_workers=1)
    ran = []
    f1 = ex.submit(lambda: (time.sleep(0.3), ran.append(1)))
    f2 = ex.submit(lambda: ran.append(2))     # queued behind f1 (1 worker)
    ex.shutdown(wait=True, cancel_futures=True)
    out["f2_cancelled"] = f2.cancelled()
    out["ran"] = list(ran)

    # ---- (2) _pending unbounded growth ----
    ex2 = cf.ThreadPoolExecutor(max_workers=4)
    for i in range(10000):
        ex2.submit(lambda: None).result()
    out["pending_len"] = len(ex2._pending)


rc.fiber(main)
rc.run()

print("(1) f2.cancelled() =", out["f2_cancelled"],
      "(stdlib: True); tasks that ran:", out["ran"], "(stdlib: [1])")
print("(2) len(_pending) after 10000 completed submits:", out["pending_len"],
      "(stdlib equivalent: 0 -- unbounded leak)")

# ---- (3) executor unusable outside a running scheduler ----
ex3 = cf.ThreadPoolExecutor(max_workers=2)
f = ex3.submit(lambda: 42)
try:
    r = f.result(timeout=2)
    print("(3) result from non-fiber main thread:", r, "(ok)")
except Exception as e:
    print("(3) BUG: submit() from plain main thread never resolves:",
          type(e).__name__)
