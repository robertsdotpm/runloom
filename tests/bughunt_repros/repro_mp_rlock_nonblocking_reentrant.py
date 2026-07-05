"""_co_recursive_semlock_methods: a NON-BLOCKING re-entrant acquire returns
True but never increments the recursion count:

    if state[0] is not None and state[0] == cur:
        if not block:
            return True          # <-- count NOT bumped
        state[1] += 1
        return True

So acquire(); acquire(False); release() fully releases the lock (sem_post)
while the caller still believes it holds it -- mutual exclusion is broken.
stdlib / unpatched mp.RLock keeps count=2 and stays held after one release.

Expected: C-level _count()==1 after the sequence and a foreign non-blocking
acquire fails.  Observed (bug): _count()==0 and the foreign acquire SUCCEEDS.
"""
import multiprocessing as mp          # must be imported BEFORE patch()
import _thread
import time as _t

import runloom.monkey as monkey
monkey.patch()

import time
import runloom_c as rc

l = mp.RLock()
out = {}


def main():
    l.acquire()             # count = 1 (C sem taken)
    ok = l.acquire(False)   # re-entrant non-blocking -> should make count 2
    assert ok
    l.release()             # should leave count = 1 (still held)
    out["c_count"] = l._semlock._count()

    box = []
    def foreign():
        box.append(l.acquire(False))   # must be False: lock still held by us
    _thread.start_new_thread(foreign, ())
    t0 = time.monotonic()
    while not box and time.monotonic() - t0 < 3:
        time.sleep(0.01)
    out["foreign_got_lock"] = box[0] if box else "timeout"


rc.fiber(main)
rc.run()
print("C-level _count after acquire+acquire(False)+release:", out["c_count"],
      "(expected 1)")
print("foreign non-blocking acquire while 'held':", out["foreign_got_lock"],
      "(expected False)")
if out["c_count"] == 0 or out["foreign_got_lock"] is True:
    print("BUG CONFIRMED: re-entrant acquire(False) lost the recursion count; "
          "lock released while logically held")
