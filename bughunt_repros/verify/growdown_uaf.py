"""Stress the borrowed-ref window in m_fiber_grow (module_run.c.inc:500).

A mutator thread keeps replacing fn.__dict__["runloom_stack"] with a fresh
frozen store list (the same first-write that grow_down_prepare's unlocked
`d[GROW_DOWN_KEY] = store` race performs in-tree).  Each replacement drops the
old list's ONLY reference, freeing it immediately.  Hub threads concurrently
run the C fast path, which fetches the store as a BORROWED ref via
PyDict_GetItemWithError and then calls PyList_GET_SIZE / PyList_GetItemRef /
PyLong_AsLong on it.  If the replacement lands inside that window, the C code
operates on a freed list -> UAF (crash, or garbage learned stack size).
"""
import sys
import threading

import runloom

K = "runloom_stack"
FROZEN_SIZE = 1 << 16   # 64 KiB, valid learned size
FROZEN_CNT = 64         # >= GROW_DOWN_SAMPLES -> C frozen fast path

def fn():
    pass

STOP = threading.Event()

def mutator():
    d = fn.__dict__
    while not STOP.is_set():
        # fresh list each time; the old one is freed on replacement
        d[K] = [FROZEN_SIZE, FROZEN_CNT]

def spawner(n):
    f = runloom.fiber
    for _ in range(n):
        f(fn)

def main():
    fn.__dict__[K] = [FROZEN_SIZE, FROZEN_CNT]
    for _ in range(8):
        runloom.fiber(lambda: spawner(200000))

muts = [threading.Thread(target=mutator, daemon=True) for _ in range(2)]
for t in muts:
    t.start()
try:
    runloom.run(8, main)
finally:
    STOP.set()
print("completed without crash")
