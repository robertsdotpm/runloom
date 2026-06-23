"""Per-g-tstate channel-churn A/B repro (the alloc-home patch validation).

Run with the PATCHED python (built from patches/cpython313t-tstate-alloc-home.patch)
+ runloom built against it:

  FIX (borrow on):  RUNLOOM_PER_G_TSTATE=1 RUNLOOM_ALLOW_UNSAFE_MIGRATION=1 \
                    PYTHON_GIL=0 PYTHONPATH=src <patched-python> mpmc_pergt_repro.py
                    -> PASS (24/24)
  BASELINE (off):   ... RUNLOOM_NO_ALLOC_HOME=1 ...  -> 8/8 abort (_mi_page_retire/qsbr)

6 producers / 5 consumers / 1 bounded chan, 4 hubs, 15 inner rounds.
"""
import runloom_c
def once(it):
    nprod, ncons, per, cap = 6, 5, 50, 8
    ch=runloom_c.Chan(cap); done=runloom_c.Chan(nprod); res=runloom_c.Chan(ncons)
    def prod(pid):
        def r():
            for s in range(per): ch.send((pid,s))
            done.send(1)
        return r
    def closer():
        for _ in range(nprod): done.recv()
        ch.close()
    def cons():
        last={}; bad=0; c=0
        for (pid,s) in ch:
            if pid in last and s<=last[pid]: bad+=1
            last[pid]=s; c+=1
        res.send((c,bad))
    runloom_c.mn_init(4)
    for _ in range(ncons): runloom_c.mn_fiber(cons)
    for p in range(nprod): runloom_c.mn_fiber(prod(p))
    runloom_c.mn_fiber(closer)
    runloom_c.mn_run()
    tot_c=tot_bad=0
    for _ in range(ncons):
        g=res.try_recv()
        if g is None: break
        (c,bad),ok=g; tot_c+=c; tot_bad+=bad
    runloom_c.mn_fini()
    assert tot_c==nprod*per, ("lost/dup",tot_c)
    assert tot_bad==0, ("FIFO",tot_bad)
for it in range(15): once(it)
print("PASS")
