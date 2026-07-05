"""run() misuse: sequential re-runs, concurrent run() from OS threads, nested run."""
import sys, threading, traceback
import runloom

mode = sys.argv[1]

if mode == "seq":
    for i in range(5):
        out = []
        runloom.run(1, lambda: out.append(1))
        assert out == [1], out
    for i in range(5):
        out = []
        runloom.run(4, lambda: out.append(1))
        assert out == [1], out
    # alternate modes
    for i in range(4):
        out = []
        runloom.run(1 if i % 2 else 4, lambda: out.append(1))
        assert out == [1], out
    print("sequential re-run OK")

elif mode == "threads1":
    # concurrent run(1) from multiple OS threads
    errs = []
    oks = []
    def t(i):
        try:
            out = []
            runloom.run(1, lambda: out.append(i))
            assert out == [i]
            oks.append(i)
        except Exception as e:
            errs.append((i, repr(e)))
    ths = [threading.Thread(target=t, args=(i,)) for i in range(8)]
    [x.start() for x in ths]
    [x.join() for x in ths]
    print("threads1 oks=%d errs=%r" % (len(oks), errs[:3]))

elif mode == "threadsN":
    # concurrent run(4) from multiple OS threads -- expect a clean error, not hang/crash
    errs = []
    oks = []
    def t(i):
        try:
            runloom.run(2, lambda: None)
            oks.append(i)
        except Exception as e:
            errs.append((i, repr(e)))
    ths = [threading.Thread(target=t, args=(i,)) for i in range(6)]
    [x.start() for x in ths]
    [x.join() for x in ths]
    print("threadsN oks=%d errs=%d %r" % (len(oks), len(errs), errs[:2]))

elif mode == "nested1":
    # run(1) nested inside a fiber (docstring says supported)
    out = []
    def inner():
        out.append("inner")
    def outer():
        out.append("outer")
        runloom.run(1, inner)
        out.append("after")
    runloom.run(1, outer)
    print("nested1:", out)

elif mode == "nestedN":
    # run(4) inside a hub fiber should raise cleanly
    res = []
    def outer():
        try:
            runloom.run(4, lambda: None)
            res.append("no-raise")
        except RuntimeError as e:
            res.append("raised")
    runloom.run(4, outer)
    print("nestedN:", res)

elif mode == "run1_in_hub":
    # run(1) inside an M:N hub fiber -- what happens?
    res = []
    def outer():
        try:
            runloom.run(1, lambda: res.append("inner-ran"))
            res.append("returned")
        except Exception as e:
            res.append("raised:" + repr(e))
    runloom.run(4, outer)
    print("run1_in_hub:", res)
