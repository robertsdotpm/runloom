"""aio bridge + runloom.time torture."""
import sys, time
import runloom

mode = sys.argv[1]

if mode == "aio":
    import asyncio
    async def main():
        # task churn + cancellation
        async def w(i):
            await asyncio.sleep(0.001)
            return i * 3
        results = await asyncio.gather(*[w(i) for i in range(500)])
        assert results == [i * 3 for i in range(500)]
        # cancellation storm
        async def sleeper():
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                raise
        tasks = [asyncio.create_task(sleeper()) for _ in range(200)]
        await asyncio.sleep(0.05)
        for t in tasks:
            t.cancel()
        got = await asyncio.gather(*tasks, return_exceptions=True)
        ncan = sum(1 for g in got if isinstance(g, asyncio.CancelledError))
        assert ncan == 200, ncan
        # exceptions
        async def bad():
            raise ValueError("x")
        try:
            await bad()
        except ValueError:
            pass
        print("aio torture OK")
    runloom.aio.run(main())

elif mode == "aio_loop":
    import asyncio
    # repeated aio.run cycles: leak / state carryover check
    import gc, os
    def rss():
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS"): return int(line.split()[1])
    async def m():
        await asyncio.sleep(0)
        return 42
    for _ in range(20):
        assert runloom.aio.run(m()) == 42
    gc.collect(); r0 = rss()
    for _ in range(200):
        assert runloom.aio.run(m()) == 42
    gc.collect(); r1 = rss()
    print("aio.run cycles: rss %d->%d (%.2f kB/iter)" % (r0, r1, (r1 - r0) / 200.0))

elif mode == "time":
    def main():
        t0 = time.monotonic()
        # After
        ch = runloom.time.After(0.05)
        v, ok = ch.recv()
        dt = time.monotonic() - t0
        assert 0.04 < dt < 0.5, dt
        # Ticker
        tk = runloom.time.Ticker(0.02)
        n = 0
        t1 = time.monotonic()
        for _ in range(5):
            tk.c.recv() if hasattr(tk, "c") else None
            n += 1
        tk.stop() if hasattr(tk, "stop") else None
        print("time OK After=%.3fs ticker5=%.3fs" % (dt, time.monotonic() - t1))
    runloom.run(4, main)

elif mode == "timer_storm":
    def main():
        done = runloom.Chan(512)
        N = 2000
        def w(i):
            runloom.sleep(0.001 * (i % 20))
            done.send(i)
        for i in range(N):
            runloom.fiber(w, i)
        def collect():
            seen = set()
            for _ in range(N):
                v, ok = done.recv()
                seen.add(v)
            assert len(seen) == N
            print("timer storm OK")
        runloom.fiber(collect)
    runloom.run(8, main)
