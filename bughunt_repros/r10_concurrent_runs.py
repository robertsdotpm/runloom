"""Historical bug: concurrent aio.run() across OS threads crashing.
Stress: 16 threads x 20 sequential runs each, with sleeps, tasks, sockets."""
import sys, threading, asyncio
import runloom.aio as aio

errors = []

def worker(tid):
    try:
        for i in range(20):
            async def main():
                async def sub(n):
                    await asyncio.sleep(0.001)
                    return n
                r = await asyncio.gather(*[sub(k) for k in range(10)])
                assert r == list(range(10))
                return sum(r)
            assert aio.run(main()) == 45
    except BaseException as e:
        errors.append((tid, repr(e)))

threads = [threading.Thread(target=worker, args=(t,)) for t in range(16)]
for t in threads: t.start()
for t in threads: t.join(timeout=60)
alive = [t for t in threads if t.is_alive()]
print("errors:", errors[:5], "alive:", len(alive))
if errors or alive:
    print("BUG: concurrent runs failed/hung")
    sys.exit(1)
print("OK")
