"""asyncio.run_coroutine_threadsafe from a foreign thread into a running
runloom loop + signal handler smoke test."""
import sys, threading, asyncio, time, signal, os
import runloom.aio as aio

loop = aio.RunloomEventLoop()
asyncio.set_event_loop(loop)
got = []

async def coro(x):
    await asyncio.sleep(0.01)
    return x * 2

def foreign():
    for i in range(20):
        f = asyncio.run_coroutine_threadsafe(coro(i), loop)
        got.append(f.result(timeout=10))

async def main():
    t = threading.Thread(target=foreign)
    t.start()
    # signal handler check while loop runs
    hits = []
    loop.add_signal_handler(signal.SIGUSR1, hits.append, 1)
    os.kill(os.getpid(), signal.SIGUSR1)
    await asyncio.sleep(0.5)
    while t.is_alive():
        await asyncio.sleep(0.05)
    t.join()
    loop.remove_signal_handler(signal.SIGUSR1)
    return hits

hits = loop.run_until_complete(main())
loop.close()
print("results:", got == [i * 2 for i in range(20)], "signal hits:", hits)
if got != [i * 2 for i in range(20)] or hits != [1]:
    print("BUG")
    sys.exit(1)
print("OK")
