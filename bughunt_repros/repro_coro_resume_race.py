"""Two OS threads race Coro.resume() on the same Coro.  The `executing`
re-entrancy guard is a plain int check-then-set (no atomics, no lock), so on
free-threaded 3.13t both threads can pass the check and swapcontext into the
same coroutine context concurrently -> stack corruption / SIGSEGV."""
import threading
import runloom_c

def body():
    for _ in range(100000):
        runloom_c.yield_()

c = runloom_c.Coro(body)

errors = []
def spin():
    try:
        while not c.done:
            try:
                c.resume()
            except RuntimeError:
                pass   # the guard fired cleanly -- fine
    except BaseException as e:
        errors.append(e)

ts = [threading.Thread(target=spin) for _ in range(8)]
for t in ts: t.start()
for t in ts: t.join()
print("done without crash; errors:", errors[:3])
