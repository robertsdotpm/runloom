import threading
import runloom_c

def body():
    for _ in range(100000):
        runloom_c.yield_()

c = runloom_c.Coro(body)

def spin():
    while not c.done:
        try:
            c.resume()
        except RuntimeError:
            pass   # guard fired cleanly -- fine

ts = [threading.Thread(target=spin) for _ in range(8)]
for t in ts: t.start()
for t in ts: t.join()
print('done without crash')
