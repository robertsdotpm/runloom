"""Channel ops from a foreign (non-fiber) OS thread while hubs run."""
import sys, threading, time
import runloom

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 4
N = 2000

got = []
lock = threading.Lock()

ch = runloom.Chan(4)
done = threading.Event()

def feeder():   # plain OS thread, not a fiber
    for i in range(N):
        ch.send(i)
    ch.close()

def main():
    def consumer():
        while True:
            v, ok = ch.recv()
            if not ok:
                break
            with lock:
                got.append(v)
    for _ in range(4):
        runloom.fiber(consumer)

t = threading.Thread(target=feeder)
t.start()
runloom.run(HUBS, main)
t.join()
assert len(got) == N and set(got) == set(range(N)), (len(got), "loss/dup")
print("foreign-thread send hubs=%d OK n=%d" % (HUBS, len(got)))

# reverse: fibers send, OS thread receives
ch2 = runloom.Chan(4)
got2 = []
def drainer():
    while True:
        v, ok = ch2.recv()
        if not ok:
            break
        got2.append(v)
t2 = threading.Thread(target=drainer)
t2.start()
def main2():
    def prod(base):
        for i in range(500):
            ch2.send(base + i)
    for k in range(4):
        runloom.fiber(prod, k * 1000)
    # need to close after producers: count with a chan
def main2b():
    donec = runloom.Chan(0)
    def prod(base):
        for i in range(500):
            ch2.send(base + i)
        donec.send(1)
    for k in range(4):
        runloom.fiber(prod, k * 1000)
    def closer():
        for _ in range(4):
            donec.recv()
        ch2.close()
    runloom.fiber(closer)
runloom.run(HUBS, main2b)
t2.join()
assert len(got2) == 2000 and len(set(got2)) == 2000, len(got2)
print("foreign-thread recv hubs=%d OK n=%d" % (HUBS, len(got2)))
