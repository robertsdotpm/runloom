import select, sys, os, time
import runloom

def patched():
    def main():
        r, w = os.pipe()
        def poller():
            p = select.poll(); p.register(r, select.POLLIN)
            print("poll done:", p.poll(None), flush=True)
        def writer():
            runloom.sleep(0.2)
            runloom.monkey.offload(time.sleep, 0.05)
            os.write(w, b"x"); print("writer done", flush=True)
        runloom.fiber(poller); runloom.fiber(writer)
    runloom.monkey.patch()
    runloom.run(1, main)   # one hub -> one shard for all offloads
    print("ALL DONE", flush=True)

if sys.argv[1] == "patched": patched()
else:
    import threading
    r, w = os.pipe()
    def poller():
        p = select.poll(); p.register(r, select.POLLIN); print("poll done:", p.poll(None))
    def writer():
        time.sleep(0.1); time.sleep(0.05); os.write(w, b"x"); print("writer done")
    t1 = threading.Thread(target=poller); t2 = threading.Thread(target=writer)
    t1.start(); t2.start(); t1.join(8); t2.join(8); print("ALL DONE")
