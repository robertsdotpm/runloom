"""wait_fd on a pipe fd that gets closed while parked: hang or wake?"""
import os, sys, faulthandler
import runloom, runloom_c

faulthandler.dump_traceback_later(10, exit=True)
HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 1

def main():
    r, w = os.pipe()
    os.set_blocking(r, False)
    res = []
    def waiter():
        rc = runloom.wait_fd(r, 0)   # 0 = read? try; parks until readable
        res.append(("woke", rc))
        print("waiter woke:", rc)
    def closer():
        runloom.sleep(0.2)
        os.close(r)   # close the fd out from under the parked waiter
        os.close(w)
        print("closed fds")
    runloom.fiber(waiter)
    runloom.fiber(closer)

runloom.run(HUBS, main)
print("run returned")
