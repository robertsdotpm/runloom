"""select.select (cooperative epoll path) also ignores the CANCELLED sentinel:
a fiber parked in select.select survives cancel_all_parked -> teardown hang."""
import socket, select, sys, time
import runloom, runloom_c

def main():
    a, b = socket.socketpair()
    state = {}
    def selector():
        try:
            r = select.select([b], [], [])   # no timeout
            state["out"] = ("ready", r)
        except Exception as e:
            state["out"] = ("exc", type(e).__name__, str(e))
    runloom.fiber(selector)
    runloom.sleep(0.3)
    n = runloom_c.cancel_all_parked()
    print("cancelled %d parked" % n, flush=True)
    runloom.sleep(0.5)
    print("selector state:", state.get("out", "STILL PARKED"), flush=True)

runloom.monkey.patch()
runloom.run(2, main)
print("run() returned", flush=True)
