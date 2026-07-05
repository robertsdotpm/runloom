"""Verify: does cancel_all_parked() unwind a fiber blocked in cooperative
select.select([sock],[],[]) (no timeout), the way it unwinds a socket.recv
waiter?  Claim: it does NOT -- the select fiber re-parks forever."""
import errno
import select
import socket
import sys
import threading
import time

sys.path.insert(0, "/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo/src")

import runloom_c
import runloom
import runloom.monkey

runloom.monkey.patch()

result = {}
a, b = socket.socketpair()      # select target (never readable)
c, d = socket.socketpair()      # recv control (never readable)


def selecter():
    try:
        r, w, x = select.select([a], [], [])   # no timeout: parks forever
        result["select"] = ("ok", (r, w, x))
    except OSError as e:
        result["select"] = ("oserror", e.errno)
    except BaseException as e:  # noqa: BLE001
        result["select"] = ("err", type(e).__name__)


def recver():
    try:
        c.recv(64)
        result["recv"] = ("ok", None)
    except OSError as e:
        result["recv"] = ("oserror", e.errno)
    except BaseException as e:  # noqa: BLE001
        result["recv"] = ("err", type(e).__name__)


def canceller():
    runloom.sleep(0.1)          # let both park
    n = runloom_c.cancel_all_parked()
    print("cancelled %d parked" % n, flush=True)
    runloom.sleep(0.3)          # give the woken fibers time to unwind
    print("recv   state:", result.get("recv", "STILL PARKED"), flush=True)
    print("select state:", result.get("select", "STILL PARKED"), flush=True)
    # second shot, in case the select fiber needs another cancel
    n2 = runloom_c.cancel_all_parked()
    print("second cancel_all_parked -> %d" % n2, flush=True)
    runloom.sleep(0.3)
    print("select state after 2nd cancel:", result.get("select", "STILL PARKED"), flush=True)


def main():
    runloom.fiber(selecter)
    runloom.fiber(recver)
    runloom.fiber(canceller)
    runloom.sleep(1.2)


def driver():
    runloom.run(1, main)
    print("run() returned", flush=True)


t = threading.Thread(target=driver, daemon=True)
t.start()
t.join(10)
if t.is_alive():
    print("run() DID NOT RETURN within 10s (fiber stranded keeps join alive)", flush=True)
print("final recv   state:", result.get("recv", "STILL PARKED"), flush=True)
print("final select state:", result.get("select", "STILL PARKED"), flush=True)
