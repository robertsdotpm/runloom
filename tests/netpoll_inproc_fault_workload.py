"""Windows netpoll fault-injection workload (driven by
test_win_netpoll_faultinject.py).

Parks a fiber on a never-readable socket with a deadline.  The pump polls
that socket every iteration, so an injected poll/submit fault (RUNLOOM_FAULT_<SITE>,
see netpoll.c) lands on a LIVE code path.  The fiber wakes via its deadline
regardless of the fault, so the workload always terminates -- the point is the
runtime's RESPONSE to the fault (retry / 1 ms backoff / clean error), measured
by how many times the fault fired (_fault_count).

Prints sentinels for the harness:
  BACKEND=<name>   the netpoll backend actually selected
  RESULT=<repr>    what the parked fiber's wait_fd returned/raised
  FAULTS=<n>       times the injection site fired during the run
  DONE
"""
import os
import socket
import sys

sys.path.insert(0, "src")
# A harness may point at an alternate build (e.g. the select-forced variant
# the POSIX select fault test builds into a temp dir) -- prepend it so its
# runloom_c wins over the default in-tree one.
_core = os.environ.get("RUNLOOM_CORE_PATH")
if _core:
    sys.path.insert(0, _core)

import runloom_c

SITE = os.environ.get("FAULT_SITE", "WSAPOLL")
TIMEOUT_MS = int(os.environ.get("FAULT_TIMEOUT_MS", "800"))


def main():
    # A UDP socket bound to an ephemeral port is never readable (nothing is
    # sent to it), so the parked fiber can only wake via its deadline --
    # which it must do even while the poll syscall is being faulted.
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    s.setblocking(False)
    result = []

    def parker():
        try:
            r = runloom_c.wait_fd(s.fileno(), 1, TIMEOUT_MS)   # READ + deadline
            result.append(("ok", r))
        except OSError as e:
            result.append(("oserror", e.errno))
        except BaseException as e:                              # noqa: BLE001
            result.append(("err", type(e).__name__))

    runloom_c.go(parker)
    runloom_c.run()
    s.close()

    print("BACKEND=%s" % runloom_c.netpoll_backend())
    print("RESULT=%r" % (result,))
    print("FAULTS=%d" % runloom_c._fault_count(SITE))
    print("DONE")


if __name__ == "__main__":
    main()
