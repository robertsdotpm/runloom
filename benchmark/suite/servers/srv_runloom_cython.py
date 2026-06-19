"""Server tiers 4 & 5: runloom_c.serve + the zero-PyObject CYTHON handler, on the
io_uring loop backend (orchestrator sets RUNLOOM_IOURING_LOOP=1).

  tier 4: no optimize()
  tier 5: runloom.optimize("throughput") first  (--optimize throughput)

The handler (handler_cy.handler) calls runloom's cooperative recv/send as plain
C functions via the runloom_c.__tcp_capi__ capsule, so the per-request hot loop
allocates no Python objects (proven by disasm_check.sh).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # find handler_cy*.so

import runloom
import runloom_c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--hubs", type=int, default=int((os.cpu_count() or 1) * 0.7))
    ap.add_argument("--optimize", default="none", choices=["none", "throughput"])
    ap.add_argument("--token", default="")
    args = ap.parse_args()

    # optimize() must run BEFORE runloom.run() (decision: tier 5 = tier 4 + this).
    if args.optimize == "throughput":
        eff = runloom.optimize("throughput")
        print("OPTIMIZE %s" % eff, flush=True)

    import handler_cy  # imported after optimize so the capsule/runtime are set

    def root():
        port, listeners = runloom_c.serve(
            args.host, args.port, handler_cy.handler,
            acceptors=args.hubs, backlog=4096)
        print("LISTENING %d" % port, flush=True)
        runloom.sleep(float("inf"))

    runloom.run(args.hubs, main_fn=root)


if __name__ == "__main__":
    main()
