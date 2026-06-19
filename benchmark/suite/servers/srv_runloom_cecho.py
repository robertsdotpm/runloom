"""All-C echo tier: runloom_c.serve(handler=None) -> the built-in tstate-free C
echo (runloom_io_c_echo), an 8-byte ping-pong. NOT a general handler.

Exists only to test the io_uring-loop "+20% over epoll" claim under its ORIGINAL
conditions: 8-byte payload, all-C fiber, the Stage-2 single-shot proactor
(loop_recv). Run it twice -- with and without RUNLOOM_IOURING_LOOP=1, at
--payload 8 on the loadgen -- to see if the +20% reproduces, and whether it
survives at 1 KiB. (It will NOT echo >8 bytes per op efficiently; use payload=8.)
"""
import argparse
import os

import runloom
import runloom_c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--hubs", type=int, default=int((os.cpu_count() or 1) * 0.7))
    ap.add_argument("--token", default="")
    args = ap.parse_args()

    def root():
        port, listeners = runloom_c.serve(
            args.host, args.port, None,        # handler=None -> all-C 8-byte echo
            acceptors=args.hubs, backlog=4096)
        print("LISTENING %d" % port, flush=True)
        runloom.sleep(float("inf"))

    runloom.run(args.hubs, main_fn=root)


if __name__ == "__main__":
    main()
