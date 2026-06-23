"""big_100 / 68 -- sys.exc_info preservation.

Inside nested exception handlers, goroutines yield and block.  sys.exc_info()
must keep reporting THIS goroutine's currently-handled exception across every
scheduling point -- not a sibling's exception that happened to be in flight on
the same hub.

Stresses: per-goroutine exception-state snapshot/restore.
"""
import sys

import harness
import runloom


class Outer(Exception):
    pass


class Inner(Exception):
    pass


def worker(H, wid, rng, state):
    tag = "w{0}".format(wid)
    while H.running():
        try:
            raise Outer(tag)
        except Outer:
            runloom.yield_now()
            cur = sys.exc_info()[1]
            if not H.check(isinstance(cur, Outer) and cur.args[0] == tag,
                           "exc_info wrong in outer wid={0}: {1!r}".format(
                               wid, cur)):
                return
            runloom.sleep(0.0003)
            try:
                raise Inner(tag)
            except Inner:
                runloom.yield_now()
                cur = sys.exc_info()[1]
                if not H.check(isinstance(cur, Inner) and cur.args[0] == tag,
                               "exc_info wrong in inner wid={0}: {1!r}".format(
                                   wid, cur)):
                    return
            # Back in the outer handler: exc_info must restore to Outer.
            runloom.yield_now()
            cur = sys.exc_info()[1]
            if not H.check(isinstance(cur, Outer) and cur.args[0] == tag,
                           "exc_info not restored to outer wid={0}: {1!r}".format(
                               wid, cur)):
                return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p68_exc_info", body, default_funcs=4000,
                 describe="sys.exc_info() stays correct across yields/blocks")
