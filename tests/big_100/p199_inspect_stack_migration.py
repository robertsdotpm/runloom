"""big_100 / 199 -- inspect.stack() / frame walks across hub migration.

Goroutines call `inspect.stack()` and `sys._getframe()` walks while migrating
between hub threads (yield/sleep between calls) and while inside an exception
handler.  Under M:N a goroutine's Python frames live on its swapped C stack;
they do NOT chain all the way up to module scope (the goroutine frame-chain
limit), so a walk that expects to reach `<module>` would be wrong.  What MUST
stay true is that the walk is SELF-CONSISTENT and never crashes: each call
returns a non-empty list/frame, the current function's name is present, and a
hub migration mid-walk-sequence doesn't corrupt the frame state.

Stresses: frame introspection (inspect.stack / sys._getframe) across a
PyThreadState swap; frame-chain self-consistency under migration.
"""
import inspect
import sys

import harness
import runloom


def inner_named_frame(H, wid, depth):
    """A named function so we can assert its name shows up in the walk."""
    # sys._getframe(0) must be THIS frame.
    fr = sys._getframe(0)
    if not H.check(fr is not None and fr.f_code.co_name == "inner_named_frame",
                   "sys._getframe(0) wrong frame wid={0}: {1}".format(
                       wid, fr.f_code.co_name if fr else None)):
        return False
    # Migrate while holding live frames on the goroutine stack.
    runloom.yield_now()
    # inspect.stack() must return a non-empty list and include THIS function.
    stk = inspect.stack()
    try:
        if not H.check(len(stk) > 0,
                       "inspect.stack() empty wid={0}".format(wid)):
            return False
        names = [fi.function for fi in stk]
        if not H.check("inner_named_frame" in names,
                       "current function missing from stack wid={0}: {1}".format(
                           wid, names[:6])):
            return False
    finally:
        # Break the reference cycle inspect.stack() creates (frames in a list
        # that the list's frame can reach) promptly -- don't lean on the GC.
        del stk
    return True


def walk_via_back(H, wid):
    """Walk f_back from the current frame; it must terminate cleanly (None) and
    never loop or crash, even though it does NOT reach module scope under M:N."""
    fr = sys._getframe(0)
    seen = 0
    while fr is not None and seen < 256:
        # Touching attributes must not crash on any frame in the chain.
        _ = fr.f_code.co_name
        _ = fr.f_lineno
        fr = fr.f_back
        seen += 1
    # seen>0 (at least our own frame); termination within the bound proves no
    # cyclic/corrupt f_back chain.
    return H.check(seen > 0 and seen < 256,
                   "f_back walk did not terminate cleanly wid={0}: seen={1}"
                   .format(wid, seen))


def worker(H, wid, rng, state):
    # inspect.stack() is expensive; gate its frequency so the run stays fast.
    for _ in H.round_range():
        # 1) frame walk inside a named function, across a migration.
        if not inner_named_frame(H, wid, 0):
            return

        # 2) frame introspection from inside an exception handler, across a
        #    migration -- the handler's frame + the raising frame must be
        #    consistent after the hub swap.
        try:
            raise ValueError(wid)
        except ValueError:
            runloom.sleep(0.0003)            # likely resume on another hub
            cur = sys.exc_info()[1]
            if not H.check(isinstance(cur, ValueError) and cur.args[0] == wid,
                           "exc state lost across migration wid={0}".format(wid)):
                return
            fr = sys._getframe(0)
            if not H.check(fr.f_code.co_name == "worker",
                           "wrong frame in handler wid={0}".format(wid)):
                return

        # 3) f_back walk: terminates, never loops.
        if not walk_via_back(H, wid):
            return

        H.op(wid)
        H.task_done(wid)
        if rng.random() < 0.3:
            runloom.yield_now()


def setup(H):
    H.state = {}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.total_ops() > 0, "no frame walks completed")
    H.log("frame_walks={0}".format(H.total_ops()))


if __name__ == "__main__":
    harness.main("p199_inspect_stack_migration", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="inspect.stack()/sys._getframe walks stay consistent "
                          "across hub migration + exception handling, no crash")
