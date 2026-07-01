"""_CancelCtx: a cancelled/completed child is never removed from its parent's
_children list.  Go's WithCancel removes the child from the parent on cancel;
here a per-request WithTimeout/WithCancel under a long-lived parent context
accumulates every dead child forever (each holding a Chan) -> unbounded leak."""
import runloom
import runloom.context as ctx

def main():
    parent, parent_cancel = ctx.WithCancel(ctx.Background())
    for i in range(10000):
        child, cancel = ctx.WithCancel(parent)   # per-request context
        cancel()                                  # request finished
    print("len(parent._children) =", len(parent._children),
          "(expected ~0; every child was cancelled)")

runloom.run(1, main)
