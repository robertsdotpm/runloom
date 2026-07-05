"""Deterministic demonstration of the WithCancel(parent) vs parent.cancel() race.

Thread A creates a child context; a per-thread trace function pauses it exactly
between the `parent._err is not None` check and the `parent._children.append(self)`
(src/runloom/context.py lines 150/154).  While paused, thread B runs the parent's
cancel() to completion.  Thread A then resumes and appends.

Expected (Go semantics / correct behavior): child ends up cancelled.
Claimed bug: child._err stays None forever -> lost cancellation.
"""
import sys
import threading

from runloom import context

in_window = threading.Event()
cancel_done = threading.Event()

parent, pcancel = context.WithCancel(context.Background())

child_holder = [None]
CTX_FILE = context.__file__

def tracer(frame, event, arg):
    if frame.f_code.co_name != "__init__" or frame.f_code.co_filename != CTX_FILE:
        return None
    def local(frame, event, arg):
        # Line 154 is `parent._children.append(self)` -- the line event fires
        # BEFORE the line executes, i.e. after the _err-is-None check passed.
        if event == "line" and frame.f_lineno == 154:
            in_window.set()
            cancel_done.wait(10)   # hold here while thread B cancels the parent
        return local
    return local

def maker():
    sys.settrace(tracer)
    try:
        c, _ = context.WithCancel(parent)
    finally:
        sys.settrace(None)
    child_holder[0] = c

def canceller():
    in_window.wait(10)
    pcancel()                      # parent fully cancelled: _err set, done closed,
                                   # children snapshotted+fanned out, list replaced
    cancel_done.set()

ta = threading.Thread(target=maker)
tb = threading.Thread(target=canceller)
ta.start(); tb.start()
ta.join(10); tb.join(10)

child = child_holder[0]
print("in_window hit:", in_window.is_set())
print("parent.err():", parent.err())
print("child.err(): ", child.err())
print("child registered in parent._children:", child in parent._children)

# Calling cancel() again is an idempotent no-op, so the child can never be
# rescued through the parent:
pcancel()
print("child.err() after second pcancel():", child.err())

if parent.err() is not None and child.err() is None:
    print("BUG CONFIRMED: parent cancelled, child NEVER cancelled (done never closes)")
    sys.exit(1)
else:
    print("no bug: child observed cancellation")
    sys.exit(0)
