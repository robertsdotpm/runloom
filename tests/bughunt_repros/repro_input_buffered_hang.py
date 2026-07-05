"""_patched_input / _patched_stdin_readline hang on buffered data.

The stdio patch parks on wait_fd(stdin_fd, READ) BEFORE calling the original
input()/readline().  But sys.stdin is a TextIOWrapper over a BufferedReader:
the FIRST read slurps everything available in the kernel pipe into the Python
buffer.  The SECOND input() then parks on wait_fd for an fd whose kernel buffer
is EMPTY -- while the next line sits, already readable, in the io buffer.  The
fiber parks forever (as long as the writer keeps the pipe open).

Unpatched Python: both input() calls return immediately.
Observed (bug): second input() hangs -> watchdog exit 2.
"""
import os
import sys
import threading as _th_mod

# Rig stdin to a pipe BEFORE patching, write two lines, keep write end open.
r, w = os.pipe()
os.write(w, b"line-one\nline-two\n")
os.dup2(r, 0)
sys.stdin = os.fdopen(0, "r")     # fresh TextIOWrapper over fd 0

import runloom.monkey as monkey
monkey.patch()

import time
import runloom_c as rc

results = []


def reader():
    a = input()
    results.append(a)
    b = input()                    # kernel pipe empty, data in io buffer -> parks
    results.append(b)


def watchdog():
    time.sleep(5)
    if len(results) < 2:
        print("DEADLOCK: got %r, second input() parked on wait_fd while "
              "'line-two' sits in the io buffer" % (results,))
        sys.stdout.flush()
        os._exit(2)


_wd = _th_mod.Thread(target=watchdog, daemon=True)
_wd.start()

rc.fiber(reader)
rc.run()
print("DONE:", results)
