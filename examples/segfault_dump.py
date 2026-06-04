"""Catching a segfault -- classified crash dumps for a goroutine stack overflow.

A goroutine runs on a small, fixed C stack.  Deep C-level recursion -- a big
``json.dumps``, an OpenSSL handshake, a recursive protocol callback -- can run
off the end of it and segfault.  Normally that's an opaque ``Segmentation
fault`` with no clue which goroutine or why.

``runloom.inspect.install_crash_handler()`` (or the ``RUNLOOM_CRASH=on`` env var)
installs a fatal-signal handler that turns it into a *classified* dump: it names
the overflowing goroutine and its stack size and tells you what to do about it.
The fault is unrecoverable -- a SIGSEGV can't be turned into a catchable Python
exception, so the process still dies -- but now it dies *informatively*, instead
of leaving you staring at a bare "Segmentation fault".

In your own program you just call ``install_crash_handler()`` once at startup.
Here we run the doomed goroutine in a CHILD process so we can show you the dump
it produces and then exit cleanly.

Run:
    python3 examples/segfault_dump.py
"""
import subprocess
import sys
import textwrap

# What the child does: install the handler, then overflow a deliberately tiny
# goroutine stack with deep C-level json recursion.
CHILD = textwrap.dedent("""
    import json
    import runloom
    import runloom_c

    runloom.inspect.install_crash_handler()      # classify fatal signals

    # A 400-deep nested list: the C json encoder recurses once per level, which
    # is far more C stack than the 16 KiB goroutine below can hold.
    nested = []
    cur = nested
    for _ in range(400):
        nxt = []
        cur.append(nxt)
        cur = nxt

    def encode_on_a_tiny_stack():
        json.dumps(nested)                       # runs off the end of the stack

    # 16 KiB is deliberately too small.  A real bug is usually a default-stack
    # goroutine that just happens to recurse deeper than expected.
    runloom_c.go(encode_on_a_tiny_stack, 16 * 1024)
    runloom_c.run()
""")


def main():
    print("Spawning a goroutine that overflows a 16 KiB C stack on purpose...\n")
    proc = subprocess.run([sys.executable, "-c", CHILD],
                          capture_output=True, text=True)

    # The classified crash dump the handler wrote to stderr (look for the
    # ">>> GOROUTINE STACK OVERFLOW <<<" line naming the goroutine + its size).
    sys.stdout.write(proc.stderr)

    sig = -proc.returncode if proc.returncode < 0 else None
    if sig is not None:
        print("\n[child died from signal {0} (SIGSEGV) -- but now you know "
              "exactly which goroutine and why]".format(sig))
    else:
        # Some platforms / sanitizer builds report it differently; the dump
        # above is the point.
        print("\n[child exit code: {0}]".format(proc.returncode))


if __name__ == "__main__":
    main()
