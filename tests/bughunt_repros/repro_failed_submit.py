"""Repro: a failed io_uring_enter in runloom_iouring_submit_sqe leaves the
already-queued SQE in the SQ ring (sq_tail was bumped before the enter).
The next submit's io_uring_enter(to_submit=1) then submits the STALE SQE
(dangling user_data -> the previous call's dead stack op record) and leaves
the new SQE unsubmitted -> every subsequent read returns the PREVIOUS
read's completion (data lands in the previous call's buffer).

Run under: strace -e inject=io_uring_enter:error=EBUSY:when=<N> (see .sh)
"""
import os, sys, tempfile
import runloom_c as rc

assert rc.iouring_available()

fd, path = tempfile.mkstemp()
os.write(fd, b"0123456789ABCDEF")

b1, b2, b3 = bytearray(4), bytearray(4), bytearray(4)

try:
    n = rc.file_read(fd, b1, 4, 0)
    print("read1: n=%d b1=%s" % (n, bytes(b1)), flush=True)
except OSError as e:
    print("read1: OSError", e, flush=True)

try:
    n = rc.file_read(fd, b2, 4, 8)         # expect b2 == b"89AB"
    print("read2: n=%d b2=%s b1=%s" % (n, bytes(b2), bytes(b1)), flush=True)
except OSError as e:
    print("read2: OSError", e, flush=True)

try:
    n = rc.file_read(fd, b3, 4, 12)        # expect b3 == b"CDEF"
    print("read3: n=%d b3=%s b2=%s" % (n, bytes(b3), bytes(b2)), flush=True)
except OSError as e:
    print("read3: OSError", e, flush=True)

os.close(fd); os.unlink(path)

if bytes(b2) == b"89AB" and bytes(b3) == b"CDEF":
    print("OK: reads landed in their own buffers")
    sys.exit(0)
else:
    print("BUG: completions shifted by one op (stale SQE submitted after failed enter)")
    sys.exit(1)
