"""Repro: runloom_c.file_read / file_write with default offset=-1
("use current fd offset" per the C comment) actually read/write at
OFFSET 0 on the io_uring path, diverging from the non-io_uring fallback
(plain read()/write(), which honours + advances the fd position).
"""
import os, sys, tempfile
import runloom_c as rc

print("iouring_available:", rc.iouring_available())

# --- read side ---
fd, path = tempfile.mkstemp()
os.write(fd, b"hello world!")
os.lseek(fd, 0, os.SEEK_SET)

buf = bytearray(6)
n1 = rc.file_read(fd, buf, 6)           # default offset -> "current fd offset"
first = bytes(buf[:n1])
n2 = rc.file_read(fd, buf, 6)           # should continue where the first left off
second = bytes(buf[:n2])
print("read1:", first, "read2:", second)

# --- write side ---
fd2, path2 = tempfile.mkstemp()
rc.file_write(fd2, b"AAAA")             # default offset
rc.file_write(fd2, b"BBBB")             # should append after the first write
os.lseek(fd2, 0, os.SEEK_SET)
content = os.read(fd2, 64)
print("file content after two default-offset writes:", content)

ok = (second != first) and (content == b"AAAABBBB")
print("SEMANTICS OK" if ok else "SEMANTIC DIVERGENCE: default offset ignored (always 0)")
os.close(fd); os.unlink(path)
os.close(fd2); os.unlink(path2)
sys.exit(0 if ok else 1)
