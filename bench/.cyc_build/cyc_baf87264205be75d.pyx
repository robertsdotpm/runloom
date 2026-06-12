# cython: language_level=3
# cython: freethreading_compatible=True
# cython: annotation_typing=True
cimport cython

SPIN = 1500

def echo_cpu_native(conn, stop):
    # Fully native compute: typed scalars AND a typed unsigned-char view over
    # the buffer, so the inner loop has ZERO PyObjects -- `cbuf[j]` is a raw
    # byte load, the arithmetic is machine ops. The only PyObjects left are the
    # recv_into/send_all method calls (the scheduler boundary), which is
    # unavoidable from Python source and is not the bottleneck.
    buf = bytearray(64)
    cbuf: cython.uchar[:]
    acc: cython.ulong
    mask: cython.ulong = 0xffffffff       # typed C constant -- NOT a Python int
    k: cython.int
    j: cython.int
    n: cython.int
    try:
        while not stop[0]:
            n = conn.recv_into(buf, 8)
            if not n:
                break
            cbuf = buf                      # acquire a typed view over the bytes
            acc = 0
            for k in range(SPIN):
                for j in range(n):
                    acc = (acc + cbuf[j] * k) & mask
            buf[0] = acc & 0xff
            conn.send_all(memoryview(buf)[:n])
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass
