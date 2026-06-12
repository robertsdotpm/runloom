# cython: language_level=3
# cython: freethreading_compatible=True
# cython: annotation_typing=True
cimport cython

SPIN = 1500

def echo_cpu(conn, stop):
    buf = bytearray(64)
    try:
        while not stop[0]:
            n = conn.recv_into(buf, 8)
            if not n:
                break
            acc = 0
            for k in range(SPIN):
                for j in range(n):
                    acc = (acc + buf[j] * k) & 0xffffffff
            buf[0] = acc & 0xff
            conn.send_all(memoryview(buf)[:n])
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass
