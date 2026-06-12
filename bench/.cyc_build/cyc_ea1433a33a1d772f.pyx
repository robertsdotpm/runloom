# cython: language_level=3
# cython: freethreading_compatible=True
# cython: annotation_typing=True
cimport cython

def echo_io(conn, stop):
    buf = bytearray(64)
    try:
        while not stop[0]:
            n = conn.recv_into(buf, 8)
            if not n:
                break
            conn.send_all(memoryview(buf)[:n])
    except OSError:
        pass
    finally:
        try:
            conn.close()
        except OSError:
            pass
