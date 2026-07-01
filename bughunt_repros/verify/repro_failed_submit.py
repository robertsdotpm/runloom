import os, tempfile, runloom_c as rc
print('iouring available:', rc.iouring_available())
assert rc.iouring_available()
fd, path = tempfile.mkstemp()
os.write(fd, b'0123456789ABCDEF')
b1, b2, b3 = bytearray(4), bytearray(4), bytearray(4)
try:
    n = rc.file_read(fd, b1, 4, 0)
    print('read1 ok n=', n, 'b1=', bytes(b1))
except OSError as e:
    print('read1 OSError', e)
n2 = rc.file_read(fd, b2, 4, 8)
print('read2 n=', n2, 'b2=', bytes(b2), 'b1=', bytes(b1), '(expect b2=b\"89AB\")')
n3 = rc.file_read(fd, b3, 4, 12)
print('read3 n=', n3, 'b3=', bytes(b3), 'b2=', bytes(b2), '(expect b3=b\"CDEF\")')
os.close(fd); os.unlink(path)
