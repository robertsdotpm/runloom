import os, tempfile, runloom_c as rc
print("iouring_available:", rc.iouring_available())
fd,_ = tempfile.mkstemp(); os.write(fd,b'hello world!'); os.lseek(fd,0,0)
buf=bytearray(6)
n1=rc.file_read(fd,buf,6); a=bytes(buf[:n1])
n2=rc.file_read(fd,buf,6); b=bytes(buf[:n2])
print('read1',a,'read2',b)   # expect hello / world! -> get hello / hello
fd2,_=tempfile.mkstemp(); rc.file_write(fd2,b'AAAA'); rc.file_write(fd2,b'BBBB')
os.lseek(fd2,0,0); print('content',os.read(fd2,64))  # expect AAAABBBB -> get BBBB
