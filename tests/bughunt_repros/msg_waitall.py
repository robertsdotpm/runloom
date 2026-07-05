"""recv(n, MSG_WAITALL) on a blocking socket must return exactly n bytes
(waiting for stragglers).  Patched runloom forces O_NONBLOCK, under which the
kernel ignores MSG_WAITALL -> short read -> framed protocols desync."""
import socket, sys, threading, time

def scenario(tag):
    a, b = socket.socketpair()
    def feeder():
        a.sendall(b"AAAA")
        time.sleep(0.3)
        a.sendall(b"BBBB")
    th = threading.Thread(target=feeder); th.start()
    time.sleep(0.1)   # let first 4 bytes land
    data = b.recv(8, socket.MSG_WAITALL)
    print(tag, "recv(8, MSG_WAITALL) ->", len(data), "bytes:", data)
    th.join(); a.close(); b.close()

if sys.argv[1] == "stock":
    scenario("stock:")
else:
    import runloom
    def main():
        runloom.fiber(lambda: scenario("patched-fiber:"))
    runloom.monkey.patch()
    runloom.run(2, main)
