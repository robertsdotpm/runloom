import socket, sys, threading, time
import runloom

def scenario(tag):
    a, b = socket.socketpair()
    def feeder():
        a.sendall(b"AAAA"); time.sleep(0.3); a.sendall(b"BBBB")
    th = threading.Thread(target=feeder); th.start()
    time.sleep(0.1)
    data = b.recv(8, socket.MSG_WAITALL)
    print(tag, len(data), data)
    th.join()

if sys.argv[1] == "stock": scenario("stock:")
else:
    def main(): runloom.fiber(lambda: scenario("patched:"))
    runloom.monkey.patch(); runloom.run(2, main)
