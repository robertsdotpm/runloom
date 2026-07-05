import runloom, socket
def main():
    runloom.monkey.patch()
    def fib():
        a, b = socket.socketpair()
        a.sendall(b"hi")
        print("got:", b.recv(10))
        a.close(); b.close()
    runloom.fiber(fib)
runloom.run(2, main)
print("OK")
