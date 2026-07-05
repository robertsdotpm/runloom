"""Sanity: single-fiber TCPConn recv works in iouring multishot mode."""
import os
os.environ["RUNLOOM_TCPCONN_IOURING"] = "1"
import socket
import runloom_c

runloom_c.mn_init(4)

out = []

def main():
    lst = socket.socket()
    lst.bind(("127.0.0.1", 0)); lst.listen(1)
    cli = socket.socket(); cli.connect(lst.getsockname())
    srv, _ = lst.accept(); lst.close()
    fd = os.dup(srv.fileno()); srv.close()
    conn = runloom_c.TCPConn(fd)
    cli.sendall(b"hello")
    data = conn.recv(5)
    out.append(data)
    cli.close()
    tail = conn.recv(5)
    out.append(tail)
    conn.close()
    print("got:", out, flush=True)
    os._exit(0)

runloom_c.mn_fiber(main)
runloom_c.mn_run()
