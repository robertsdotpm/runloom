"""TCP echo client — sends N parallel goroutines, each doing M round-trips."""
import socket
import sys
import time

sys.path.insert(0, "src")
import runloom
import runloom.monkey
import runloom_c


HOST = "127.0.0.1"
PORT = 9000


def client(client_id, n_msgs, payload):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    try:
        for _ in range(n_msgs):
            s.sendall(payload)
            got = b""
            while len(got) < len(payload):
                chunk = s.recv(len(payload) - len(got))
                if not chunk:
                    return
                got += chunk
    finally:
        s.close()


def main():
    runloom.monkey.patch()
    n_clients = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    n_msgs    = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    size      = int(sys.argv[3]) if len(sys.argv) > 3 else 64
    payload = b"x" * size
    print("clients={0} msgs/client={1} size={2}B".format(
        n_clients, n_msgs, size))

    for i in range(n_clients):
        runloom_c.go(lambda i=i: client(i, n_msgs, payload))
    t0 = time.perf_counter()
    runloom_c.run()
    t = time.perf_counter() - t0
    total = n_clients * n_msgs
    print("{0} round-trips in {1:.2f}s -- {2:.0f} req/s, {3:.1f} us/RT".format(
        total, t, total / t, t / total * 1e6))


if __name__ == "__main__":
    main()
