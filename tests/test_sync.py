"""Tests for runloom.sync -- the no-async-await facade.

Demonstrates: TCP echo server + N concurrent clients, UDP send/recv,
Lock/Event coordination, all written WITHOUT a single async def or
await keyword.  Same scheduler as runloom.aio; same throughput.
"""
import socket
import unittest

import runloom.sync as ps


class TestBasics(unittest.TestCase):
    def test_fiber_run(self):
        out = []
        def w():
            out.append("ran")
        ps.fiber(w)
        ps.run()
        self.assertEqual(out, ["ran"])

    def test_sleep_yields(self):
        order = []
        def a():
            order.append("a-1")
            ps.sleep(0.01)
            order.append("a-2")
        def b():
            order.append("b-1")
            ps.yield_now()
            order.append("b-2")
        ps.fiber(a); ps.fiber(b); ps.run()
        # a starts, parks on sleep, b runs (a-1, b-1, b-2), then a-2.
        self.assertIn("a-2", order)
        self.assertIn("b-2", order)


class TestTCP(unittest.TestCase):
    def test_echo(self):
        result_holder = []
        listen_holder = []  # keep the listen socket alive + accessible

        def server(listen_sock):
            conn, _addr = listen_sock.accept()
            data = conn.recv(1024)
            conn.sendall(data)
            conn.close()
            listen_sock.close()

        def client(host, port):
            s = ps.tcp_connect(host, port)
            s.sendall(b"hello-sync")
            data = s.recv(1024)
            s.close()
            result_holder.append(data)

        def main():
            listen = ps.tcp_listen("127.0.0.1", 0)
            listen_holder.append(listen)
            host, port = listen.getsockname()[:2]
            ps.fiber(server, listen)
            ps.fiber(client, host, port)

        ps.run(main)
        self.assertEqual(result_holder, [b"hello-sync"])

    def test_many_clients(self):
        results = []

        def server(listen_sock, expected):
            try:
                for _ in range(expected):
                    conn, _ = listen_sock.accept()
                    ps.fiber(handle_one, conn)
            finally:
                listen_sock.close()

        def handle_one(conn):
            data = conn.recv(1024)
            conn.sendall(data[::-1])
            conn.close()

        def client(host, port, payload):
            s = ps.tcp_connect(host, port)
            s.sendall(payload)
            data = s.recv(1024)
            s.close()
            results.append(data)

        def main():
            listen = ps.tcp_listen("127.0.0.1", 0)
            host, port = listen.getsockname()[:2]
            N = 50
            ps.fiber(server, listen, N)
            for i in range(N):
                ps.fiber(client, host, port, ("msg-%d" % i).encode())

        ps.run(main)
        self.assertEqual(sorted(results),
                         sorted(("msg-%d" % i).encode()[::-1]
                                for i in range(50)))


class TestUDP(unittest.TestCase):
    def test_send_recv(self):
        result_holder = []

        def receiver(sock):
            try:
                data, _addr = sock.recvfrom(1024)
                result_holder.append(data)
            finally:
                sock.close()

        def sender(host, port):
            s = ps.udp_endpoint(remote_addr=(host, port))
            s.send(b"ping")
            s.close()

        def main():
            recv = ps.udp_endpoint(local_addr=("127.0.0.1", 0))
            host, port = recv.getsockname()[:2]
            ps.fiber(receiver, recv)
            ps.fiber(sender, host, port)

        ps.run(main)
        self.assertEqual(result_holder, [b"ping"])


class TestPrimitives(unittest.TestCase):
    def test_lock_excludes(self):
        counter = [0]

        def worker(lk):
            for _ in range(50):
                with lk:
                    v = counter[0]
                    ps.yield_now()
                    counter[0] = v + 1

        def main():
            lk = ps.Lock()
            for _ in range(4):
                ps.fiber(worker, lk)

        ps.run(main)
        self.assertEqual(counter[0], 200)

    def test_event_wakes(self):
        out = []

        def waiter(ev):
            ev.wait()
            out.append("woken")

        def setter(ev):
            ps.sleep(0.005)
            ev.set()

        def main():
            ev = ps.Event()
            ps.fiber(waiter, ev)
            ps.fiber(setter, ev)

        ps.run(main)
        self.assertEqual(out, ["woken"])


if __name__ == "__main__":
    unittest.main()
