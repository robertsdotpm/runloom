"""TCP open_connection / start_server tests for pygo.aio."""
import asyncio
import socket
import threading
import unittest

import pygo.aio as paio


class TestEcho(unittest.TestCase):
    def test_echo_round_trip(self):
        async def handler(reader, writer):
            data = await reader.read(4096)
            writer.write(data)
            await writer.drain()
            writer.close()

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", 0)
            sock = server.sockets[0]
            host, port = sock.getsockname()[:2]

            reader, writer = await paio.open_connection(host, port)
            writer.write(b"hello pygo")
            await writer.drain()
            data = await reader.read(4096)
            writer.close()

            server.close()
            return data

        self.assertEqual(paio.run(main()), b"hello pygo")

    def test_readline(self):
        async def handler(reader, writer):
            writer.write(b"line-one\nline-two\n")
            await writer.drain()
            writer.close()

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]
            r, w = await paio.open_connection(host, port)
            a = await r.readline()
            b = await r.readline()
            w.close()
            server.close()
            return (a, b)

        a, b = paio.run(main())
        self.assertEqual(a, b"line-one\n")
        self.assertEqual(b, b"line-two\n")

    def test_readexactly(self):
        async def handler(reader, writer):
            writer.write(b"abcdefghij")
            await writer.drain()
            writer.close()

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]
            r, w = await paio.open_connection(host, port)
            data = await r.readexactly(5)
            w.close()
            server.close()
            return data

        self.assertEqual(paio.run(main()), b"abcde")

    def test_readexactly_incomplete_raises(self):
        async def handler(reader, writer):
            writer.write(b"abc")
            await writer.drain()
            writer.close()

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]
            r, w = await paio.open_connection(host, port)
            try:
                await r.readexactly(10)
                outcome = "no-raise"
            except asyncio.IncompleteReadError as e:
                outcome = ("partial", e.partial)
            w.close()
            server.close()
            return outcome

        outcome = paio.run(main())
        self.assertEqual(outcome[0], "partial")
        self.assertEqual(outcome[1], b"abc")

    def test_many_concurrent_clients(self):
        # 500 clients hammer the server in parallel.  Each one sends its
        # number, expects it back doubled.
        async def handler(reader, writer):
            data = await reader.readline()
            n = int(data.strip())
            writer.write(("%d\n" % (n * 2)).encode())
            await writer.drain()
            writer.close()

        async def client(host, port, n):
            r, w = await paio.open_connection(host, port)
            w.write(("%d\n" % n).encode())
            await w.drain()
            data = await r.readline()
            w.close()
            return int(data.strip())

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]
            results = await asyncio.gather(*[client(host, port, i)
                                             for i in range(500)])
            server.close()
            return results

        results = paio.run(main())
        self.assertEqual(results, [i * 2 for i in range(500)])


class TestThreadedServerLoop(unittest.TestCase):
    """A server event loop running on a NON-main OS thread (run_forever) must
    wake on an incoming connection driven by a BLOCKING socket client on the
    main thread.  This is the aiosmtpd threaded-Controller pattern: the loop's
    netpoll pump runs on the controller thread, so the accept/recv wake must
    route to that thread's scheduler (Phase C per-thread sched + Phase 2
    cross-thread netpoll wake), not the main thread's."""

    def test_blocking_client_wakes_threaded_server(self):
        ready = threading.Event()
        box = {}

        def server_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def handle(reader, writer):
                data = await reader.readline()
                writer.write(b"echo:" + data)
                await writer.drain()
                writer.close()

            async def setup():
                server = await asyncio.start_server(handle, "127.0.0.1", 0)
                box["addr"] = server.sockets[0].getsockname()[:2]
                box["server"] = server
                ready.set()

            loop.run_until_complete(setup())   # listening; accept goroutine parked
            box["loop"] = loop
            loop.run_forever()                 # serve on THIS (non-main) thread

        t = threading.Thread(target=server_thread, daemon=True)
        t.start()
        self.assertTrue(ready.wait(10), "server never came up")
        host, port = box["addr"]
        try:
            # Blocking client on the MAIN thread -- must drive a cross-thread
            # netpoll wake on the server's (non-main) loop thread.
            with socket.create_connection((host, port), timeout=10) as s:
                s.sendall(b"hello\n")
                s.settimeout(10)
                resp = s.recv(100)
            self.assertEqual(resp, b"echo:hello\n")
        finally:
            loop = box.get("loop")
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
            t.join(10)
            self.assertFalse(t.is_alive(), "server loop thread did not stop")


if __name__ == "__main__":
    unittest.main()
