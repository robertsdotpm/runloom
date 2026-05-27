"""Compatibility tests for the asyncio surface aionetiface (and most
other async network libraries) actually use.  Verifies every API
identified by `grep -rhoE 'asyncio\\.[a-zA-Z_]+' src/` of aionetiface.

If a test fails it surfaces the exact API gap before it bites a real
port.  See the survey comment at the top of each section."""
import asyncio
import socket
import unittest

import pygo.aio as paio


# ====================================================================
# Section 1: top-level functions
# ====================================================================
class TestRun(unittest.TestCase):
    def test_run_basic(self):
        async def main():
            return 42
        self.assertEqual(paio.run(main()), 42)

    def test_install_then_asyncio_run(self):
        paio.install()
        async def main():
            return "via install"
        self.assertEqual(asyncio.run(main()), "via install")


class TestSleep(unittest.TestCase):
    def test_zero(self):
        async def main():
            await asyncio.sleep(0)
            return "ok"
        self.assertEqual(paio.run(main()), "ok")

    def test_returns_value(self):
        async def main():
            return await asyncio.sleep(0, result="X")
        self.assertEqual(paio.run(main()), "X")


# ====================================================================
# Section 2: task / future / gather / wait_for / shield
# ====================================================================
class TestTaskFuture(unittest.TestCase):
    def test_create_task(self):
        async def child():
            await asyncio.sleep(0.005)
            return 7
        async def main():
            t = asyncio.create_task(child())
            return await t
        self.assertEqual(paio.run(main()), 7)

    def test_ensure_future_with_coro(self):
        async def child():
            return "ok"
        async def main():
            t = asyncio.ensure_future(child())
            return await t
        self.assertEqual(paio.run(main()), "ok")

    def test_ensure_future_passthrough_future(self):
        async def main():
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            fut.set_result(99)
            t = asyncio.ensure_future(fut)
            return await t
        self.assertEqual(paio.run(main()), 99)


class TestGather(unittest.TestCase):
    def test_basic(self):
        async def w(i): return i * 10
        async def main():
            return await asyncio.gather(w(1), w(2), w(3))
        self.assertEqual(paio.run(main()), [10, 20, 30])

    def test_return_exceptions(self):
        async def good(): return "ok"
        async def bad(): raise RuntimeError("boom")
        async def main():
            return await asyncio.gather(good(), bad(), return_exceptions=True)
        r = paio.run(main())
        self.assertEqual(r[0], "ok")
        self.assertIsInstance(r[1], RuntimeError)


class TestWaitFor(unittest.TestCase):
    def test_completes(self):
        async def fast():
            await asyncio.sleep(0.005)
            return "done"
        async def main():
            return await asyncio.wait_for(fast(), timeout=1.0)
        self.assertEqual(paio.run(main()), "done")

    def test_timeout(self):
        async def slow():
            await asyncio.sleep(1.0)
        async def main():
            try:
                await asyncio.wait_for(slow(), timeout=0.01)
                return "no-raise"
            except asyncio.TimeoutError:
                return "timeout"
        self.assertEqual(paio.run(main()), "timeout")


class TestShield(unittest.TestCase):
    def test_protects(self):
        async def inner():
            await asyncio.sleep(0.01)
            return "kept"
        async def main():
            t = asyncio.create_task(inner())
            return await asyncio.shield(t)
        self.assertEqual(paio.run(main()), "kept")


# ====================================================================
# Section 3: synchronization primitives
# ====================================================================
class TestPrimitives(unittest.TestCase):
    def test_event(self):
        async def setter(ev):
            await asyncio.sleep(0.005)
            ev.set()
        async def main():
            ev = asyncio.Event()
            asyncio.create_task(setter(ev))
            await ev.wait()
            return "woken"
        self.assertEqual(paio.run(main()), "woken")

    def test_lock(self):
        async def main():
            lk = asyncio.Lock()
            counter = [0]
            async def w():
                async with lk:
                    v = counter[0]
                    await asyncio.sleep(0.001)
                    counter[0] = v + 1
            await asyncio.gather(*[w() for _ in range(10)])
            return counter[0]
        self.assertEqual(paio.run(main()), 10)

    def test_queue(self):
        async def main():
            q = asyncio.Queue(maxsize=2)
            out = []
            async def producer():
                for i in range(5):
                    await q.put(i)
                await q.put(None)
            async def consumer():
                while True:
                    x = await q.get()
                    if x is None: return
                    out.append(x)
            await asyncio.gather(producer(), consumer())
            return out
        self.assertEqual(paio.run(main()), [0, 1, 2, 3, 4])

    def test_semaphore(self):
        async def main():
            sem = asyncio.Semaphore(2)
            active = [0]
            peak = [0]
            async def w():
                async with sem:
                    active[0] += 1
                    if active[0] > peak[0]:
                        peak[0] = active[0]
                    await asyncio.sleep(0.005)
                    active[0] -= 1
            await asyncio.gather(*[w() for _ in range(10)])
            return peak[0]
        self.assertLessEqual(paio.run(main()), 2)


# ====================================================================
# Section 4: cancellation + introspection
# ====================================================================
class TestCancellation(unittest.TestCase):
    def test_cancel(self):
        async def slow():
            try:
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                return "cancelled"
        async def main():
            t = asyncio.create_task(slow())
            await asyncio.sleep(0.005)
            t.cancel()
            return await t
        self.assertEqual(paio.run(main()), "cancelled")


class TestIntrospection(unittest.TestCase):
    def test_current_task(self):
        async def main():
            t = asyncio.current_task()
            return t is not None and t.get_name()
        result = paio.run(main())
        self.assertTrue(result)

    def test_iscoroutine(self):
        async def f(): return None
        self.assertTrue(asyncio.iscoroutine(f()))
        self.assertFalse(asyncio.iscoroutine(42))

    def test_iscoroutinefunction(self):
        async def f(): return None
        def g(): return None
        self.assertTrue(asyncio.iscoroutinefunction(f))
        self.assertFalse(asyncio.iscoroutinefunction(g))


# ====================================================================
# Section 5: low-level socket ops via loop
# ====================================================================
class TestLoopSock(unittest.TestCase):
    def test_sock_connect_recv_sendall(self):
        async def server_handler(reader, writer):
            data = await reader.read(64)
            writer.write(data[::-1])
            await writer.drain()
            writer.close()

        async def main():
            server = await paio.start_server(server_handler, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]
            loop = asyncio.get_running_loop()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            await loop.sock_connect(s, (host, port))
            await loop.sock_sendall(s, b"hello")
            data = await loop.sock_recv(s, 64)
            s.close()
            server.close()
            return data

        self.assertEqual(paio.run(main()), b"olleh")


# ====================================================================
# Section 6: UDP (DatagramProtocol + create_datagram_endpoint)
# ====================================================================
class TestUDP(unittest.TestCase):
    def test_send_receive(self):
        received = []

        class Proto(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                received.append(data)

        async def main():
            loop = asyncio.get_running_loop()
            t1, _ = await loop.create_datagram_endpoint(
                Proto, local_addr=("127.0.0.1", 0))
            addr = t1.get_extra_info("sockname")
            t2, _ = await loop.create_datagram_endpoint(
                Proto, remote_addr=addr)
            t2.sendto(b"ping")
            await asyncio.sleep(0.05)
            t1.close()
            t2.close()
            return received

        out = paio.run(main())
        self.assertEqual(out, [b"ping"])


# ====================================================================
# Section 7: getaddrinfo + run_in_executor
# ====================================================================
class TestLoopExtras(unittest.TestCase):
    def test_getaddrinfo(self):
        async def main():
            loop = asyncio.get_running_loop()
            return await loop.getaddrinfo("127.0.0.1", 0, type=socket.SOCK_STREAM)
        infos = paio.run(main())
        self.assertTrue(len(infos) >= 1)

    def test_run_in_executor(self):
        def blocking(x):
            return x * 2
        async def main():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, blocking, 21)
        self.assertEqual(paio.run(main()), 42)


# ====================================================================
# Section 8: streams (open_connection / start_server / StreamReader /
# StreamWriter); duplicates of test_aio_net.py kept here for completeness.
# ====================================================================
class TestStreams(unittest.TestCase):
    def test_open_connection_round_trip(self):
        async def handler(r, w):
            data = await r.read(64)
            w.write(b"echo:" + data)
            await w.drain()
            w.close()
        async def main():
            server = await paio.start_server(handler, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]
            r, w = await paio.open_connection(host, port)
            w.write(b"hi")
            await w.drain()
            data = await r.read(64)
            w.close()
            server.close()
            return data
        self.assertEqual(paio.run(main()), b"echo:hi")


if __name__ == "__main__":
    unittest.main()
