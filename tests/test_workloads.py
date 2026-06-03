"""Realistic workload patterns: scenarios that approximate how real
applications use runloom / paio.

Each test exercises a complete request/response or pipeline pattern
end-to-end -- the goal is to surface integration bugs that pure unit
tests miss (e.g. buffer reassembly, partial reads, backpressure,
cancellation cascading through a server).
"""
import asyncio
import os
import socket
import unittest

import runloom_c
import runloom.aio as paio


def _pick_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ====================================================================
# HTTP-like request/response (without real HTTP parsing)
# ====================================================================
class TestHttpLike(unittest.TestCase):
    def test_one_shot_request_response(self):
        port = _pick_port()

        async def handler(reader, writer):
            line = await reader.readline()
            assert line == b"PING\n", repr(line)
            writer.write(b"PONG\n")
            await writer.drain()
            writer.close()

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", port)
            r, w = await paio.open_connection("127.0.0.1", port)
            w.write(b"PING\n")
            await w.drain()
            resp = await r.readline()
            w.close()
            server.close()
            return resp

        self.assertEqual(paio.run(main()), b"PONG\n")

    def test_pipelined_requests(self):
        """Client sends N requests back-to-back; server echoes each."""
        port = _pick_port()
        N = 50

        async def handler(reader, writer):
            for _ in range(N):
                line = await reader.readline()
                if not line:
                    break
                writer.write(b"echo:" + line)
            await writer.drain()
            writer.close()

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", port)
            r, w = await paio.open_connection("127.0.0.1", port)
            for i in range(N):
                w.write(("req%d\n" % i).encode())
            await w.drain()
            out = []
            for i in range(N):
                out.append(await r.readline())
            w.close()
            server.close()
            return out

        out = paio.run(main())
        self.assertEqual(len(out), N)
        for i, line in enumerate(out):
            self.assertEqual(line, ("echo:req%d\n" % i).encode())

    def test_large_payload_streaming(self):
        """Read a 1MB payload in pieces; verify reassembly."""
        port = _pick_port()
        SIZE = 1024 * 1024
        payload = (b"abcdefgh" * (SIZE // 8))[:SIZE]

        async def handler(reader, writer):
            await reader.readline()    # request marker
            writer.write(payload)
            await writer.drain()
            writer.close()

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", port)
            r, w = await paio.open_connection("127.0.0.1", port)
            w.write(b"GIVE\n")
            await w.drain()
            chunks = []
            got = 0
            while got < SIZE:
                chunk = await r.read(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                got += len(chunk)
            w.close()
            server.close()
            return b"".join(chunks)

        got = paio.run(main())
        self.assertEqual(len(got), SIZE)
        self.assertEqual(got, payload)


# ====================================================================
# Worker-pool pattern
# ====================================================================
class TestWorkerPool(unittest.TestCase):
    def test_n_workers_consume_m_jobs(self):
        """Classic Go pattern: a pool of N workers reading from a job
        channel, writing to a results channel."""
        N_WORKERS = 8
        N_JOBS = 200

        jobs = runloom_c.Chan(N_JOBS)
        results = runloom_c.Chan(N_JOBS)

        def worker():
            while True:
                v, ok = jobs.recv()
                if not ok:
                    return
                results.send(v * 2)

        def feeder():
            for i in range(N_JOBS):
                jobs.send(i)
            jobs.close()

        for _ in range(N_WORKERS):
            runloom_c.go(worker)
        runloom_c.go(feeder)

        out = []
        def collector():
            for _ in range(N_JOBS):
                v, _ = results.recv()
                out.append(v)
        runloom_c.go(collector)
        runloom_c.run()
        self.assertEqual(sorted(out), [i * 2 for i in range(N_JOBS)])

    def test_pipeline_three_stages(self):
        """Stage 1 emits 0..99, stage 2 doubles, stage 3 sums."""
        in_ch = runloom_c.Chan(10)
        mid_ch = runloom_c.Chan(10)
        out_ch = runloom_c.Chan(1)

        def stage1():
            for i in range(100):
                in_ch.send(i)
            in_ch.close()

        def stage2():
            for v in in_ch:
                mid_ch.send(v * 2)
            mid_ch.close()

        def stage3():
            total = 0
            for v in mid_ch:
                total += v
            out_ch.send(total)

        runloom_c.go(stage1)
        runloom_c.go(stage2)
        runloom_c.go(stage3)
        runloom_c.run()
        v, _ = out_ch.recv()
        self.assertEqual(v, sum(i * 2 for i in range(100)))


# ====================================================================
# Cancellation cascade through a real server
# ====================================================================
class TestCancellationCascade(unittest.TestCase):
    def test_client_disconnect_unblocks_server_read(self):
        port = _pick_port()
        server_saw_eof = []

        async def handler(reader, writer):
            # Read until EOF
            while True:
                data = await reader.read(1024)
                if not data:
                    server_saw_eof.append(True)
                    break
            writer.close()

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", port)
            r, w = await paio.open_connection("127.0.0.1", port)
            w.write(b"hi\n")
            await w.drain()
            w.close()                       # client EOFs
            # Give server time to notice EOF.
            await asyncio.sleep(0.05)
            server.close()

        paio.run(main())
        self.assertEqual(server_saw_eof, [True])


# ====================================================================
# UDP echo
# ====================================================================
class TestUdpEcho(unittest.TestCase):
    def test_udp_echo(self):
        port = _pick_port()
        out = []

        class EchoProto(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                out.append(data)
                self.transport.sendto(data, addr)
            def connection_made(self, transport):
                self.transport = transport

        async def main():
            loop = asyncio.get_running_loop()
            transport, proto = await loop.create_datagram_endpoint(
                EchoProto, local_addr=("127.0.0.1", port))
            # Send from a plain socket.
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setblocking(False)
            s.sendto(b"hello", ("127.0.0.1", port))
            # Wait for server to echo.
            for _ in range(50):
                await asyncio.sleep(0.01)
                try:
                    data, _addr = s.recvfrom(1024)
                    s.close()
                    transport.close()
                    return data
                except BlockingIOError:
                    pass
            s.close()
            transport.close()
            return None

        self.assertEqual(paio.run(main()), b"hello")


# ====================================================================
# Mixed asyncio + raw goroutine
# ====================================================================
class TestMixedExecution(unittest.TestCase):
    def test_goroutine_feeds_into_asyncio_chan(self):
        """A raw goroutine produces values; an asyncio task consumes
        them via Chan."""
        ch = runloom_c.Chan(50)
        N = 30
        out = []

        def producer():
            for i in range(N):
                ch.send(i)
            ch.close()

        async def consumer():
            # Bridge chan recv into asyncio via run_in_executor-like
            # pattern: poll try_recv with yield.
            count = 0
            while count < N:
                v = ch.try_recv()
                if v is None:
                    await asyncio.sleep(0.001)
                    continue
                val, ok = v
                if not ok:
                    break
                out.append(val)
                count += 1

        async def main():
            runloom_c.go(producer)
            await consumer()

        paio.run(main())
        self.assertEqual(out, list(range(N)))


# ====================================================================
# Resource cleanup
# ====================================================================
class TestResourceCleanup(unittest.TestCase):
    def test_server_close_releases_port(self):
        """Open + close a server on a port, then reopen on the SAME port."""
        port = _pick_port()

        async def handler(reader, writer):
            writer.close()

        async def cycle():
            server = await paio.start_server(handler, "127.0.0.1", port)
            server.close()
            try:
                await server.wait_closed()
            except Exception:
                pass

        async def main():
            for _ in range(3):
                await cycle()
            return "ok"

        self.assertEqual(paio.run(main()), "ok")


if __name__ == "__main__":
    unittest.main()
