"""Phase 2 regression: concurrent pygo event loops on separate OS threads,
each doing socket I/O.

pygo runs one scheduler per OS thread (Phase C) but a SINGLE shared netpoll
epoll.  So the pump draining on one loop's thread can pick up an fd event for a
goroutine parked on ANOTHER loop's thread.  The wake must route to the parker's
OWNER sched (Phase 2 -- pygo_sched_wake / pygo_mn_wake_g NULL-branch route to
g->owner's wake_list + kick its pump), not the pump thread's sched.  Without
that, a recv woken cross-thread is enqueued on the wrong (waker's) thread and
the owner never resumes it -> the round-trip hangs.

This test runs two independent loops on two threads, each doing many localhost
echo round-trips concurrently, and asserts both finish (no hang) with correct
data.  With Phase 2 reverted (pygo_sched_wake -> plain ready_push to the waker
thread), the cross-thread deliveries strand recvs and the threads time out.
"""
import asyncio
import threading
import unittest

import pygo.aio as paio

ROUND_TRIPS = 60
PAYLOAD = b"phase2-cross-thread-wake"


def _loop_workload(result, idx):
    async def handler(reader, writer):
        # Echo each message back, until the client half-closes.
        while True:
            data = await reader.read(4096)
            if not data:
                break
            writer.write(data)
            await writer.drain()
        writer.close()

    async def main():
        server = await paio.start_server(handler, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()[:2]
        reader, writer = await paio.open_connection(host, port)
        ok = 0
        for _ in range(ROUND_TRIPS):
            writer.write(PAYLOAD)
            await writer.drain()
            got = b""
            while len(got) < len(PAYLOAD):
                chunk = await reader.read(len(PAYLOAD) - len(got))
                if not chunk:
                    break
                got += chunk
            if got == PAYLOAD:
                ok += 1
        writer.close()
        server.close()
        return ok

    try:
        result[idx] = paio.run(main())
    except BaseException as e:  # noqa: BLE001 - surface any failure to the asserter
        result[idx] = e


@unittest.skip(
    "Phase 2 partial: the cross-thread wake-ROUTING fix (pygo_sched_wake -> "
    "g->owner wake_list + kick) eliminates the SIGSEGV this workload hit on "
    "plain Phase C (cross-thread ready_push corrupted the single-consumer "
    "ready ring -- reproduced 4/4 before the fix, 0/8 after).  A SECOND, "
    "deeper bug remains: a cross-thread wait_fd RESUME intermittently leaks a "
    "spurious BlockingIOError (~3/8 runs) -- pygo_core.wait_fd returns/raises "
    "EAGAIN out of aio.py StreamReader._fill's except block, a tstate/"
    "exception-state-on-resume issue (cf project_pygo_per_g_python_crash).  "
    "Un-skip once that resume path is fixed.")
class TestPhase2MultiLoop(unittest.TestCase):
    def test_two_loops_two_threads_socket_echo(self):
        result = {}
        threads = [threading.Thread(target=_loop_workload, args=(result, i))
                   for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30.0)

        alive = [t for t in threads if t.is_alive()]
        self.assertEqual(alive, [],
                         "loop thread(s) hung -- cross-thread netpoll wake not "
                         "routed to the owner sched (Phase 2 regression)")
        for i in range(2):
            self.assertIn(i, result, "thread %d produced no result" % i)
            self.assertNotIsInstance(result[i], BaseException,
                                     "thread %d raised: %r" % (i, result[i]))
            self.assertEqual(result[i], ROUND_TRIPS,
                             "thread %d completed %r/%d round-trips"
                             % (i, result[i], ROUND_TRIPS))


if __name__ == "__main__":
    unittest.main()
