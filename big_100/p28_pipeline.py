"""big_100 / 28 -- pipeline builder.

Each goroutine builds a shell-style 3-stage pipeline -- producer | filter |
consumer -- by chaining subprocess pipes, then reads the final output and
verifies it.  Many such pipelines run concurrently.

Stresses: pipe backpressure between processes, subprocess lifecycle, fd
plumbing.
"""
import subprocess

import procutil

import harness
import netutil

PRODUCE = "import sys\nfor i in range({0}): sys.stdout.write('%d\\n' % i)\n"
# filter: keep even numbers; consumer: sum them.  We compute both in Python -c.
FILTER = ("import sys\n"
          "for line in sys.stdin:\n"
          "    n=int(line)\n"
          "    if n % 2 == 0: sys.stdout.write(line)\n")


def expected_sum(n):
    return sum(i for i in range(n) if i % 2 == 0)


def worker(H, wid, rng, state):
    py = state["py"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        n = rng.randint(50, 500)
        p1 = p2 = None
        try:
            p1 = procutil.popen([py, "-c", PRODUCE.format(n)],
                                  stdout=subprocess.PIPE)
            p2 = procutil.popen([py, "-c", FILTER],
                                  stdin=p1.stdout, stdout=subprocess.PIPE)
            p1.stdout.close()       # let p2 own the read end
            # communicate() drains p2.stdout via a cooperative selector;
            # a raw .read() would bypass monkey and wedge the hub.
            out, _ = p2.communicate()
            p1.wait()
            total = sum(int(x) for x in out.split())
            if not H.check(total == expected_sum(n),
                           "pipeline sum {0} != {1} (n={2}) wid={3}".format(
                               total, expected_sum(n), n, wid)):
                return
            H.op(wid)
            H.task_done(wid)
        except OSError as e:
            if not H.running():
                break
            H.fail("pipeline error wid={0}: {1}".format(wid, e))
            return
        finally:
            for p in (p1, p2):
                if p is not None and p.poll() is None:
                    p.kill()
                    p.wait()


def setup(H):
    import sys
    H.state = {"py": sys.executable}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p28_pipeline", body, setup=setup, default_funcs=400,
                 describe="producer|filter|consumer subprocess pipelines")
