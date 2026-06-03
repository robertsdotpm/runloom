"""profile/target.py -- a sustained, scheduler-bound runloom workload to profile.

Unlike the one-shot tools/faultinj/workload.py, this runs a *bounded but
sustained* load (many channel ping-pong rounds) so a sampling profiler
(Coz / off-CPU / Scalene) gets a steady picture of the scheduler, channel and
park/wake hot paths rather than startup noise.

Each unit of work is wrapped in a Coz latency progress point when the optional
`coz` package is importable (pip install coz); harmless no-op otherwise. Coz
uses progress points to compute *virtual speedups* -- "if this code were X%
faster, the program would be Y% faster".

Env: PROFILE_UNITS (rounds, default 2000), PROFILE_PINGS (roundtrips/round, 500).
House style: .format(), no f-strings.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "..", "src"))
import runloom_c

try:
    import coz
except Exception:
    coz = None


def mark_begin():
    if coz is not None and hasattr(coz, "begin"):
        try:
            coz.begin("unit")
        except Exception:
            pass


def mark_end():
    if coz is not None and hasattr(coz, "end"):
        try:
            coz.end("unit")
        except Exception:
            pass


def ping_pong(n):
    a = runloom_c.Chan()
    b = runloom_c.Chan()

    def pinger():
        for i in range(n):
            a.send(i)
            b.recv()

    def ponger():
        for _ in range(n):
            v, _ = a.recv()
            b.send(v)

    runloom_c.go(pinger)
    runloom_c.go(ponger)
    runloom_c.run()


def main():
    units = int(os.environ.get("PROFILE_UNITS", "2000"))
    pings = int(os.environ.get("PROFILE_PINGS", "500"))
    if hasattr(runloom_c, "warmup"):
        runloom_c.warmup(2000)
    for _ in range(units):
        mark_begin()
        ping_pong(pings)
        mark_end()
    sys.stdout.write("PROFILE_TARGET_OK {} units x {} pings\n".format(units, pings))


if __name__ == "__main__":
    main()
