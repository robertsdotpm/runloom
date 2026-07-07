"""Run the whole remote-internet suite (n01..n05), one line per program.

Each n0* runs in its OWN subprocess so a crash/hang is isolated and attributable.
Exit-code -> verdict:  0 PASS, 77 SKIP(ENV) -> benign;  1 FINDING, 2 CRASH,
3 HANG (or a fatal signal) -> a real finding.  The n0* programs write their own
hang_hunter-format finding files under --report-dir/findings/ for exit 1/2; this
runner synthesizes one for exit 3 / signal deaths (the watchdog os._exit's
before it can write).  run_all itself exits 0 iff every program was PASS or SKIP.

Opt-in: does nothing unless RUNLOOM_NET_TESTS=1 (each child re-checks the gate).

  RUNLOOM_NET_TESTS=1 PYTHONPATH=src python3 tests/net/run_all_net.py \\
      --hubs 8 --top 32 --timeout 3 --report-dir docs/dev/soak/inbox_artifacts/net
"""
import argparse
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import netlist   # noqa: E402

PROGRAMS = ["n01_stun_tcp_v4", "n02_stun_tcp_v6", "n03_ntp_udp",
            "n04_mqtt_tcp", "n05_stun_udp_testnat"]

VERDICT = {0: "PASS", netlist.SKIP: "SKIP", netlist.FINDING: "FINDING",
           netlist.CRASH: "CRASH", netlist.HANG: "HANG"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hubs", type=int, default=8)
    ap.add_argument("--top", type=int, default=32)
    ap.add_argument("--timeout", type=float, default=3.0)
    ap.add_argument("--report-dir",
                    default=os.path.join(HERE, "_findings"))
    ap.add_argument("--only", default=None, help="comma-separated program stems")
    args = ap.parse_args()

    if not netlist.enabled():
        print("SKIP all: RUNLOOM_NET_TESTS!=1 (opt-in gate)")
        return 0

    os.makedirs(os.path.join(args.report_dir, "findings"), exist_ok=True)
    progs = PROGRAMS
    if args.only:
        want = set(args.only.split(","))
        progs = [p for p in PROGRAMS if p in want or p.split("_")[0] in want]

    env = dict(os.environ)
    env.setdefault("PYTHON_GIL", "0")
    env["RUNLOOM_NET_TESTS"] = "1"
    # keep PYTHONPATH=src so the child can import runloom
    src = os.path.join(os.path.dirname(os.path.dirname(HERE)), "src")
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")

    worst = 0
    for stem in progs:
        path = os.path.join(HERE, stem + ".py")
        cmd = [sys.executable, path, "--hubs", str(args.hubs), "--top",
               str(args.top), "--timeout", str(args.timeout),
               "--report-dir", args.report_dir]
        try:
            rc = subprocess.call(cmd, env=env)
        except Exception as e:                        # noqa: BLE001
            rc = netlist.CRASH
            print("CRASH   %s  (runner: %s)" % (stem, e))
        tag = VERDICT.get(rc if rc >= 0 else netlist.CRASH, "CRASH(rc=%d)" % rc)
        print("%-8s %s" % (tag, stem))
        if rc not in (0, netlist.SKIP):
            # For HANG / signal death the child couldn't write a finding; do it here.
            if rc == netlist.HANG:
                netlist.write_finding(args.report_dir, "net-hang", "%s|hang" % stem,
                                      "%s: watchdog HANG (rc=3) -- lost wake on a "
                                      "remote socket" % stem)
            elif rc < 0 or rc >= 128:
                netlist.write_finding(args.report_dir, "net-crash",
                                      "%s|signal" % stem,
                                      "%s: died on signal (rc=%d)" % (stem, rc))
            worst = max(worst, 1)
    return worst


if __name__ == "__main__":
    sys.exit(main())
