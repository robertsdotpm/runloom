"""net_echo_forever.py -- continuous pygo echo client soak over the REAL internet.

THIS machine runs runloom (M:N) client fibers that connect to a remote TCP echo
server (default ovh1.p2pd.net:7, the inetd echo service) OVER THE INTERNET, send
a payload, read it echoed back, and verify it byte-for-byte -- forever, in ONE
long-lived process.  Unlike the loopback cserve_echo soak this exercises pygo's
netpoll against real WAN conditions (hundreds-of-ms RTT, real connect() churn,
real packet loss / resets) instead of 127.0.0.1.

Design goal: STAY ALIVE as a single continuous run (NOT chopped into hourly
iterations) so that if the runtime actually CRASHES the process dies and we see
it -- a restart wrapper would hide the crash.  Expected WAN errors (connection
reset / timeout) are counted and the fiber reconnects; only a real runtime crash
(SIGSEGV / abort / unhandled non-OSError) or an ECHO INTEGRITY mismatch is a
finding.  faulthandler dumps a traceback on a fatal signal; `kill -USR1 <pid>`
dumps every fiber-thread's live stack (a HANG is then as visible as a crash).

Env knobs:
  RUNLOOM_ECHO_HOST    target host      (default ovh1.p2pd.net)
  RUNLOOM_ECHO_PORT    target port      (default 7)
  RUNLOOM_ECHO_FAM     4 | 6 | any      (default 6 -- v6 is the faster path)
  RUNLOOM_ECHO_HUBS    M:N hubs         (default 4)
  RUNLOOM_ECHO_CONC    concurrent client fibers   (default 16)
  RUNLOOM_ECHO_RTRIPS  echo round-trips per connection before reconnect (default 32)
  RUNLOOM_ECHO_PAYLOAD payload bytes    (default 64)
  RUNLOOM_ECHO_REPORT  seconds between progress lines (default 15)
"""
import faulthandler
import os
import signal
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import runloom
import runloom_c

HOST = os.environ.get("RUNLOOM_ECHO_HOST", "ovh1.p2pd.net")
PORT = int(os.environ.get("RUNLOOM_ECHO_PORT", "7"))
FAM = os.environ.get("RUNLOOM_ECHO_FAM", "6")
HUBS = int(os.environ.get("RUNLOOM_ECHO_HUBS", "4"))
CONC = int(os.environ.get("RUNLOOM_ECHO_CONC", "16"))
RTRIPS = int(os.environ.get("RUNLOOM_ECHO_RTRIPS", "32"))
PAYLEN = int(os.environ.get("RUNLOOM_ECHO_PAYLOAD", "64"))
REPORT = float(os.environ.get("RUNLOOM_ECHO_REPORT", "15"))


def resolve_target():
    """Resolve ONCE to an address literal so TCPConn.connect never blocks a hub
    on a synchronous getaddrinfo (DNS is not cooperative).  Prefer the requested
    family; fall back to any."""
    want = {"4": socket.AF_INET, "6": socket.AF_INET6}.get(FAM)
    ai = socket.getaddrinfo(HOST, PORT, 0, socket.SOCK_STREAM)
    cands = [a[0] for f, _, _, _, a in ai if want is None or f == want]
    if not cands:
        cands = [a[0] for f, _, _, _, a in ai]
    return cands[0]


ADDR = resolve_target()
PAYLOAD = (b"pygo-net-echo-" * ((PAYLEN // 14) + 1))[:PAYLEN]
STOP = [False]

# Per-fiber counter slots (one slot per client fiber -> race-free with the GIL
# off; the reporter only SUMS them, never writes).  A shared += loses counts.
rts = [0] * CONC          # completed & verified echo round-trips
neterr = [0] * CONC       # expected WAN errors (reset / timeout) -> reconnect
integ = [0] * CONC        # ECHO INTEGRITY mismatches -- REAL findings
reconn = [0] * CONC       # reconnects
rtt_sum = [0.0] * CONC    # summed RTT seconds
rtt_max = [0.0] * CONC    # worst RTT seconds


def client(i):
    buf = bytearray(PAYLEN)
    mv = memoryview(buf)
    while not STOP[0]:
        try:
            c = runloom_c.TCPConn.connect(ADDR, PORT)
        except OSError:
            neterr[i] += 1
            runloom_c.sched_sleep(0.5)          # brief backoff, then retry
            continue
        try:
            for _ in range(RTRIPS):
                if STOP[0]:
                    break
                t0 = time.monotonic()
                c.send_all(PAYLOAD)
                got = 0
                while got < PAYLEN:
                    n = c.recv_into(mv[got:])
                    if not n:
                        raise OSError("early EOF from echo server")
                    got += n
                dt = time.monotonic() - t0
                if bytes(buf[:PAYLEN]) != PAYLOAD:
                    integ[i] += 1
                    sys.stderr.write(
                        "[net_echo] INTEGRITY MISMATCH fiber={0} after {1} RTs: "
                        "echo != sent\n".format(i, rts[i]))
                    sys.stderr.flush()
                rts[i] += 1
                rtt_sum[i] += dt
                if dt > rtt_max[i]:
                    rtt_max[i] = dt
        except OSError:
            neterr[i] += 1                       # WAN reset / timeout -> reconnect
        finally:
            try:
                c.close()
            except OSError:
                pass
            reconn[i] += 1


def reporter(t_start):
    last_total = 0
    last_t = t_start
    while not STOP[0]:
        runloom_c.sched_sleep(REPORT)
        now = time.monotonic()
        total = sum(rts)
        dt = now - last_t
        rate = (total - last_total) / dt if dt > 0 else 0.0
        avg_rtt = (sum(rtt_sum) / total * 1000.0) if total else 0.0
        worst = (max(rtt_max) if rtt_max else 0.0) * 1000.0
        sys.stderr.write(
            "[net_echo] up={0:.0f}s RTs={1} rate={2:.0f}/s neterr={3} "
            "integrity={4} reconn={5} rtt_avg={6:.0f}ms rtt_max={7:.0f}ms "
            "-> {8}:{9}\n".format(
                now - t_start, total, rate, sum(neterr), sum(integ),
                sum(reconn), avg_rtt, worst, ADDR, PORT))
        sys.stderr.flush()
        last_total = total
        last_t = now


def root():
    t_start = time.monotonic()
    runloom.fiber(lambda: reporter(t_start))
    for i in range(CONC):
        runloom.fiber(lambda i=i: client(i))
    # Clients loop forever, so root must never return (a return tears the run
    # down) -- park it until a signal flips STOP.
    while not STOP[0]:
        runloom_c.sched_sleep(3600)


def request_stop(signum, frame):
    STOP[0] = True


def main():
    faulthandler.enable()
    faulthandler.register(signal.SIGUSR1, all_threads=True)   # kill -USR1 == live stacks
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    sys.stderr.write(
        "[net_echo] START pid={0} target={1}:{2} (fam={3}) hubs={4} conc={5} "
        "rtrips={6} payload={7}B -- continuous, NO restart (a crash stays "
        "crashed so it is visible)\n".format(
            os.getpid(), ADDR, PORT, FAM, HUBS, CONC, RTRIPS, PAYLEN))
    sys.stderr.flush()
    runloom.run(HUBS, main_fn=root)


if __name__ == "__main__":
    main()
