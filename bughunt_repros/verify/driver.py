import subprocess, sys, time, os, signal

PY = "/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo/.venv/bin/python"
REPRO = "/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/repros/verify/repro8.py"
TRIALS = int(sys.argv[1]) if len(sys.argv) > 1 else 40
BUDGET = float(sys.argv[2]) if len(sys.argv) > 2 else 8.0

misses = 0
lats = []
for t in range(TRIALS):
    p = subprocess.Popen([PY, REPRO], stdout=subprocess.DEVNULL,
                         stderr=subprocess.PIPE, text=False)
    t0 = time.time()
    seen = False
    buf = b""
    # read stderr incrementally with deadline
    os.set_blocking(p.stderr.fileno(), False)
    while time.time() - t0 < BUDGET:
        try:
            chunk = p.stderr.read()
        except Exception:
            chunk = None
        if chunk:
            buf += chunk
            if b"DEADLOCK" in buf or b"STALL" in buf:
                seen = True
                break
        time.sleep(0.02)
    lat = time.time() - t0
    p.kill(); p.wait()
    if seen:
        lats.append(lat)
    else:
        misses += 1
        print(f"trial {t}: MISS (no banner in {BUDGET}s)", flush=True)
print(f"trials={TRIALS} misses={misses} hit-latencies: min={min(lats):.3f} max={max(lats):.3f}" if lats else f"trials={TRIALS} misses={misses}")
