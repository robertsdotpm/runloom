import runloom.monkey as monkey
monkey.patch()
import time
import runloom_c as rc
from runloom.monkey import offload, _raw_time_sleep

lat = [None]

def slow():
    offload(_raw_time_sleep, 3.0)

def quick_open():
    time.sleep(0.1)  # let slow() submit first
    t0 = time.monotonic()
    with open("/etc/hostname") as f:
        f.read()
    lat[0] = time.monotonic() - t0

rc.fiber(slow)
rc.fiber(quick_open)
rc.run()
print("open() latency behind a 3s offloaded sleep: %.2fs" % lat[0])
print("HEAD-OF-LINE BLOCKED" if lat[0] > 2.0 else "OK")
