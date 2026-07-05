import runloom.monkey as monkey
monkey.patch()
import concurrent.futures as cf
import runloom_c as rc
import os

def rss():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])  # kB

def main():
    ex = cf.ThreadPoolExecutor(max_workers=4)
    for i in range(1000):
        ex.submit(lambda: None).result()
    base = rss()
    for i in range(100000):
        ex.submit(lambda: None).result()
    after = rss()
    print("pending:", len(ex._pending))
    print("RSS growth for 100k extra submits: %d kB (%.1f bytes/task)" %
          (after - base, (after - base) * 1024.0 / 100000))

rc.fiber(main); rc.run()
