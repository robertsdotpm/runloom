import os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))
import runloom
def noop(): pass
def root():
    for _ in range(80000):
        runloom.fiber(noop)
runloom.run(8, root)
# how many large VMAs carry the 'hg' (MADV_HUGEPAGE) VmFlag?
hg = big = 0
cur_sz = 0
for line in open("/proc/self/smaps"):
    if line and line[0].isdigit() or (line and line[0] in "0123456789abcdef" and "-" in line[:40] and "Size:" not in line):
        pass
    if line.startswith("Size:"):
        cur_sz = int(line.split()[1])
    elif line.startswith("VmFlags:"):
        if cur_sz > 100000:  # >100MB VMAs (the arena classes)
            big += 1
            if " hg" in line: hg += 1
print("big(>100MB) VMAs=%d  with hg flag=%d" % (big, hg))
for s in range(6):
    time.sleep(1.0)
    ahp = 0
    for line in open("/proc/self/smaps_rollup"):
        if line.startswith("AnonHugePages:"): ahp = int(line.split()[1])
    print("  t+%ds AnonHugePages=%d MB" % (s+1, ahp//1024))
