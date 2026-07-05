import os
os.environ["RUNLOOM_HOT_AUTO"] = "1"
os.environ["RUNLOOM_HOT_AUTO_AFTER"] = "8"
import runloom
results = []
def make(tag):
    def handler():
        results.append(tag)
    return handler
def main():
    for i in range(100):
        runloom.fiber(make(i))   # 100 DISTINCT closures
    runloom.sleep(1.0)
runloom.run(2, main)
print(len(set(results)), "distinct tags (expected 100)")
from collections import Counter
c = Counter(results)
print("ran", len(results), "handlers; most common:", c.most_common(3))
