"""_AutoHot.resolve() (auto per-core scaling; optimize("throughput") /
RUNLOOM_HOT_AUTO=1) keys promotion on fn.__code__.  Distinct closure INSTANCES
from the same factory share one code object, so once one instance is promoted,
EVERY later spawn of any sibling closure resolves to the promoted instance --
and runs with the WRONG captured values."""
import os
os.environ["RUNLOOM_HOT_AUTO"] = "1"
os.environ["RUNLOOM_HOT_AUTO_AFTER"] = "8"   # promote quickly for the demo

import runloom

results = []

def make(tag):
    def handler():
        results.append(tag)
    return handler

def main():
    for i in range(100):
        runloom.fiber(make(i))   # 100 DISTINCT closures, one per tag
    runloom.sleep(1.0)

runloom.run(2, main)
distinct = sorted(set(results))
print("ran %d handlers; %d distinct tags (expected 100)" %
      (len(results), len(distinct)))
print("tags seen:", distinct[:20], "...")
from collections import Counter
mc = Counter(results).most_common(1)[0]
print("most common tag: %r appeared %d times (expected 1)" % mc)
if len(distinct) < 100:
    print("BUG: later spawns ran the PROMOTED closure's captures, not their own")
