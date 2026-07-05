import runloom.monkey as monkey
monkey.patch()
import concurrent.futures as cf
ex = cf.ThreadPoolExecutor(max_workers=2)
f = ex.submit(lambda: 42)
try:
    print(f.result(timeout=2))
except Exception as e:
    print("BUG: never resolves:", type(e).__name__)
