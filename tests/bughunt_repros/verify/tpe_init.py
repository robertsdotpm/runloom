import sys, threading
from concurrent.futures import ThreadPoolExecutor as _stock_check  # ensure import before patch

USE_PATCH = "--patch" in sys.argv

calls = [0]
lock = threading.Lock()

def init():
    with lock:
        calls[0] += 1

def work(x):
    return x * 2

def main():
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4, initializer=init) as ex:
        futs = [ex.submit(work, i) for i in range(100)]
        results = [f.result() for f in futs]
    assert results == [i*2 for i in range(100)]
    print("initializer call count:", calls[0], "(max_workers=4, 100 submits)")

if USE_PATCH:
    import runloom
    from runloom import monkey
    monkey.patch()
    runloom.run(1, main)
else:
    main()
