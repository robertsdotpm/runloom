import os, sys, threading, time, queue
q = queue.SimpleQueue()
results = []
def consumer():
    results.append(q.get())
def villain():
    time.sleep(0.05)
    q.put("x")
    assert q.get() == "x"
    time.sleep(0.05)
    q.put("y")
    time.sleep(0.3)
    print("qsize after second put:", q.qsize(), "results:", results)
t1 = threading.Thread(target=consumer); t2 = threading.Thread(target=villain)
t1.start(); t2.start(); t1.join(timeout=5); t2.join()
print("DONE, results:", results, "consumer alive:", t1.is_alive())
