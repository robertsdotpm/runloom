import multiprocessing as mp, _thread, time
l = mp.RLock()
l.acquire(); ok = l.acquire(False); assert ok; l.release()
print("stock c_count after acquire+acquire(False)+release:", l._semlock._count())
box = []
_thread.start_new_thread(lambda: box.append(l.acquire(False)), ())
t0 = time.monotonic()
while not box and time.monotonic() - t0 < 3: time.sleep(0.01)
print("stock foreign_got_lock:", box[0] if box else "timeout")
