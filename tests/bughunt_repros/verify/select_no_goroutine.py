import time, runloom_c as rc
ch = rc.Chan()
t0=time.time()
try: ch.recv()
except RuntimeError: print('recv raised in %.3fs' % (time.time()-t0))
t0=time.time()
try: rc.select([('recv', ch)])
except RuntimeError as e: print('select raised after %.3fs: %s' % (time.time()-t0, e))
