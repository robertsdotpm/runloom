import threading, faulthandler
import runloom
faulthandler.dump_traceback_later(20, exit=True)
barrier = threading.Barrier(2)
def t(i):
    def work():
        for _ in range(50): runloom.yield_now()
    barrier.wait()
    try:
        runloom.run(2, work)
    except Exception as e:
        print('thread', i, 'raised', type(e).__name__)
ths = [threading.Thread(target=t, args=(i,)) for i in range(2)]
[x.start() for x in ths]; [x.join() for x in ths]
print('done')
