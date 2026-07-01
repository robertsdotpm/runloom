import threading, time
import runloom.aio as aio
loop = aio.RunloomEventLoop()
results = []
async def initial_task(): results.append('task')
loop.call_soon(lambda: results.append('cb'))
t_obj = loop.create_task(initial_task())
t = threading.Thread(target=loop.run_forever, daemon=True); t.start()
time.sleep(0.5); loop.stop(); t.join(timeout=5)
print('after worker run:', results, t_obj.done())
# Now run an UNRELATED aio.run on the creator (main) thread:
async def unrelated(): pass
aio.run(unrelated())
print('after unrelated aio.run on creator thread:', results, t_obj.done())
