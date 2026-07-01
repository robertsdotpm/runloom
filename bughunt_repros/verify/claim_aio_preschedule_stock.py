import threading, time, asyncio
loop = asyncio.new_event_loop()
results = []
async def initial_task(): results.append('task')
loop.call_soon(lambda: results.append('cb'))
t_obj = loop.create_task(initial_task())
t = threading.Thread(target=loop.run_forever, daemon=True); t.start()
time.sleep(1.0); loop.call_soon_threadsafe(loop.stop); t.join(timeout=5)
print('results:', results, 'task done:', t_obj.done())
