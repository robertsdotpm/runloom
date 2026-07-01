"""Protocol fidelity: a custom task factory installing STOCK asyncio.Task
(supported per loop_schedule._pg_make_task docs) -- can the stock C Task drive
runloom futures (asyncio.sleep -> RunloomFuture via call_later)?"""
import sys, asyncio
import runloom.aio as aio

async def main():
    loop = asyncio.get_event_loop()
    loop.set_task_factory(lambda loop, coro, **kw: asyncio.Task(coro, loop=loop, **kw))
    async def sub():
        await asyncio.sleep(0.01)
        f = loop.create_future()          # RunloomFuture
        loop.call_later(0.01, f.set_result, "hi")
        return await f
    t = loop.create_task(sub())
    r = await asyncio.wait_for(t, 5)
    assert isinstance(t, asyncio.Task) and type(t) is not aio.RunloomTask
    loop.set_task_factory(None)
    return r

r = aio.run(main())
print("stock-task result:", r)
assert r == "hi"
print("OK")
