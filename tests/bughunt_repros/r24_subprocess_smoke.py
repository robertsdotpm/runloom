"""Subprocess smoke: communicate, returncode, stdin round-trip, kill."""
import sys, asyncio
import runloom.aio as aio

async def main():
    p = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import sys; print(sys.stdin.read().upper())",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    out, err = await asyncio.wait_for(p.communicate(b"hello"), 20)
    assert out.strip() == b"HELLO", out
    assert p.returncode == 0

    p2 = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(60)")
    await asyncio.sleep(0.2)
    p2.kill()
    rc = await asyncio.wait_for(p2.wait(), 20)
    assert rc != 0
    return "OK"

print(aio.run(main()))
