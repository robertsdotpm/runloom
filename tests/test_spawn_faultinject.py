"""Allocation-failure (OOM) injection at the goroutine-spawn alloc sites.

The netpoll/syscall fault campaign never exercised the *allocator*.  These
sites are where a real out-of-memory bites a spawn -- the g-struct calloc and
the coroutine stack mmap -- and the spawn error paths (drop the half-built g,
raise MemoryError) must be leak- and corruption-free.

RUNLOOM_FAULT_SPAWN_G / RUNLOOM_FAULT_SPAWN_STACK = once:CODE | always:CODE force
those allocations to "fail"; each scenario runs in its own interpreter
(the env is read once and cached).  Run under ASan to turn a leak/corruption
in the error path into a hard error.
"""
import os
import subprocess
import sys
import unittest

sys.path.insert(0, "src")


def _run(code, env_extra):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["RUNLOOM_SYSMON"] = "0"
    env["RUNLOOM_DEADLOCK"] = "off"
    env["PYTHONPATH"] = "src" + os.pathsep + env.get("PYTHONPATH", "")
    env.update(env_extra)
    p = subprocess.run([sys.executable, "-c", code], env=env,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       timeout=60)
    return p.returncode, p.stdout.decode("utf-8", "replace")

# Inject once, confirm MemoryError, then confirm the scheduler recovers
# (the next spawn succeeds and runs) and leaks nothing.
_RECOVER = """
import runloom, runloom_c as c
ran = []
try:
    c.go(lambda: ran.append('x'))
    print('NO_ERROR')
except MemoryError:
    print('OOM_RAISED')
c.go(lambda: ran.append('y'))   # fault was once -> succeeds
runloom.run_single()
print('RECOVERED' if ran == ['y'] and c.goroutine_count() == 0 else 'BAD')
"""

# Many always-failing spawns, then confirm no goroutine leaked.
_NOLEAK = """
import runloom_c as c
fails = 0
for _ in range(3000):
    try: c.go(lambda: None)
    except MemoryError: fails += 1
print('NOLEAK' if fails == 3000 and c.goroutine_count() == 0 else 'BAD:%d:%d' % (fails, c.goroutine_count()))
"""


class TestSpawnOOM(unittest.TestCase):
    def test_g_struct_oom_recovers(self):
        rc, out = _run(_RECOVER, {"RUNLOOM_FAULT_SPAWN_G": "once:12"})
        self.assertIn("OOM_RAISED", out)
        self.assertIn("RECOVERED", out)
        self.assertEqual(rc, 0)

    def test_stack_oom_recovers(self):
        rc, out = _run(_RECOVER, {"RUNLOOM_FAULT_SPAWN_STACK": "once:12"})
        self.assertIn("OOM_RAISED", out)
        self.assertIn("RECOVERED", out)
        self.assertEqual(rc, 0)

    def test_g_struct_oom_no_leak(self):
        rc, out = _run(_NOLEAK, {"RUNLOOM_FAULT_SPAWN_G": "always:12"})
        self.assertIn("NOLEAK", out, out)
        self.assertEqual(rc, 0)

    def test_stack_oom_no_leak(self):
        rc, out = _run(_NOLEAK, {"RUNLOOM_FAULT_SPAWN_STACK": "always:12"})
        self.assertIn("NOLEAK", out, out)
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
