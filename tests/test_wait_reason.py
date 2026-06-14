"""Wait-reason taxonomy: the deadlock/wedge dump subdivides the opaque
PARKED_SAFE "park" with the fiber's wait reason (future / waitgroup / lock /
...), set either explicitly via runloom_c.set_wait_reason or by the high-level
sync primitives, so an operator can see WHY each fiber is blocked.

Driven through a subprocess because the dump is written straight to fd 2 by the
deadlock census; raise mode makes the run return promptly after the dump.
"""
import os
import subprocess
import sys
import textwrap

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(HERE), "src")


def _dump_for(body):
    code = textwrap.dedent("""
        import sys
        sys.path.insert(0, {src!r})
        import runloom, runloom_c
        from runloom.sync import WaitGroup
        runloom_c.set_deadlock_mode(2)   # raise: dump then return
        def main():
            {body}
        try:
            runloom.run(2, main)
        except RuntimeError:
            pass
    """).format(src=SRC, body=body)
    env = dict(os.environ, PYTHON_GIL="0", RUNLOOM_DEADLOCK_MS="40",
               PYTHONUNBUFFERED="1", PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    p = subprocess.run([sys.executable, "-c", code], capture_output=True,
                       text=True, timeout=40, env=env)
    return p.stdout + p.stderr


def test_explicit_set_wait_reason_shows_in_dump():
    out = _dump_for("runloom_c.set_wait_reason(runloom_c.WR_FUTURE); runloom_c.park()")
    assert "park:future" in out, out


def test_waitgroup_primitive_tags_its_park():
    out = _dump_for("wg = WaitGroup(); wg.add(1); wg.wait()")
    assert "park:waitgroup" in out, out


def test_unset_reason_defaults_to_sync():
    out = _dump_for("runloom_c.park()")
    assert "park:sync" in out, out
