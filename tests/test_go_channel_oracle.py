"""Differential channel/select conformance: runloom vs COMPILED Go (item 8, Go half).

runloom's channels promise Go semantics.  The oracle for that promise is the Go
compiler itself, not a model: each scenario is written twice -- once in Go, once
in runloom -- and reduced to the SAME normalized outcome string (delivered
values with their comma-ok flags, close behaviour, panic/raise, fan-in sums).
A divergence is a real semantics bug in runloom's chan/select, surfaced against
ground truth instead of via a downstream deadlock.

Covers the deterministic core (bugs in the chan/select/park class): buffered
drain-then-closed, unbuffered handoff, recv from a closed empty channel, send on
a closed channel, select over a single ready case, and cross-goroutine fan-in.
Skips cleanly if the Go toolchain is absent.

House style: %/.format, prints kept.
"""
import os
import shutil
import subprocess
import sys
import tempfile

import pytest

PY = sys.executable
ENV = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
GO = shutil.which("go")

pytestmark = pytest.mark.skipif(GO is None, reason="Go toolchain not installed")


# Each scenario: (go_source, runloom_body).  BOTH must print a line
# "OUTCOME: <s>"; the normalized <s> is compared.  Normalization is baked into
# each side so a conforming runtime prints byte-identical tails.
SCENARIOS = {
    # buffered cap-3: fill, close, recv 5x -> 3 values ok, then zero+notok twice.
    # Normalize the closed-zero to "Z" so Go's 0 and runloom's None agree.
    "buffered_drain_then_closed": (
        r'''
package main
import "fmt"
func main() {
    ch := make(chan int, 3)
    ch <- 10; ch <- 20; ch <- 30; close(ch)
    s := ""
    for i := 0; i < 5; i++ {
        v, ok := <-ch
        if ok { s += fmt.Sprintf("%dT ", v) } else { s += "ZF " }
    }
    fmt.Println("OUTCOME:", s)
}
''',
        r'''
def body():
    ch = rc.Chan(3)
    ch.send(10); ch.send(20); ch.send(30); ch.close()
    s = ""
    for _ in range(5):
        v, ok = ch.recv()
        s += ("%dT " % v) if ok else "ZF "
    print("OUTCOME:", s)
rc.fiber(body); rc.run()
'''),

    # unbuffered handoff: a goroutine sends, main receives.
    "unbuffered_handoff": (
        r'''
package main
import "fmt"
func main() {
    ch := make(chan int)
    go func(){ ch <- 42 }()
    v, ok := <-ch
    fmt.Printf("OUTCOME: %dT ok=%v\n", v, ok)
}
''',
        r'''
def body():
    ch = rc.Chan(0)
    rc.fiber(lambda: ch.send(42))
    v, ok = ch.recv()
    print("OUTCOME: %dT ok=%s" % (v, "true" if ok else "false"))
rc.fiber(body); rc.run()
'''),

    # recv from a closed, empty channel: zero + not-ok, immediately.
    "recv_closed_empty": (
        r'''
package main
import "fmt"
func main() {
    ch := make(chan int)
    close(ch)
    v, ok := <-ch
    _ = v
    fmt.Printf("OUTCOME: closed ok=%v\n", ok)
}
''',
        r'''
def body():
    ch = rc.Chan(0); ch.close()
    v, ok = ch.recv()
    print("OUTCOME: closed ok=%s" % ("true" if ok else "false"))
rc.fiber(body); rc.run()
'''),

    # send on a closed channel: Go panics; runloom must raise.  Both normalize to
    # "send-on-closed-error".
    "send_on_closed": (
        r'''
package main
import "fmt"
func main() {
    defer func(){ if recover() != nil { fmt.Println("OUTCOME: send-on-closed-error") } }()
    ch := make(chan int, 1); close(ch)
    ch <- 1
    fmt.Println("OUTCOME: no-error")
}
''',
        r'''
def body():
    ch = rc.Chan(1); ch.close()
    try:
        ch.send(1); print("OUTCOME: no-error")
    except Exception:
        print("OUTCOME: send-on-closed-error")
rc.fiber(body); rc.run()
'''),

    # select over a single ready case: must pick it deterministically.
    "select_one_ready": (
        r'''
package main
import "fmt"
func main() {
    a := make(chan int, 1); b := make(chan int, 1)
    a <- 7
    select {
    case v := <-a: fmt.Printf("OUTCOME: got-a %d\n", v)
    case v := <-b: fmt.Printf("OUTCOME: got-b %d\n", v)
    }
}
''',
        r'''
def body():
    a = rc.Chan(1); b = rc.Chan(1)
    a.send(7)
    idx, payload = rc.select([("recv", a), ("recv", b)])
    v, ok = payload
    print("OUTCOME: got-%s %d" % ("a" if idx == 0 else "b", v))
rc.fiber(body); rc.run()
'''),

    # fan-in: P goroutines each send their id; main sums -> deterministic total.
    "fan_in_sum": (
        r'''
package main
import "fmt"
func main() {
    const P = 8
    ch := make(chan int, P)
    for i := 0; i < P; i++ { go func(x int){ ch <- x }(i) }
    sum := 0
    for i := 0; i < P; i++ { v, _ := <-ch; sum += v }
    fmt.Printf("OUTCOME: sum=%d\n", sum)
}
''',
        r'''
def body():
    P = 8; ch = rc.Chan(P)
    for i in range(P): rc.fiber(lambda x=i: ch.send(x))
    total = 0
    for _ in range(P):
        v, _ = ch.recv(); total += v
    print("OUTCOME: sum=%d" % total)
rc.fiber(body); rc.run()
'''),
}


def go_outcome(src, tmp):
    path = os.path.join(tmp, "prog.go")
    open(path, "w").write(src)
    p = subprocess.run([GO, "run", path], capture_output=True, text=True,
                       timeout=60)
    return extract(p.stdout), p


def runloom_outcome(body):
    script = "import runloom_c as rc, runloom\n" + body
    p = subprocess.run([PY, "-c", script], env=ENV, capture_output=True,
                       text=True, timeout=45)
    return extract(p.stdout), p


def extract(out):
    for line in out.splitlines():
        if line.startswith("OUTCOME:"):
            return line[len("OUTCOME:"):].strip()
    return None


def run_scenario(name, tmp):
    go_src, rl_body = SCENARIOS[name]
    g, gp = go_outcome(go_src, tmp)
    r, rp = runloom_outcome(rl_body)
    return g, r, gp, rp


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_go_conformance(name):
    with tempfile.TemporaryDirectory() as tmp:
        g, r, gp, rp = run_scenario(name, tmp)
    assert g is not None, "Go produced no OUTCOME: %s" % gp.stderr[-400:]
    assert r is not None, "runloom produced no OUTCOME: %s" % rp.stderr[-400:]
    assert g == r, ("scenario %r diverges from Go:\n  go     : %r\n  runloom: %r"
                    % (name, g, r))


def main():
    fails = []
    with tempfile.TemporaryDirectory() as tmp:
        for name in SCENARIOS:
            g, r, gp, rp = run_scenario(name, tmp)
            ok = (g is not None and g == r)
            print("  %-28s %s" % (name, "OK" if ok else "DIVERGES"))
            if not ok:
                print("      go     : %r" % g)
                print("      runloom: %r" % r)
                fails.append(name)
    if fails:
        print("GO-DIFFERENTIAL FAIL: %s" % ", ".join(fails))
        return 1
    print("all %d scenarios conform to compiled Go" % len(SCENARIOS))
    return 0


if __name__ == "__main__":
    if GO is None:
        print("SKIP: Go toolchain not installed")
        sys.exit(0)
    sys.exit(main())
