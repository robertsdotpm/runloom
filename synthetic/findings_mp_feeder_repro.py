# Minimal repro: multiprocessing.Queue's internal _feed daemon OS thread uses
# monkey-patched cooperative threading primitives concurrently with goroutines
# under M:N (run(8)); under free-threading this races/UAFs at teardown.
# Flaky -- run under load (many copies) to hit it.  ~1/1000 normal, ~0.6% stressed.
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import multiprocessing as mp
import runloom

def child(q):
    q.get(); raise RuntimeError("intended")

def main():
    runloom.monkey.patch()
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    p = ctx.Process(target=child, args=(q,))
    state = {}
    def root():
        p.start(); q.put(b"x" * 300000); p.join(); state["x"] = p.exitcode
    runloom.run(8, root)
    q.close()
    print("OK", state)

if __name__ == "__main__":
    main()
