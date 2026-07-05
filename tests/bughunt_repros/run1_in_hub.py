"""run(1, main_fn) called inside an M:N hub fiber returns WITHOUT running main_fn.
Docstring: 'run(1) re-entrancy IS supported'. Observed: it returns immediately;
the inner main_fn runs later on the hubs (or its result is lost)."""
import time
import runloom

res = []

def outer():
    def inner():
        res.append("inner-ran")
    n = runloom.run(1, inner)   # should block until inner finishes
    res.append(("returned", "inner_done" if "inner-ran" in res else "INNER NOT RUN YET", "count", n))

runloom.run(4, outer)
print(res)
