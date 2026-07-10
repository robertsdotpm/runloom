"""linz -- a generative linearizability battery for runloom's concurrent objects.

This is the *abstract* generalization of tests/big_100 (which is concrete: 170-odd
hand-written programs).  Instead of enumerating programs, `linz` is built from:

  * checker.py   -- a pure-Python Wing-&-Gong (WGL) linearizability checker, a
                    faithful port of the algorithm behind Porcupine
                    (github.com/anishathalye/porcupine, MIT).  Reads the SAME
                    JSON history format as tools/lincheck/porcupine so the Go
                    binary can cross-check any Chan history (differential trust).
  * specs.py     -- ~15-line sequential reference models per primitive
                    (Chan-FIFO, Mutex, RWMutex, counting Semaphore, WaitGroup,
                    Event).  Coverage is UNBOUNDED from these tiny specs.
  * workloads.py -- random concurrent op-sequence generators (seed -> program).
  * record.py    -- runs a workload on the REAL M:N scheduler and records the
                    call/return history.  Two modes: wall-clock (genuine real-time
                    overlap) and DST-seeded (RUNLOOM_MN_SEED -> deterministic
                    logical-clock history, so every non-linearizable finding
                    reduces to a single reproducible integer seed).
  * battery.py   -- the driver: sweep seeds x primitives, record, check, and on a
                    NOT-LINEARIZABLE verdict print the seed + the minimal history.

Runs GIL-off on free-threaded CPython 3.14t (the M:N scheduler is only real with
the GIL off).  House style: %/.format, no f-strings, prints kept, no leading
underscores on public names.
"""
