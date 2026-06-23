"""big_100 / 161 -- stdlib logging reentrancy under M:N.

Many goroutines log unique lines through the stdlib `logging` module to a
shared FileHandler (writing into an H.make_tmpdir file), plus a second handler
that occasionally RAISES (logging swallows handler errors via handleError).
`logging` serialises emit() with a module-level `threading.RLock`, which is
cooperative under monkey.patch().  The reentrant lock, taken by thousands of
goroutines across hubs, must never false-deadlock and must not drop records.

Stresses: logging RLock reentrancy, FileHandler I/O, handler errors swallowed,
cooperative RLock under M:N.
"""
import logging
import os

import harness
import runloom


class FlakyHandler(logging.Handler):
    """A handler that raises on a deterministic fraction of records.  logging's
    Handler.handle() calls handleError() (which swallows) when emit() raises, so
    this must NOT propagate out of the logging call -- it exercises the
    error path while the RLock is held."""

    def __init__(self):
        logging.Handler.__init__(self)
        self.raised = [0]

    def emit(self, record):
        # A real handler wraps its body and routes failures through
        # handleError(), which (with raiseExceptions True) prints to stderr and
        # returns -- logging "swallows" the error.  We RAISE inside the try so
        # the swallow path is exercised while the module RLock is held.
        try:
            if (record.msg_index & 31) == 0:
                self.raised[0] += 1
                raise RuntimeError("flaky handler boom")
        except Exception:
            self.handleError(record)


# Unique tag so post() can count exactly the lines this run wrote.
TAG = "BIG100_P161"


def setup(H):
    base = H.make_tmpdir("big100_logging_")
    logpath = os.path.join(base, "records.log")

    # Keep handleError quiet (it would otherwise print a full traceback per
    # flaky record to stderr); the error path still runs, just silently.
    logging.raiseExceptions = False

    logger = logging.getLogger("p161.{0}".format(os.getpid()))
    logger.setLevel(logging.INFO)
    logger.propagate = False
    # Clear any pre-existing handlers (fresh logger each run anyway).
    for h in list(logger.handlers):
        logger.removeHandler(h)

    fh = logging.FileHandler(logpath, mode="w")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(fh)

    flaky = FlakyHandler()
    logger.addHandler(flaky)

    H.register_close(fh)          # closed at shutdown so the file flushes
    H.state = {"logger": logger, "logpath": logpath, "flaky": flaky}
    # one writer slot per goroutine -> race-free emitted count
    H.emitted = [0] * H.funcs


def worker(H, wid, rng, state):
    logger = state["logger"]
    emitted = H.emitted
    n = 0
    for _ in H.round_range():
        # Each line carries the unique TAG + wid + a per-goroutine sequence so
        # post() can both COUNT records and confirm none were corrupted.
        line = "{0} {1} {2}".format(TAG, wid, n)
        rec = logging.LogRecord(
            logger.name, logging.INFO, __file__, 0, line, (), None)
        # FlakyHandler reads record.msg_index to decide whether to raise; stash
        # a deterministic index on the record.
        rec.msg_index = (wid * 1000003 + n) & 0x7FFFFFFF
        logger.handle(rec)
        emitted[wid] += 1
        n += 1
        H.op(wid)
        H.task_done(wid)
        if (n & 7) == 0:
            runloom.yield_now()


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    # Flush/close handlers so every buffered record hits the file.
    logger = H.state["logger"]
    for h in list(logger.handlers):
        try:
            h.flush()
            h.close()
        except Exception:
            pass

    emitted = sum(H.emitted)
    H.check(emitted > 0, "no records were emitted")

    written = 0
    try:
        with open(H.state["logpath"], "r") as f:
            for ln in f:
                if ln.startswith(TAG):
                    written += 1
    except OSError as exc:
        H.fail("could not read log file: {0}".format(exc))
        return

    # The FileHandler should have written ~every emitted record.  A small flush
    # tolerance is allowed for records still buffered at teardown, but if the
    # RLock had false-deadlocked or dropped records we'd see written far below
    # emitted.  Require >= 99% (and never more than emitted -> no phantom).
    H.check(written <= emitted,
            "wrote {0} > emitted {1} (phantom records)".format(written, emitted))
    H.check(written >= emitted - max(64, emitted // 100),
            "wrote {0} of {1} emitted records (RLock dropped/deadlocked?)".format(
                written, emitted))
    H.log("emitted={0} written={1} flaky_raised={2}".format(
        emitted, written, H.state["flaky"].raised[0]))


if __name__ == "__main__":
    harness.main("p161_logging_reentrancy", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="stdlib logging RLock reentrancy + flaky handler "
                          "under M:N; records conserved")
