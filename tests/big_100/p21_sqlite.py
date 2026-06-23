"""big_100 / 21 -- SQLite concurrent workload.

Many goroutines, each with its own SQLite connection (offloaded to the
blocking-worker pool via runloom.blocking, since sqlite3 is a GIL-releasing C
extension that would otherwise wedge a hub), insert/select/delete against one
shared WAL database, retrying on "database is locked".  Each goroutine works
only on its own rows so the result is deterministic and checkable.

Stresses: C-extension blocking through the offload pool, database locks,
busy-timeout retries.

SCALE NOTE: SQLite WAL supports at most ~2000 concurrent writers cleanly before
lock contention and connection-create time make drain time explode.
run_pool(max_concurrent=MAX_ACTIVE) keeps exactly MAX_ACTIVE goroutines alive
(no CoSemaphore needed, which would create one pipe-pair per waiting goroutine
and blow the FD limit at 1M funcs).

MAX_FUNCS cap (sqlite library ceiling, NOT a runloom limit): each worker opens
and CLOSES its own connection, so total funcs == total connection lifecycles.
At ~1M close churn, macOS's libsqlite3 flakily SIGSEGVs INSIDE sqlite3Close
(backtrace: sqlite3Close <- pysqlite_connection_close <- the blocking-pool
worker; fault 0x1).  The runloom blocking-pool handoff is provably ordered
(done release/acquire + bp_lock chain the connection's create->step->close across
worker threads, and the fiber keeps the connection PyObject referenced
throughout) -- the fault is in libsqlite3's own close under extreme WAL
connection churn, which the test must not drive past the library's ceiling.
CONFIRMED by experiment: an own-db-per-goroutine variant (no shared WAL -shm)
did identical churn (same ops/s, same 2000-concurrent close burst) yet crashed
0 times in 12x1M runs, vs ~1/6 for this shared-WAL version -- so the trigger is
sqlite's SHARED-WAL concurrent close, not the runloom blocking pool.
Cap total connection lifecycles at the level the prior full 100k sweep ran
cleanly.  (Confine-to-one-thread would also avoid it but the blocking pool has
no thread-affinity, and a thread-per-connection pool can't scale to MAX_ACTIVE.)
"""
import sqlite3

import harness
import runloom

CYCLE = 64        # rows inserted before a worker deletes its set and restarts
MAX_ACTIVE = 2000


def setup(H):
    base = H.make_tmpdir("big100_sqlite_")
    db = "{0}/work.db".format(base)
    con = sqlite3.connect(db)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("CREATE TABLE kv(wid INTEGER, seq INTEGER, val INTEGER)")
    con.execute("CREATE INDEX kv_wid ON kv(wid)")
    con.commit()
    con.close()
    H.state = {"db": db}


def worker(H, wid, rng, state):
    db = state["db"]

    def connect():
        c = sqlite3.connect(db, timeout=30, check_same_thread=False)
        c.execute("PRAGMA busy_timeout=30000")
        return c

    con = runloom.blocking(connect)
    try:
        seq = 0
        while H.running():
            def step(seq=seq):
                con.execute("INSERT INTO kv VALUES (?,?,?)",
                            (wid, seq, seq * 7))
                con.commit()
                row = con.execute(
                    "SELECT COUNT(*), COALESCE(SUM(val),0) FROM kv WHERE wid=?",
                    (wid,)).fetchone()
                return row

            try:
                cnt, total = runloom.blocking(step)
            except sqlite3.OperationalError:
                H.sleep(0.005)              # locked despite busy_timeout: retry
                continue
            n = seq + 1
            expected = 7 * (n - 1) * n // 2
            if not H.check(cnt == n and total == expected,
                           "sqlite inconsistency wid={0}: cnt={1} n={2} "
                           "sum={3} exp={4}".format(wid, cnt, n, total, expected)):
                return
            H.op(wid)
            seq += 1
            if seq >= CYCLE:
                try:
                    runloom.blocking(lambda: (con.execute(
                        "DELETE FROM kv WHERE wid=?", (wid,)), con.commit()))
                    seq = 0
                    H.task_done(wid)
                except sqlite3.OperationalError:
                    H.sleep(0.005)
    finally:
        runloom.blocking(con.close)


def body(H):
    H.run_pool(H.funcs, worker, H.state, max_concurrent=MAX_ACTIVE)


if __name__ == "__main__":
    harness.main("p21_sqlite", body, setup=setup, default_funcs=2000,
                 max_funcs=100000,   # sqlite3Close churn ceiling -- see SCALE NOTE
                 describe="concurrent SQLite WAL read/write with lock retries")
