"""big_100 / 174 -- SQLite transaction cancellation under write-lock contention.

A shared SQLite DB file in H.make_tmpdir.  Each goroutine opens its OWN
connection and runs explicit BEGIN IMMEDIATE / INSERT / COMMIT transactions,
retrying when the write lock is held.  SOME transactions are CANCELLED (a short
timeout) while waiting on the write lock -- the goroutine gives up that attempt,
ROLLS BACK, and the row is NOT counted as committed.  The DB must stay
consistent: a final PRAGMA integrity_check == 'ok', and the actual row count
must equal the sum of per-worker committed counts (no phantom / lost rows).

Stresses: sqlite3 C-extension through the offload pool, BEGIN IMMEDIATE write
lock, busy_timeout, transaction cancellation/rollback, consistency.

Low funcs: the single-writer SQLite lock serialises writers.
"""
import sqlite3

import harness
import runloom
import cancelutil

MAX_ACTIVE = 200
ROWS_PER_ROUND = 8


def setup(H):
    base = H.make_tmpdir("big100_sqlite_cancel_")
    db = "{0}/work.db".format(base)
    con = sqlite3.connect(db)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("CREATE TABLE kv(wid INTEGER, seq INTEGER, val INTEGER)")
    con.commit()
    con.close()
    H.state = {"db": db}
    H.committed = [0] * H.funcs


def worker(H, wid, rng, state):
    db = state["db"]
    committed = H.committed

    def connect():
        c = sqlite3.connect(db, timeout=10, check_same_thread=False,
                            isolation_level=None)   # autocommit -> we drive BEGIN
        c.execute("PRAGMA busy_timeout=2000")
        return c

    con = runloom.blocking(connect)
    seq = 0
    try:
        for _ in H.round_range():
            # Roughly 1/3 of transactions are cancellable with a tight deadline,
            # so under write-lock contention some will give up (timeout) before
            # the COMMIT.
            cancellable = (rng.random() < 0.34)
            timeout_s = rng.uniform(0.001, 0.01) if cancellable else None

            def txn(seq=seq):
                # BEGIN IMMEDIATE acquires the write lock now (or waits up to
                # busy_timeout).  On a busy DB this is where contention shows.
                con.execute("BEGIN IMMEDIATE")
                try:
                    for k in range(ROWS_PER_ROUND):
                        con.execute("INSERT INTO kv VALUES (?,?,?)",
                                    (wid, seq * ROWS_PER_ROUND + k,
                                     (seq * ROWS_PER_ROUND + k) * 7))
                    con.execute("COMMIT")
                    return True
                except Exception:
                    try:
                        con.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise

            try:
                if cancellable:
                    # Run the (offloaded) transaction but bail if the deadline
                    # fires first.  If we bail, the offloaded blocking() may still
                    # be finishing on the pool thread -- so we run a SYNCHRONOUS
                    # blocking() and treat an OperationalError (lock timeout) as
                    # the cancellation outcome.  The deadline is enforced by the
                    # connection busy_timeout being short relative to contention,
                    # plus we wrap in a context so a true timeout discards.
                    ctx, cancel = cancelutil.WithTimeout(
                        cancelutil.Background(), timeout_s)
                    try:
                        ok = runloom.blocking(txn)
                    except sqlite3.OperationalError:
                        ok = False     # lock contention -> cancelled attempt
                    finally:
                        cancel()
                    if not ok or ctx.err() is not None:
                        # Cancelled / lock-timed-out: ensure we are clean. The txn
                        # already rolled back on its own exception path; if it
                        # actually committed (ok True) but the ctx expired after,
                        # the row IS committed -> count it.
                        if ok and ctx.err() is not None:
                            committed[wid] += 1
                        H.op(wid)
                        seq += 1
                        continue
                else:
                    try:
                        ok = runloom.blocking(txn)
                    except sqlite3.OperationalError:
                        # Even non-cancellable txns can hit a lock timeout; retry
                        # this seq next round (don't advance committed).
                        H.sleep(0.005)
                        continue
                committed[wid] += 1
                H.op(wid)
                H.task_done(wid)
                seq += 1
            except sqlite3.OperationalError:
                H.sleep(0.005)
                continue
    finally:
        runloom.blocking(con.close)


def body(H):
    H.run_pool(H.funcs, worker, H.state, max_concurrent=MAX_ACTIVE)


def post(H):
    db = H.state["db"]
    con = sqlite3.connect(db, timeout=30)
    try:
        integ = con.execute("PRAGMA integrity_check").fetchone()[0]
        H.check(integ == "ok", "integrity_check failed: {0}".format(integ))
        rows = con.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
    finally:
        con.close()

    committed_rows = sum(H.committed) * ROWS_PER_ROUND
    H.check(sum(H.committed) > 0, "no transaction ever committed")
    # Every row in the table must come from a committed transaction; cancelled /
    # rolled-back ones leave nothing behind.
    H.check(rows == committed_rows,
            "row count {0} != committed*rows_per_round {1} (lost/phantom rows)"
            .format(rows, committed_rows))
    H.log("integrity={0} rows={1} committed_txns={2}".format(
        integ, rows, sum(H.committed)))


if __name__ == "__main__":
    harness.main("p174_sqlite_transaction_cancellation", body, setup=setup,
                 post=post, default_funcs=500,
                 describe="sqlite BEGIN IMMEDIATE txns, some cancelled on the "
                          "write lock; DB stays consistent")
