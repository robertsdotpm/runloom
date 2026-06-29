"""big_100 / 487 -- mailbox.mbox per-instance message cache isolation under M:N.

mailbox.mbox stores messages in a file and caches them in the _cache attribute.
When a fiber reads messages from a mailbox, it populates _cache. Under M:N,
multiple fibers on the same hub thread could share mailbox instances if there is
a cross-fiber leak in mailbox state, and a sibling's _cache updates would corrupt
the message set.

WHERE M:N BREAKS IT (the gap this program probes).  The LOAD-BEARING hazard is:
each fiber creates its OWN distinct mailbox (temp file), adds unique messages,
reads them back across yields, and asserts they are EXACTLY its own messages.  If
mailbox._cache were shared or leaked across instances, a fiber's get_message()
could return messages from a SIBLING's mailbox or the _cache could be
cross-polluted -- a corruptionof message isolation.

This program isolates the hazard:
  LOAD-BEARING arm: each fiber creates DISTINCT temp mbox files, adds unique
    messages, reads them back across yields, asserts messages are correct
    (wrong _cache => wrong mail, cross-fiber message leak => FAIL).
  MEASURED arm: if a fiber's mbox contains a message it never added
    (impossible under correct isolation), report the leak.

The mailbox.mbox _cache dict should be PER-INSTANCE, not shared across fibers.
A cross-mailbox message leak is the M:N cache-isolation bug.

Stresses: mailbox.mbox._cache per-instance isolation, temp file creation/cleanup,
message add/get across yields, module-global state leaks.

Good TSan / controlled-M:N-replay target: mailbox._cache is a plain dict mutated
by add() and get_message(); a data race on _cache  or cross-mailbox entry would
surface before the oracle fires.
"""
import mailbox
import tempfile
import os
import email

import harness
import runloom

# Message pool (deterministic per fiber + iteration)
MESSAGE_TEXTS = [
    "msg_type_alpha",
    "msg_type_beta",
    "msg_type_gamma",
    "msg_type_delta",
]


def setup(H):
    H.state = {
        # LOAD-BEARING arm: message integrity checks per fiber's own mailbox
        "lb_checks": [0] * 1024,        # fibers that read from own mailbox
        "lb_wrong_msg": [0] * 1024,     # read wrong message payload
        "lb_wrong_count": [0] * 1024,   # expected N messages, got != N
        # MEASURED arm: spurious messages in mailbox (must stay 0)
        "leak_checks": [0] * 1024,      # times we checked for leaks
        "leak_detections": [0] * 1024,  # detected a spurious message
        # Sample of first failure
        "sample": [None],
    }


def make_mbox_id(wid, iteration):
    """A unique mailbox "identity" per fiber + iteration."""
    return "{0:05d}_{1:06d}".format(wid, iteration)


def get_message_text(mbox_id, seq):
    """Deterministic message text for a message index in a mailbox."""
    h = (hash(mbox_id) + seq) & 0xFFFFFFFF
    idx = h % len(MESSAGE_TEXTS)
    return MESSAGE_TEXTS[idx]


# --------------------------------------------------------------------------
# LOAD-BEARING arm: each fiber creates DISTINCT temp mailbox, adds unique
# messages, reads them back across yields, asserts they are ITS OWN.
# --------------------------------------------------------------------------
def lb_check(H, wid, iteration, state):
    """The load-bearing check: fiber's own mailbox must return fiber's own messages."""
    mbox_id = make_mbox_id(wid, iteration)
    tmpdir = tempfile.mkdtemp(prefix="mbox_" + mbox_id + "_")
    try:
        # Create a DISTINCT mailbox for this fiber
        mbox_path = os.path.join(tmpdir, "INBOX.mbox")
        mbox = mailbox.mbox(mbox_path)

        # Add N_MESSAGES unique messages to THIS mailbox only
        N_MESSAGES = 4
        expected_payloads = []
        for seq in range(N_MESSAGES):
            text = get_message_text(mbox_id, seq)
            # Create an email.message.Message object
            msg = email.message_from_string(text)
            msg['Subject'] = '{0}_msg_{1}'.format(mbox_id, seq)
            msg['From'] = 'fiber_{0}'.format(wid)
            # Add to mailbox (returns the new key, an int in mbox format)
            key = mbox.add(msg)
            expected_payloads.append((key, text))

        # YIELD + SLEEP: the mailbox._cache is now populated with this fiber's
        # messages. A sibling on the same hub might be mid-operation on its own
        # mailbox. If the _cache were shared across instances (a runloom M:N
        # isolation bug), a sibling's get_message() could hit THIS fiber's
        # cache, or THIS fiber's next read could hit the SIBLING's cache.
        runloom.yield_now()
        if iteration & 1:
            runloom.sleep(0.0002)

        # Read messages back and verify they match what WE added
        # In mbox format, keys are sequential integers 0, 1, 2, ...
        messages_read = []
        for i in range(len(expected_payloads)):
            try:
                msg = mbox[i]
                # Extract the message payload
                payload = msg.get_payload() if hasattr(msg, 'get_payload') else str(msg)
                messages_read.append((i, payload))
            except (KeyError, TypeError, mailbox.NoSuchMailboxError) as e:
                # Message should be there; if not, it's a structural issue
                state["lb_wrong_count"][wid & 1023] += 1
                if state["sample"][0] is None:
                    state["sample"][0] = (wid, mbox_id, "missing key", i)
                H.fail("mailbox INTEGRITY: fiber {0} (mbox {1}) added message at "
                       "index {2} but cannot read it back -- possibly corrupted by a "
                       "sibling's _cache operation across mailbox instances: {3}".format(
                           wid, mbox_id, i, e))
                return

        # Verify the messages we got are the ones WE added (not a sibling's)
        # Check that we got the expected number of messages
        if len(messages_read) != len(expected_payloads):
            state["lb_wrong_count"][wid & 1023] += 1
            if state["sample"][0] is None:
                state["sample"][0] = (wid, mbox_id, len(expected_payloads),
                                      len(messages_read))
            H.fail("mailbox COUNT MISMATCH: fiber {0} (mbox {1}) expected {2} "
                   "messages but read {3} -- possible _cache corruption from a "
                   "sibling across mailbox instances".format(
                       wid, mbox_id, len(expected_payloads), len(messages_read)))
            return

        # Verify each message payload matches what we added
        for i, (expected_key, expected_text) in enumerate(expected_payloads):
            _, got_text = messages_read[i]
            # Strip trailing whitespace (email.message_from_string adds newlines)
            got_text_stripped = got_text.strip()
            if expected_text != got_text_stripped:
                # The message text doesn't match -- wrong message or _cache leak
                state["lb_wrong_msg"][wid & 1023] += 1
                if state["sample"][0] is None:
                    state["sample"][0] = (wid, mbox_id, expected_text, got_text_stripped)
                H.fail("mailbox MESSAGE CORRUPTION: fiber {0} (mbox {1}) expected "
                       "message payload '{2}' but got '{3}' at index {4} -- a "
                       "sibling's message leaked into this mailbox's _cache or this "
                       "mailbox's cache was cross-polluted (the M:N cache-isolation "
                       "bug)".format(wid, mbox_id, expected_text, got_text, i))
                return

        state["lb_checks"][wid & 1023] += 1
        mbox.close()
    finally:
        # Clean up temp mailbox files
        try:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass


# --------------------------------------------------------------------------
# MEASURED arm: detect any spurious messages in mailbox (must stay 0).
# This arm is a safety net: if a fiber's mailbox somehow contains a message
# it never added, we detect and report it (but don't fail).
# --------------------------------------------------------------------------
def leak_check(H, wid, iteration, state):
    """Probe for cross-mailbox message leaks (report only, never fail)."""
    mbox_id = make_mbox_id(wid, iteration)
    tmpdir = tempfile.mkdtemp(prefix="mbox_" + mbox_id + "_")
    try:
        mbox_path = os.path.join(tmpdir, "INBOX.mbox")
        mbox = mailbox.mbox(mbox_path)

        # Add exactly ONE message
        probe_text = "unique_probe_text_{0}".format(wid)
        msg = email.message_from_string(probe_text)
        msg['Subject'] = "leak_probe_{0}_{1}".format(wid, iteration)
        msg['From'] = 'fiber_{0}'.format(wid)
        probe_key = mbox.add(msg)

        # Sleep to let siblings run
        runloom.yield_now()

        # Check that the mailbox still contains only OUR one message
        keys = list(mbox.keys())
        if len(keys) != 1 or keys[0] != probe_key:
            # Extra messages in the mailbox -> _cache cross-pollution from a sibling
            state["leak_detections"][wid & 1023] += 1
            if state["sample"][0] is None:
                state["sample"][0] = (wid, mbox_id, "spurious keys", str(keys))
            # MEASURED: report but never fail (this is a safety net, not the oracle)

        state["leak_checks"][wid & 1023] += 1
        mbox.close()
    finally:
        try:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass


# Sustained checks per worker, bounded by H.running().  The _cache isolation
# hazard only manifests under sustained churn -- many fibers simultaneously
# mid-read and yielded, so the scheduler reliably runs a sibling mid-operation
# on a different mailbox before this fiber resumes.  A single check per fiber
# barely overlaps a sibling's.  So each worker runs a sustained internal loop
# (one LOAD-BEARING check + one MEASURED leak check per iteration) until
# H.running() or INNER_CAP.  Bounding by H.running() makes the oracle fire
# at the DEFAULT --rounds 1 (it does not depend on a large --rounds);
# INNER_CAP stops one worker from monopolizing teardown if the box is slow.
INNER_CAP = 10000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per iteration: the LOAD-BEARING mailbox message
    check (fail-fast on isolation breach) and the MEASURED leak detector
    (report-only, must stay 0%).  The two do not interact -- each creates its own
    temp mailbox -- so running them in the same fiber keeps the hub busy with
    mixed message churn without one reaching the other's oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            lb_check(H, wid, idx, state)     # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            leak_check(H, wid, idx, state)   # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    lb = sum(H.state["lb_checks"])
    wrong_msg = sum(H.state["lb_wrong_msg"])
    wrong_cnt = sum(H.state["lb_wrong_count"])
    leaks = sum(H.state["leak_detections"])
    lchecks = sum(H.state["leak_checks"])
    lpct = (100.0 * leaks / lchecks) if lchecks else 0.0
    sample = H.state["sample"][0]

    H.log("mailbox: LOAD-BEARING checks={0} (all passed fail-fast) | wrong_msg={1} "
          "wrong_count={2} | MEASURED leak_checks={3} leaks={4} ({5:.2f}%, must "
          "stay 0%) | sample={6}".format(
              lb, wrong_msg, wrong_cnt, lchecks, leaks, lpct, sample))

    if leaks:
        H.log("note: the MEASURED leak detector observed {0} spurious messages "
              "in mailbox instances across {1} checks -- unexpected (should stay "
              "0%); suggests mailbox._cache is shared or corrupted across fibers".format(
                  leaks, lchecks))

    # NON-VACUITY: the load-bearing mailbox isolation hazard was actually exercised
    H.check(lb > 0,
            "no mailbox integrity checks ran -- the load-bearing _cache-isolation "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-mailbox operation
    H.require_no_lost("mailbox.mbox per-instance cache isolation")


if __name__ == "__main__":
    harness.main("p487_mailbox", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="mailbox.mbox._cache is a per-instance dict that "
                          "caches messages.  Under M:N, fibers on the same hub "
                          "create DISTINCT mailbox instances, but if the _cache "
                          "or mailbox state were somehow shared (a module-global "
                          "leak or cross-fiber registry bug), a fiber's "
                          "get_message() could return messages from a SIBLING's "
                          "mailbox -- a cross-fiber message leak.  LOAD-BEARING: "
                          "each fiber creates its own temp mbox file, adds unique "
                          "messages, reads across yields, asserts returned messages "
                          "are ITS OWN (0 under plain threads GIL on AND off; "
                          "cross-mailbox leak is the runloom M:N bug).  MEASURED "
                          "leak detector stays 0%")
