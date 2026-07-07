"""big_100 / 522 -- netrc per-file hosts-dict isolation under M:N.

netrc.netrc(path) parses a .netrc file with a hand-rolled shlex-style lexer
(_netrclex): a per-instance object holding a `pushback` list, a running `lineno`,
and an `instream` file object it pulls one character at a time via read(1).  The
parser walks the token stream ("machine NAME login U account A password P ...")
and fills a per-instance `self.hosts` dict mapping machine-name -> (login,
account, password).  Every piece of that state -- the lexer's pushback deque, the
character cursor, the accumulating hosts dict -- lives on freshly-created objects
owned by the ONE netrc() call that built them.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each read(1) on the
underlying file goes through the monkey-patched file layer, which offloads the
blocking read and PARKS the fiber -- a natural preemption point in the MIDDLE of a
parse.  When the fiber resumes it may land on a different hub while a SIBLING
fiber is driving its OWN netrc() parse of its OWN file on that hub.  If the lexer
instance, its pushback list, or the hosts dict were somehow not fiber-local -- if
a sibling's token stream bled into this parse's pushback, or a sibling's
machine/login/password landed in this fiber's hosts dict -- then this fiber's
hosts mapping would carry an entry it never wrote (a cross-file host leak) or a
value from the wrong file.  Under a CORRECT M:N runtime every netrc() call's
lexer + hosts dict is single-owner and the parse is deterministic, so the
recovered hosts must be EXACTLY what this fiber wrote to its own file.

WHICH ORACLE IS LOAD-BEARING, AND WHY.

  * LOAD-BEARING -- PER-FILE HOSTS ISOLATION (worker, HARD, fail-fast).  Each
    fiber owns a private tmp file to which it writes a KNOWN set of machine
    entries, every field wid-embedded (host_W{wid}_M{m}, user_W{wid}_M{m},
    acct_W{wid}_M{m}, and a SPACE-containing quoted password "pw W{wid} M{m}" so
    the lexer's quoted-token branch runs).  The fiber then:
      - netrc.netrc(own_path) -> nrc0, forcing the full lexer parse.
      - Asserts set(nrc0.hosts) == the exact machine-name set it wrote, and for
        every machine nrc0.hosts[host] == the exact (login, account, password)
        3-tuple it wrote, and authenticators(host) agrees.
      - Asserts EVERY recovered host name carries THIS fiber's wid marker
        ("W{wid}_") -- a host tagged with a different wid is a sibling's entry
        that leaked across files (the isolation bug), a hard fault.
      - Yields (runloom.yield_now) so siblings parse their own files on this hub.
      - Re-parses the SAME file -> nrc1 and asserts nrc1.hosts == nrc0.hosts
        byte-for-byte: the mapping is STABLE across the park (no field mutated,
        no host gained or lost).
    Single-owner: the file, the netrc objects, and both hosts dicts are all
    fiber-local, created and read by exactly one fiber.  A cross-file host, a
    wrong field value, or an unstable re-parse is a runloom isolation bug -- and
    on a correct runtime this oracle PASSES (the program exits 0 when there is
    no bug).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-parse
    (parked inside a read(1) whose wake was lost) never returns; the watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (netrc_checks>0).

FAIL ON: a recovered host carrying a sibling's wid, a (login, account, password)
tuple that differs from what this fiber wrote, a hosts key-set mismatch, an
unstable re-parse across the yield, or authenticators() disagreeing with the
hosts dict.  There is NO shared object here (each file + netrc is single-owner),
so any mismatch is a genuine cross-fiber leak of parser/hosts state, never
documented shared-container semantics.

Resource note: netrc is FILE-BACKED (one small tmp file per fiber under a shared
make_tmpdir), so max_funcs caps the pool (default 256) -- the forever loop's
--funcs 1000000 is clamped and per-fiber file churn stays bounded.

Stresses: netrc._netrclex hand-rolled lexer (pushback deque + read(1) char
cursor + lineno) across the monkey-offloaded file-read park, per-instance
hosts-dict fill under M:N hub migration, quoted-token parse branch, and per-file
parse isolation vs cross-file host leakage.
"""
import os

import netrc

import harness
import runloom

# Machine entries written per fiber.  A handful is enough to force multiple
# top-level "machine" keywords + follower tokens through the lexer while keeping
# each file tiny (file-backed -> bounded pool).
NMACHINES = 3


def build_netrc_text(wid):
    """Build this fiber's private .netrc text plus the EXACT expected hosts map.

    Every field carries the wid marker "W{wid}_" so a leaked sibling entry is
    detectable, and the password is a SPACE-containing quoted token so the
    lexer's quoted-string branch (the `if ch == '"'` path in _netrclex) runs.

    Returns (text, expected) where expected maps host -> (login, account,
    password) exactly as netrc.hosts[host] must come back."""
    parts = []
    expected = {}
    for m in range(NMACHINES):
        host = "host_W{0}_M{1}".format(wid, m)
        login = "user_W{0}_M{1}".format(wid, m)
        account = "acct_W{0}_M{1}".format(wid, m)
        # A space inside the password forces the quoted-token lexer path; netrc
        # stores hosts[host] == (login, account, password) with the quotes
        # stripped and the interior space preserved.
        password = "pw W{0} M{1}".format(wid, m)
        expected[host] = (login, account, password)
        parts.append(
            'machine {0}\n\tlogin {1}\n\taccount {2}\n\tpassword "{3}"\n'.format(
                host, login, account, password))
    return "".join(parts), expected


def netrc_check(H, wid, path, state):
    """Single-owner per-file hosts-isolation check (fail-fast).

    Writes this fiber's private .netrc, parses it, verifies the recovered hosts
    dict is EXACTLY what it wrote (with no sibling's wid leaking in), yields, then
    re-parses and asserts the mapping is stable across the park."""
    text, expected = build_netrc_text(wid)
    marker = "W{0}_".format(wid)

    # Write this fiber's OWN file (single-owner; siblings never touch it).
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

    # ---- parse #1: force the full lexer walk into a fresh hosts dict --------
    nrc0 = netrc.netrc(path)
    hosts0 = nrc0.hosts

    # Key-set must be EXACTLY the machines this fiber wrote.
    if set(hosts0.keys()) != set(expected.keys()):
        H.fail("netrc hosts key-set mismatch (wid {0}): parsed {1!r} but wrote "
               "{2!r} -- a machine entry was lost, doubled, or a sibling's host "
               "leaked into this fiber's hosts dict".format(
                   wid, sorted(hosts0.keys()), sorted(expected.keys())))
        return

    for host, want in expected.items():
        # Cross-file leak guard: every recovered host MUST carry this fiber's wid.
        if marker not in host:
            H.fail("netrc host {0!r} does not carry this fiber's marker {1!r} "
                   "(wid {2}) -- a sibling's machine entry leaked across files "
                   "into this fiber's hosts dict".format(host, marker, wid))
            return

        got = hosts0.get(host)
        if got != want:
            H.fail("netrc field mismatch for host {0!r} (wid {1}): parsed {2!r} "
                   "but wrote {3!r} -- a (login,account,password) field was torn "
                   "or crossed with a sibling parse".format(host, wid, got, want))
            return

        # authenticators() must agree with the hosts dict.
        auth = nrc0.authenticators(host)
        if auth != want:
            H.fail("netrc authenticators({0!r}) == {1!r} != hosts entry {2!r} "
                   "(wid {3}) -- lookup disagrees with the parsed mapping".format(
                       host, auth, want, wid))
            return

    # YIELD: park here so a sibling drives its OWN netrc parse on this hub before
    # we re-read.  If parser/hosts state were not fiber-local, a sibling's tokens
    # could bleed into this fiber's mapping across the migration.
    runloom.yield_now()

    # ---- parse #2: the mapping must be identical across the park -------------
    nrc1 = netrc.netrc(path)
    hosts1 = nrc1.hosts
    if hosts1 != hosts0:
        H.fail("netrc RE-PARSE unstable across a yield (wid {0}): first parse "
               "{1!r}, second parse {2!r} -- the file is single-owner and "
               "unchanged, so a differing re-parse is a cross-fiber parser-state "
               "leak".format(wid, hosts0, hosts1))
        return

    state["netrc_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber owns one private .netrc path under the shared tmpdir and runs
    the single-owner hosts-isolation check every round."""
    path = os.path.join(state["base"], "nrc_{0}".format(wid))
    for _ in H.round_range():
        if not H.running():
            break
        netrc_check(H, wid, path, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    base = H.make_tmpdir("big100_netrc_")
    H.state = {
        "base": base,
        "netrc_checks": [0] * 1024,   # non-vacuity tally (sharded; single-owner check)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["netrc_checks"])
    H.log("netrc[single-owner per-file LOAD-BEARING]: {0} hosts-isolation checks "
          "(all passed fail-fast); ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the load-bearing per-file parse/verify arm actually ran.
    H.check(checks > 0,
            "no netrc per-file hosts-isolation checks ran -- the load-bearing "
            "parser-isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a
    # monkey-offloaded read(1) mid-parse with a lost wake).
    H.require_no_lost("netrc per-file hosts isolation")


if __name__ == "__main__":
    harness.main(
        "p522_netrc_perfile_hosts_isolation", body, setup=setup, post=post,
        default_funcs=256, max_funcs=256,
        describe="netrc.netrc(path) parses a .netrc via a hand-rolled shlex-style "
                 "lexer (_netrclex: pushback deque + read(1) char cursor) into a "
                 "per-instance hosts dict.  Each read(1) parks through the "
                 "monkey-offloaded file layer -- a preemption point mid-parse.  "
                 "LOAD-BEARING: each fiber writes its OWN wid-tagged .netrc, parses "
                 "it, and asserts the recovered hosts map is EXACTLY what it wrote "
                 "with every host carrying this fiber's wid marker (no cross-file "
                 "leak) and stable across a re-parse taken after a yield.  A host "
                 "tagged with a sibling's wid, a torn field, or an unstable "
                 "re-parse is the runloom parser-isolation bug")
