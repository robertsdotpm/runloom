"""big_100 / 607 -- sysconfig.get_paths() single-owner path-expansion purity under M:N.

sysconfig is a PROCESS-GLOBAL module: get_config_vars() lazily initializes and
caches one module-level _CONFIG_VARS dict (under a _CONFIG_VARS_LOCK), and
_INSTALL_SCHEMES is a module-level constant mapping scheme name -> a dict of path
TEMPLATES like '{base}/lib/{implementation_lower}{py_version_short}{abi_thread}/
site-packages'.  Those globals are NOT single-owner, so the oracle is NOT built on
them.

WHAT IS single-owner: the dict RETURNED by sysconfig.get_paths(scheme, vars).  Read
_expand_vars(scheme, vars) (the callee): it allocates a FRESH `res = {}`, merges the
caller-supplied `vars` with the cached config vars via _extend_dict (which mutates
only the caller's OWN vars dict -- a fiber-local object), then for each template does
res[key] = os.path.normpath(_subst_vars(value, vars)) where _subst_vars is
value.format(**vars).  The result is a brand-new dict whose every value is a pure
string.format + normpath of (this fiber's vars) over (read-only module templates).
That returned dict, and this fiber's vars dict, are OWNED BY THE CALLING FIBER --
never shared.  So get_paths is a PURE function of (scheme, vars): identical inputs
must yield a BIT-IDENTICAL dict, and that dict must equal an INDEPENDENT closed-form
recomputation of the same templates over the same vars.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber picks vars whose
substitution values carry a UNIQUE per-fiber token (wid + idx).  It calls get_paths,
captures the returned dict, YIELDS so siblings run their own get_paths with DIFFERENT
tokens on the same/other hubs, then recomputes.  If the runtime tore the returned
dict, or a sibling's vars/expansion bled into this fiber's result (a cross-fiber leak
of a value the module built while another fiber was mid-expansion), this fiber's dict
would (a) differ across the yield, or (b) diverge from its own closed-form expected
(it would contain a SIBLING'S token, never this fiber's).  On a correct runtime the
single-owner returned dict is stable and matches the closed form exactly -- the
program exits 0.

WHY THE ORACLE IS LOAD-BEARING (verified reasoning, matches plain-thread semantics).
get_paths reads only read-only module constants (_INSTALL_SCHEMES) and the read-only
cached config vars, and writes only fiber-local objects (the caller's vars, the fresh
res dict).  A standalone plain-threads control (8 OS threads each calling get_paths
with its own unique-token vars, GIL on AND off) returns, for every thread, a dict that
equals that thread's own closed-form expansion -- 0 cross-thread bleed.  Under a
CORRECT runloom it must also hold.  A returned path that changes across a yield, or
that does not equal this fiber's independent recomputation (i.e. carries a sibling's
token), is a runloom single-owner-object corruption / cross-fiber leak.

ORACLES:
  * LOAD-BEARING -- PATH-EXPANSION PURITY (worker, HARD, fail-fast).  Per iteration a
    fiber:
      - builds vars_i, a fresh fiber-local dict whose values embed the unique token
        "W{wid}_I{idx}" (every {key} the scheme's templates reference is supplied, so
        the expansion is fully determined by vars_i -- config vars never fill a hole);
      - computes EXPECTED independently, by formatting a FROZEN private snapshot of the
        scheme templates over vars_i with the module's own merge/normpath order
        (closed form -- no call into get_paths);
      - calls sysconfig.get_paths(scheme, vars=dict(vars_i)) -> got1 (single-owner);
      - asserts got1 == EXPECTED and set(got1) == the scheme's path-name set;
      - YIELDS (yield_now + occasional tiny sleep) so siblings expand their own tokens;
      - calls get_paths again with a fresh identical vars -> got2, and asserts
        got2 == got1 == EXPECTED (bit-identical across the yield) and that EVERY value
        contains THIS fiber's token (no sibling token leaked in).
    Single-owner: vars_i and the returned dicts are fiber-local; a failure is a
    runloom returned-object corruption / cross-fiber expansion leak, never documented
    Python semantics.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (path_checks > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-expansion (e.g.
    parked inside _subst_vars/normpath on a desynced object) never returns; the
    watchdog + require_no_lost catch it.

FAIL ON: a returned path dict that differs across a yield, that diverges from this
fiber's independent closed-form expansion, that is missing/extra a path name, or a
value that does not carry this fiber's unique token (a cross-fiber leak).  There is no
shared-mutable arm: get_paths touches only read-only globals + fiber-local objects, so
there is no documented-shared-race to measure.

Stresses: sysconfig.get_paths / _expand_vars fresh-dict allocation + str.format
template substitution + os.path.normpath under M:N hub migration and yields; purity of
a process-global module's single-owner PRODUCT (the returned path dict) vs its
non-single-owner globals; cross-fiber leak of expansion state.
"""
import os

import harness
import runloom
import sysconfig

# The scheme we expand.  get_default_scheme() is 'posix_prefix' on Linux; on any
# platform its templates are a read-only constant we snapshot below.
SCHEME = sysconfig.get_default_scheme()

# FROZEN private snapshot of the scheme's path templates, taken once in the root
# (import time, single-threaded).  This is the INDEPENDENT closed-form basis: the
# oracle formats THIS snapshot itself rather than trusting get_paths' expansion, so
# a divergence localizes a corruption in the module's returned dict, not in the
# templates (which never change).  dict() copies the mapping; the template strings
# are immutable.
FROZEN_TEMPLATES = dict(sysconfig._INSTALL_SCHEMES[SCHEME])

# The exact set of path names the scheme must produce -- a closed universe.  A
# returned dict missing one of these, or carrying an extra key, is a torn result.
EXPECTED_KEYS = frozenset(FROZEN_TEMPLATES.keys())

# Every {key} the templates reference.  We supply ALL of them in each fiber's vars so
# the expansion is fully determined by fiber-local values (config vars never fill a
# hole, so the closed form is exact).  Derived once from the frozen snapshot.
TEMPLATE_KEYS = (
    "base", "installed_base", "installed_platbase", "platbase", "platlibdir",
    "implementation_lower", "py_version_short", "abiflags", "abi_thread",
)


def make_vars(wid, idx):
    """Build one fiber's UNIQUE-token vars dict.  Every value embeds "W{wid}_I{idx}"
    so this fiber's expanded paths are distinguishable from every sibling's; a sibling
    token appearing in this fiber's result is a cross-fiber leak.  All values are
    slash-free plain tokens (except the leading '/'), so normpath is a no-op and
    expanduser never fires (no '~'), keeping the closed form trivially exact."""
    tok = "W{0}_I{1}".format(wid, idx)
    return {
        "base": "/base_" + tok,
        "installed_base": "/ibase_" + tok,
        "installed_platbase": "/ipbase_" + tok,
        "platbase": "/pbase_" + tok,
        "platlibdir": "libdir_" + tok,
        "implementation_lower": "impl_" + tok,
        "py_version_short": "ver_" + tok,
        "abiflags": "abi_" + tok,
        "abi_thread": "thr_" + tok,
    }


def compute_expected(vars_local):
    """Independent closed-form expansion of the FROZEN templates over `vars_local`,
    replicating sysconfig._expand_vars' own order (expanduser on the template, then
    str.format substitution, then normpath) but WITHOUT calling into get_paths.  Since
    vars_local supplies every template key, the module's config-var merge never
    contributes, so this is bit-exact to a correct get_paths result.  Reads only the
    frozen snapshot + this fiber's own vars -- no shared mutable state."""
    res = {}
    posix_or_nt = os.name in ("posix", "nt")
    for key, template in FROZEN_TEMPLATES.items():
        value = os.path.expanduser(template) if posix_or_nt else template
        res[key] = os.path.normpath(value.format(**vars_local))
    return res


# Sustained checks per worker, bounded by H.running().  The single-owner-object
# hazard only manifests under SUSTAINED churn: many fibers simultaneously allocating
# fresh path dicts + formatting templates while PARKED across a yield, so the
# scheduler reliably interleaves a sibling's expansion before this fiber resumes.
INNER_CAP = 100000


def path_check(H, wid, idx, state):
    """Single-owner path-expansion purity check (fail-fast).

    Build fiber-local unique-token vars, expand via get_paths, and assert the returned
    single-owner dict is stable across a yield and equal to this fiber's independent
    closed-form expansion (never a sibling's token)."""
    vars_local = make_vars(wid, idx)
    tok = "W{0}_I{1}".format(wid, idx)
    expected = compute_expected(vars_local)

    # First expansion.  Pass a COPY of vars_local (get_paths' _extend_dict mutates the
    # dict it is handed -- keep our own pristine for the second call + token scan).
    got1 = sysconfig.get_paths(SCHEME, vars=dict(vars_local))

    if set(got1.keys()) != EXPECTED_KEYS:
        H.fail("get_paths returned WRONG key set: {0!r} != {1!r} (wid {2}, idx {3}) "
               "-- a torn/incomplete result dict under M:N".format(
                   sorted(got1.keys()), sorted(EXPECTED_KEYS), wid, idx))
        return
    if got1 != expected:
        H.fail("get_paths result diverged from closed-form expansion BEFORE yield "
               "(wid {0}, idx {1}): got {2!r} expected {3!r} -- a corrupted returned "
               "dict or a cross-fiber expansion leak".format(wid, idx, got1, expected))
        return

    # YIELD: let siblings run their own get_paths with DIFFERENT tokens.  If the
    # returned dict were shared/torn, a sibling's expansion would bleed in here.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Second expansion with a fresh identical vars: must be bit-identical to the first
    # and to the closed form.
    got2 = sysconfig.get_paths(SCHEME, vars=dict(vars_local))
    if got2 != got1:
        H.fail("get_paths result CHANGED across a yield (wid {0}, idx {1}): before "
               "{2!r} after {3!r} -- the single-owner returned dict was mutated by, or "
               "leaked into by, a concurrent sibling expansion".format(
                   wid, idx, got1, got2))
        return
    if got2 != expected:
        H.fail("get_paths result diverged from closed-form expansion AFTER yield "
               "(wid {0}, idx {1}): got {2!r} expected {3!r} -- cross-fiber leak".format(
                   wid, idx, got2, expected))
        return

    # Cross-fiber-leak tripwire: EVERY expanded value must carry THIS fiber's token.
    # A sibling token here would mean the module handed back another fiber's expansion.
    for key, val in got2.items():
        if tok not in val:
            H.fail("get_paths value for {0!r} == {1!r} is MISSING this fiber's token "
                   "{2!r} (wid {3}, idx {4}) -- a sibling's expansion leaked into this "
                   "fiber's single-owner result".format(key, val, tok, wid, idx))
            return

    # Closed-world tally: this check verified exactly len(EXPECTED_KEYS) path entries.
    state["path_checks"][wid] += 1
    state["entries"][wid] += len(got2)


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            path_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Warm the process-global config-var cache once in the root (single-threaded) so
    # its lazy _init_config_vars() lock dance is NOT part of the measured oracle --
    # the oracle is about the single-owner RETURNED dict, not the module's own init.
    sysconfig.get_config_vars()
    # Race-free CONSERVATION/non-vacuity counters: ONE slot per worker (single writer),
    # allocated here where H.funcs is known.  NEVER wid & MASK (would alias GIL-off).
    H.state = {
        "path_checks": [0] * H.funcs,   # completed single-owner purity checks
        "entries": [0] * H.funcs,       # total path entries verified (len * checks)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["path_checks"])
    entries = sum(H.state["entries"])
    H.log("sysconfig[single-owner LOAD-BEARING]: {0} get_paths purity checks "
          "({1} path entries verified, all matched the closed-form expansion "
          "fail-fast); scheme={2}; ops={3}".format(
              checks, entries, SCHEME, H.total_ops()))

    # Self-consistency of the closed-world tally: every check verified exactly
    # len(EXPECTED_KEYS) entries, so entries must equal checks * len(EXPECTED_KEYS).
    H.check(entries == checks * len(EXPECTED_KEYS),
            "entry tally inconsistent: {0} entries != {1} checks * {2} keys -- a "
            "per-slot counter lost/gained a write".format(
                entries, checks, len(EXPECTED_KEYS)))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(checks > 0,
            "no get_paths purity checks ran -- the single-owner path-expansion hazard "
            "was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-expansion.
    H.require_no_lost("sysconfig get_paths purity")


if __name__ == "__main__":
    harness.main(
        "p607_sysconfig_paths_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="sysconfig.get_paths(scheme, vars) is a PURE function of its inputs: "
                 "it allocates a FRESH dict and str.format-expands read-only scheme "
                 "templates over the caller's fiber-local vars.  LOAD-BEARING: each "
                 "fiber expands vars carrying a UNIQUE per-fiber token, captures the "
                 "single-owner returned dict, yields so siblings expand different "
                 "tokens, then re-expands and asserts the dict is bit-identical across "
                 "the yield and equal to an independent closed-form recomputation "
                 "(every value must carry THIS fiber's token, never a sibling's).  A "
                 "dict that changes across the yield, diverges from the closed form, "
                 "or carries a leaked sibling token is the runloom single-owner-object "
                 "/ cross-fiber-leak bug.  The module's process-global config-var cache "
                 "+ scheme templates are read-only, so there is no shared-mutable arm")
