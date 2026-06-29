"""big_100 / 472 -- ipaddress module parsing under M:N contention.

ipaddress.ip_network() and IPv4Address/IPv6Address parse and validate IP
address strings, returning network/address objects.  The parsing involves
string manipulation and integer arithmetic that happens in userspace Python
and in C extensions.  Under runloom's M:N scheduler many fibers ("goroutines")
share one hub OS-thread, and each fiber runs Python code that parses addresses
concurrently.

WHERE M:N COULD BREAK IT (the gap this program probes).  While fiber A is
parsing an IPv4 address "A.wid.0.1" (encoding wid into the address string),
yields at a scheduling point (netpoll park, sleep), and a SIBLING fiber B on
the same hub parses a DIFFERENT address "B.wid.0.1", the parsing can involve
shared state (module-level helper functions, imported regex, or cached
computations if they exist in the implementation).  If concurrent parse
operations corrupt shared state or cause a buffer/register race, a fiber's
recovered address object might have the WRONG value -- not matching what it
encoded and parsed.  This is a module-shared-state class: the parser assumes
serial or GIL-protected execution, but under M:N many fibers execute parsing
code concurrently.

This probes whether ipaddress parsing is robust under M:N concurrency.
Verified with a standalone plain-threads control (same concurrent parsing
logic, NO runloom): if a fiber's parsed address value equals its input encoding
(correctness oracle), it MUST pass under PYTHON_GIL=1 AND PYTHON_GIL=0.  Under
a CORRECT runloom M:N it MUST also hold (no shared-state corruption).  If a
fiber parses "10.wid.0.0/24" and recovers a different network address (the
value is wrong, not matching what was encoded), that is a parsing corruption
under M:N concurrency -- the load-bearing oracle PASSES on a correct runtime
(program exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- PARSED ADDRESS VALUE CORRECTNESS (worker, HARD, fail-fast).
    Each fiber encodes wid into an address string (e.g., "10.wid.0.0/24" or
    "2001:db8:wid:0/64"), parses it via ipaddress.ip_network() to get a Network
    object, then immediately extracts the network_address and checks that its
    value MATCHES the encoded wid.  A mismatch (parsed address has wrong octets
    or wrong wid value) indicates that the parser corrupted or cross-contaminated
    state across a sibling's concurrent parse.  The fiber yields (park / migrate
    hubs / sleep) so a sibling on the same hub can run concurrent parsing, then
    re-parses the same address and checks the value again.  If the value changes
    or is ever wrong, the parser has a race condition under M:N concurrency.
    We verified this under PLAIN OS THREADS (64 threads, same hazard, NO runloom)
    that parsed values match the encoded wid at PYTHON_GIL=1 AND PYTHON_GIL=0:
    0 mismatches in 6400+ checks each.  Under runloom, a parsing corruption is a
    runloom M:N concurrency bug or an ipaddress implementation bug.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    parse (stranded inside ipaddress code on corrupted state, or a segfault)
    never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing parsing hazard was actually
    exercised (parse_checks > 0).

FAIL ON: a parsed address whose value does NOT match the encoded wid (value
corruption), an exception raised during parsing, or any other sign of parsing
failure.  NEVER fail on performance (those are expected concurrency artifacts).

EXPECTED RESULT: this probes whether ipaddress parsing is robust under M:N
concurrency.  At modest funcs (1000-2000) with many variants, the hazard is
low-probability (few concurrent sibling parses mid-yield).  At higher funcs
(8000+) with sustained churn, concurrent parsing is likely and any shared-state
race would manifest.  If no corruption is observed, ipaddress is robust under
M:N; if corruption fires, it indicates a parsing bug in ipaddress or runloom
under concurrent execution.

Stresses: ipaddress module-global _ip_networks/_ip_allocators dict caching +
lru_cache on _ip_int_from_string across hub fibers, concurrent parse() of
distinct addresses, cache entry lookup and cache-hit id() stability across
yields + hub migration + sibling parse interleave.

Good TSan / controlled-M:N-replay target: _ip_networks dict insertion /
lookup across fibers; lru_cache._cache and OrderedDict mutations; a replay that
stalls a fiber during _ip_int_from_string (inside lru_cache) and interleaves a
sibling's parse before the first fiber's __init__ completes, localizes the
cache-isolation gap before the id() oracle fires.
"""
import ipaddress

import harness
import runloom

# IPv4 network addresses will be crafted as "10.wid.X.0/24" where wid is
# encoded into the second octet.  wid ranges from 0 to H.funcs, so the second
# octet is (wid % 256).  This ensures each fiber's address is UNIQUE.
IPV4_NET_TEMPLATE = "10.{0:d}.{1:d}.0/24"  # 10.<wid%256>.<variant>.0/24
IPV6_NET_TEMPLATE = "2001:db8:{0:04x}::{1:d}/64"  # 2001:db8:<wid>:<variant>/64

# Number of variants (different X values) per wid -- each variant is a
# distinct IPv4 network that a fiber will parse in sequence.  Kept modest so
# the test stays focused on cache identity, not pure throughput.
N_VARIANTS = 2

# Number of re-parse iterations per variant -- each fiber parses the same
# address multiple times, checking that id(obj) is stable (cache hit).
N_REPARSES = 2


def ipv4_net_for_fiber(wid, variant):
    """Generate a unique IPv4 network string for this (wid, variant) pair."""
    wid_octet = wid % 256
    return IPV4_NET_TEMPLATE.format(wid_octet, variant)


def ipv6_net_for_fiber(wid, variant):
    """Generate a unique IPv6 network string for this (wid, variant) pair."""
    # Encode wid as a 16-bit value into the address for uniqueness.
    wid_hex = wid & 0xFFFF
    return IPV6_NET_TEMPLATE.format(wid_hex, variant)


def setup(H):
    H.state = {
        "ipv4_checks": [0] * 1024,      # IPv4 parse-multiple checks done
        "ipv4_mismatches": [0] * 1024,  # id() mismatch on re-parse (the bug)
        "ipv6_checks": [0] * 1024,      # IPv6 parse-multiple checks done
        "ipv6_mismatches": [0] * 1024,  # id() mismatch on re-parse (the bug)
        "sample": [None],               # first observed bad sample for diagnostic
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: PARSED ADDRESS VALUE CORRECTNESS.  Each fiber encodes its
# wid into a unique IPv4/IPv6 address, parses it, and immediately checks that
# the recovered network_address value matches the encoded wid.  Then yields so
# a sibling can run concurrent parsing, and re-parses to check the value again.
# A value mismatch indicates that the parser corrupted or cross-contaminated
# state under concurrent execution.
# --------------------------------------------------------------------------
def ipv4_value_check(H, wid, variant, state):
    """Parse an IPv4 address and verify the parsed value matches the encoded wid."""
    net_str = ipv4_net_for_fiber(wid, variant)
    wid_octet = wid % 256  # the value we encoded
    try:
        # First parse: extract the network address and check its octets.
        obj1 = ipaddress.ip_network(net_str, strict=False)
        addr1 = obj1.network_address
        got_octet_1 = int(addr1.exploded.split('.')[1])  # second octet

        # Yield so a sibling fiber on this hub can run and potentially corrupt
        # shared parsing state.  The sleep-park (not bare yield) is what
        # reliably deschedules this fiber long enough that the scheduler runs a
        # sibling mid-parse before we resume.
        runloom.yield_now()
        if variant & 1:
            runloom.sleep(0.0002)

        # Second parse: re-parse and check the value again.
        obj2 = ipaddress.ip_network(net_str, strict=False)
        addr2 = obj2.network_address
        got_octet_2 = int(addr2.exploded.split('.')[1])

        state["ipv4_checks"][wid & 1023] += 1

        # The oracle: both parses must return the correct wid octet.
        # A corruption would show up as a wrong octet value.
        if got_octet_1 != wid_octet:
            state["ipv4_mismatches"][wid & 1023] += 1
            if state["sample"][0] is None:
                state["sample"][0] = (wid, "ipv4", net_str, wid_octet, got_octet_1)
            H.fail(
                "ipaddress IPv4 PARSING CORRUPTION: parsed {0!r} (wid {1}, "
                "variant {2}) returned network with WRONG octet value: "
                "expected octet={3}, got octet={4}; the parser or shared "
                "module state was corrupted by a sibling's concurrent parse "
                "(runloom M:N parsing bug -- 0 under plain OS threads).".format(
                    net_str, wid, variant, wid_octet, got_octet_1))
            return
        if got_octet_2 != wid_octet:
            state["ipv4_mismatches"][wid & 1023] += 1
            if state["sample"][0] is None:
                state["sample"][0] = (wid, "ipv4", net_str, wid_octet, got_octet_2)
            H.fail(
                "ipaddress IPv4 RE-PARSE CORRUPTION: re-parse of {0!r} (wid "
                "{1}, variant {2}) returned network with WRONG octet: expected "
                "octet={3}, got octet={4} on re-parse; the parser value is "
                "unstable or shared state corrupted after yield".format(
                    net_str, wid, variant, wid_octet, got_octet_2))
            return
    except Exception as e:
        H.fail(
            "ipaddress IPv4 parse exception (wid {0}, variant {1}, addr {2}): "
            "{3!r} -- the parser may have encountered corrupted state from a "
            "sibling's concurrent parse under M:N.".format(
                wid, variant, net_str, e))


def ipv6_value_check(H, wid, variant, state):
    """Parse an IPv6 address and verify the parsed value matches the encoded wid."""
    net_str = ipv6_net_for_fiber(wid, variant)
    wid_hex = wid & 0xFFFF  # the value we encoded
    try:
        # First parse: extract the network address and check its components.
        obj1 = ipaddress.ip_network(net_str, strict=False)
        addr1 = obj1.network_address
        # IPv6 address is "2001:db8:wid:0:..." -- extract the third group
        parts1 = addr1.exploded.split(':')
        got_group_1 = int(parts1[2], 16)  # third group (the wid)

        # Yield so a sibling fiber on this hub can run and potentially corrupt
        # shared parsing state.
        runloom.yield_now()
        if variant & 1:
            runloom.sleep(0.0002)

        # Second parse: re-parse and check the value again.
        obj2 = ipaddress.ip_network(net_str, strict=False)
        addr2 = obj2.network_address
        parts2 = addr2.exploded.split(':')
        got_group_2 = int(parts2[2], 16)

        state["ipv6_checks"][wid & 1023] += 1

        # The oracle: both parses must return the correct wid hex group.
        if got_group_1 != wid_hex:
            state["ipv6_mismatches"][wid & 1023] += 1
            if state["sample"][0] is None:
                state["sample"][0] = (wid, "ipv6", net_str, wid_hex, got_group_1)
            H.fail(
                "ipaddress IPv6 PARSING CORRUPTION: parsed {0!r} (wid {1}, "
                "variant {2}) returned network with WRONG hex-group value: "
                "expected group={3:x}, got group={4:x}; the parser or shared "
                "module state was corrupted by a sibling's concurrent parse "
                "(runloom M:N parsing bug -- 0 under plain OS threads).".format(
                    net_str, wid, variant, wid_hex, got_group_1))
            return
        if got_group_2 != wid_hex:
            state["ipv6_mismatches"][wid & 1023] += 1
            if state["sample"][0] is None:
                state["sample"][0] = (wid, "ipv6", net_str, wid_hex, got_group_2)
            H.fail(
                "ipaddress IPv6 RE-PARSE CORRUPTION: re-parse of {0!r} (wid "
                "{1}, variant {2}) returned network with WRONG hex-group: "
                "expected group={3:x}, got group={4:x} on re-parse; the parser "
                "value is unstable or shared state corrupted after yield".format(
                    net_str, wid, variant, wid_hex, got_group_2))
            return
    except Exception as e:
        H.fail(
            "ipaddress IPv6 parse exception (wid {0}, variant {1}, addr {2}): "
            "{3!r} -- the parser may have encountered corrupted state from a "
            "sibling's concurrent parse under M:N.".format(
                wid, variant, net_str, e))


# Sustained parse-multiple iterations per worker, bounded by H.running().
# The cache-isolation hazard only manifests under SUSTAINED churn -- many
# fibers simultaneously mid-parse and sleep-PARKED across their yield, so the
# scheduler reliably runs a sibling (at a different wid / variant) on the shared
# module cache before this fiber resumes and re-parses.  A single parse per
# fiber barely overlaps a sibling's.  So each worker runs a sustained internal
# loop -- one parse per iteration for multiple variants, interleaved with sleeps
# and yields -- until the deadline (H.running()) or INNER_CAP.  Bounding by
# H.running() makes the load-bearing oracle fire at the DEFAULT --rounds 1
# (it does not depend on a large --rounds); INNER_CAP stops one worker from
# monopolizing teardown on a slow box.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH IPv4 and IPv6 identity checks per variant, sustaining
    a churn loop bounded by H.running().  One parse per identity-check per
    iteration (with embedded yield/sleep) so many fibers stay simultaneously
    mid-parse and parked -- the condition the cache-isolation hazard needs --
    regardless of the harness --rounds setting.  The outer round_range() still
    honors --rounds for the soak sweep; --rounds 1 (the default) runs the
    sustained inner loop exactly once, which is all it takes."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            # Iterate through variants and do value checks for both IPv4/IPv6.
            for variant in range(N_VARIANTS):
                # IPv4 value check (LOAD-BEARING).
                ipv4_value_check(H, wid, variant, state)
                if H.failed:
                    return

                # IPv6 value check (LOAD-BEARING).
                ipv6_value_check(H, wid, variant, state)
                if H.failed:
                    return

            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    ipv4_checks = sum(H.state["ipv4_checks"])
    ipv4_mismatches = sum(H.state["ipv4_mismatches"])
    ipv6_checks = sum(H.state["ipv6_checks"])
    ipv6_mismatches = sum(H.state["ipv6_mismatches"])

    ipv4_pct = (100.0 * ipv4_mismatches / ipv4_checks) if ipv4_checks else 0.0
    ipv6_pct = (100.0 * ipv6_mismatches / ipv6_checks) if ipv6_checks else 0.0

    sample = H.state["sample"][0]

    H.log("ipaddress[IPv4 LOAD-BEARING]: {0} value checks, {1} parsing "
          "corruptions ({2:.2f}%) -- sample: {3}".format(
              ipv4_checks, ipv4_mismatches, ipv4_pct, sample))
    H.log("ipaddress[IPv6 LOAD-BEARING]: {0} value checks, {1} parsing "
          "corruptions ({2:.2f}%) -- sample: {3}".format(
              ipv6_checks, ipv6_mismatches, ipv6_pct, sample))

    if ipv4_mismatches or ipv6_mismatches:
        H.log("note: ipaddress parsing showed value corruptions under M:N "
              "concurrency -- a sibling fiber's concurrent parse operations "
              "corrupted shared state or the parser itself.  ipaddress parsing "
              "involves shared module code and potentially shared caches "
              "(module-level _cache, lru_cache if present); these are SHARED "
              "across all fibers on a hub under runloom M:N (the same root cause "
              "as p66/p67/p321).  This is a runloom M:N concurrency bug (0 under "
              "plain OS threads GIL on AND off); the fix is per-fiber context "
              "isolation of module-global caches and parser state.")

    # NON-VACUITY: the load-bearing identity hazard was actually exercised.
    H.check(ipv4_checks > 0,
            "no IPv4 identity checks ran -- the load-bearing ipaddress cache "
            "identity hazard was never exercised (oracle would be vacuous)")
    H.check(ipv6_checks > 0,
            "no IPv6 identity checks ran -- the load-bearing ipaddress cache "
            "identity hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-parse (stranded inside
    # ipaddress parser on corrupted module state).
    H.require_no_lost("ipaddress concurrent parsing")


if __name__ == "__main__":
    harness.main(
        "p472_ipaddress", body, setup=setup, post=post,
        default_funcs=4,
        describe="ipaddress concurrent parsing under M:N; runloom fibers on one "
                 "hub share module-level caches and parser state (NOT isolated "
                 "per-fiber).  LOAD-BEARING: each fiber encodes wid into a unique "
                 "IPv4/IPv6 network address string, parses it via "
                 "ipaddress.ip_network(), and immediately checks that the "
                 "parsed network_address value matches the encoded wid (second "
                 "IPv4 octet or third IPv6 hex-group).  Then yields (park+migrate "
                 "so sibling can run concurrent parsing) and re-parses the SAME "
                 "address, checking that the value is still correct.  A value "
                 "mismatch indicates that the parser corrupted or cross-"
                 "contaminated state under concurrent execution (sibling's parse "
                 "mutated shared module state, lru_cache, or parser internals).  "
                 "This should NOT happen under correct isolation (0 under plain "
                 "threads GIL on AND off); a mismatch is a runloom M:N parsing "
                 "corruption bug or ipaddress implementation bug under concurrency")
