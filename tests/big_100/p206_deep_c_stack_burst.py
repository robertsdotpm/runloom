"""big_100 / 206 -- deep C-stack burst across a hub migration.

Each goroutine descends DEEP into a single C call -- the json C encoder/decoder
of a deeply nested structure -- interleaved with yield_now()/sleep() so the deep
C frames span a hub migration.  json.dumps / json.loads recurse one C frame per
nesting level in ONE call (no Python frame per level, unlike copy.deepcopy which
recurses in Python and is bounded by sys.recursionlimit); if that C burst
overflows the M:N goroutine's swapped C stack (the harness pins it to 512KB) the
process SEGVs.  The angle is the 128K M:N stack-overflow class -- here we pick a
depth that is deep but safe so the program PASSES, and the invariant proves the
data round-tripped correctly across the migration.

Stresses: deep C recursion on the swapped g-stack, json encode/decode depth, hub
migration mid-burst, the g-stack guard page.
"""
import json

import harness
import runloom

# Depth of the nested structure.  The json C encoder/decoder recurse one C frame
# per nesting level.  A 512KB g-stack comfortably holds a few-hundred-deep nest;
# 200 is "deep but safe" -- a genuine deep C burst on the swapped stack, well
# clear of the guard page.  If this depth SEGVs, lower DEPTH and report it.
DEPTH = 200


def make_nested(depth):
    """A nested dict {'v': i, 'n': {...}} `depth` levels deep, with a leaf."""
    node = {"v": depth, "leaf": [depth, depth * 2, "x" * (depth % 7)]}
    for i in range(depth - 1, -1, -1):
        node = {"v": i, "n": node, "tag": i & 0xFF}
    return node


def depth_of(obj):
    """Walk the 'n' chain and count levels -- a real structural check that the
    round-tripped object still has every level."""
    d = 0
    cur = obj
    while isinstance(cur, dict) and "n" in cur:
        cur = cur["n"]
        d += 1
    return d


def worker(H, wid, rng, state):
    template = make_nested(DEPTH)
    for _ in H.round_range():
        if not H.running():
            break
        # 1) json round-trip with a migration point sandwiched in the middle.
        #    dumps recurses DEPTH frames down the C stack in one call.
        text = json.dumps(template)
        runloom.yield_now()                  # likely resume on another hub
        back = json.loads(text)
        if not H.check(back == template,
                       "json round-trip mismatch wid={0} (depth={1})".format(
                           wid, DEPTH)):
            return
        if not H.check(depth_of(back) == DEPTH,
                       "json lost levels wid={0}: depth={1} expected {2}".format(
                           wid, depth_of(back), DEPTH)):
            return
        H.op(wid)

        # 2) a second json burst with a sleep (definite reschedule) sandwiched
        #    BETWEEN encode and decode, so the deep C decode frame materialises
        #    on a freshly-migrated hub.
        text2 = json.dumps(template)
        runloom.sleep(0.0)                    # definite reschedule point
        back2 = json.loads(text2)
        if not H.check(back2 == template and depth_of(back2) == DEPTH,
                       "second json burst mismatch wid={0}".format(wid)):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def setup(H):
    H.state = None


if __name__ == "__main__":
    harness.main("p206_deep_c_stack_burst", body, setup=setup,
                 default_funcs=1000,
                 describe="deep json/deepcopy C burst across a hub migration; "
                          "data round-trips, no g-stack overflow SEGV")
