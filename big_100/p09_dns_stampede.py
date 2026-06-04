"""big_100 / 09 -- DNS resolver stampede.

Tens of thousands of goroutines hammer socket.getaddrinfo concurrently with a
mix of lookups that succeed (numeric addresses, localhost) and lookups that
fail fast (a non-numeric string with AI_NUMERICHOST set -> EAI_NONAME, fully
offline).  getaddrinfo is a GIL-releasing blocking C call the scheduler must
offload to its worker pool without serialising everyone behind it.

Stresses: getaddrinfo blocking/offload, resolver-thread interaction, error
disposal.  Stays entirely offline (no external resolver is contacted).
"""
import socket

import harness

GOOD = ["127.0.0.1", "::1", "10.0.0.1", "192.168.0.1", "8.8.4.4", "localhost"]
BAD = ["definitely-not-an-ip", "still.not.numeric", "xxxxx", "::zz::"]


def lookup(host, numeric_only):
    flags = socket.AI_NUMERICHOST if numeric_only else 0
    return socket.getaddrinfo(host, 80, socket.AF_UNSPEC,
                              socket.SOCK_STREAM, 0, flags)


def client(H, wid, rng, state):
    H.sleep(rng.random() * 0.5)
    while H.running():
        if rng.random() < 0.7:
            host = rng.choice(GOOD)
            numeric = host not in ("localhost",)
            try:
                res = lookup(host, numeric_only=numeric)
                H.check(len(res) >= 1,
                        "no addrinfo for {0} (wid={1})".format(host, wid))
            except socket.gaierror as e:
                # localhost can legitimately fail on a misconfigured box; a
                # numeric address must never fail.
                if numeric:
                    H.fail("numeric lookup {0} failed: {1}".format(host, e))
        else:
            host = rng.choice(BAD)
            try:
                lookup(host, numeric_only=True)
                H.fail("bad name {0} resolved (wid={1})".format(host, wid))
            except (socket.gaierror, UnicodeError, OSError):
                pass            # expected fast offline failure
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, client, None)


if __name__ == "__main__":
    harness.main("p09_dns_stampede", body, default_funcs=10000,
                 describe="concurrent getaddrinfo stampede, success+fail mix")
