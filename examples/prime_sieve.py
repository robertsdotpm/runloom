"""Concurrent prime sieve — a goroutine per prime.

Doug McIlroy's pipeline sieve, the example that sold Go's goroutines:
numbers flow down a chain of channels, and each prime spins up a new
filter goroutine that drops its own multiples.  What survives to the
end of the chain is prime.  It's gloriously wasteful and a perfect
stress test of cheap spawning + channel hand-off.

Run:
    python3 examples/prime_sieve.py
"""

import runloom

LIMIT = 60

def generate(out):
    for i in range(2, LIMIT + 1):
        out.send(i)
    out.close()

def filter_multiples(prime, inp, out):
    for n in inp:
        if n % prime != 0:
            out.send(n)            # only non-multiples continue downstream
    out.close()

def main():
    ch = runloom.Chan()
    runloom.go(generate, ch)

    primes = []
    while True:
        prime, ok = ch.recv()      # head of the chain is always prime
        if not ok:
            break
        primes.append(prime)
        nxt = runloom.Chan()
        runloom.go(filter_multiples, prime, ch, nxt)
        ch = nxt                    # next round reads from the filtered stream

    print("primes up to {0}:".format(LIMIT))
    print(" ".join(str(p) for p in primes))

if __name__ == "__main__":
    runloom.run(main)
