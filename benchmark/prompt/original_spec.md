# Runloom Benchmark Suite — Original Specification (archived verbatim)

> Archived from the user's original prompt for reproducibility. This is the
> source-of-truth spec the benchmark suite implements. Decisions and deviations
> agreed during scoping are recorded at the bottom under "Scoping decisions".

---

You are building a benchmark suite for Runloom. The project will be saved in a directory in the Runloom repo called benchmark. There will be different benchmark tests for aspects of the run time.

Requirements:
	- pin all programs to CPU cores
	- ensure debug mode is not used on any programs (as it interferes with benches)
	- use fresh netns to bypass firewall throttling for tests
	- system wide changes before bench ensure:
sysctl -w net.ipv4/tcp_wmem="4096 16384 2097152"  # 2 MB max
sysctl -w net.ipv4/tcp_rmem="4096 87380 2097152"
	- ensure fd limit can handle millions of connections
	- programs launched from vs code shell inherit limits -- ensure you take this into consideration

Performance benchmark: testing the requests / second of [runloom in various configurations, go]

Client:
	- Language = go
	- process cores = int(os.cpu_count() * 0.25)
	- fixed 1.5 mb buffer initalized at start
	- disable naggle for client socks
	- method:
		- num_no cons are connected to server
		- the buffer is sent to the server and read back
	- methodology:
		- start with low con no
		- measure req/s
		- continue to raise until req/s doesn't increase
		- return the max req/s as the result for the server

Servers:
	1. runloom default epoll/kqueue/wsa (zero optimized / bad):
		- uses wrapped python calls, no direct C calls, and python objects
		- hubs = int(os.cpu_count() * 0.7)
		- method:
			listener = runloom.sync.tcp_listen("127.0.0.1", 9000)
			while True:
				conn, _ = listener.accept()
				runloom.go(handle, conn)

	2. Runloom default epoll/kqueue/wsa (C optimized):
		- pros: uses C calls so should be faster than the sync / runloom api wrappers
		- cons: still a regular python func handler (no CPython.)
		- hubs = int(os.cpu_count() * 0.7)
		- method:
			def main():
				# C server scaffold: listen, accept, spawn all in C
				# Returns (bound_port, [listener_objects])
				port, listeners = runloom_c.serve(
					host="127.0.0.1",
					port=9000,
					handler=handle,
					acceptors=hubs,      # hubs accept-loop fibers (one per core, with SO_REUSEPORT)
					backlog=128
				)

				print(f"Server listening on {port}")
				# Server runs indefinitely; close listeners to stop
				try:
					runloom.sleep(float('inf'))
				except KeyboardInterrupt:
					for ln in listeners:
						ln.close()

			runloom.run(hubs, main)

	3. Runloom using io_uring (very optimized)
		- less security than default backends
		- python handlers still used here
		- hubs = int(os.cpu_count() * 0.7)
		- same code as 1. "runloom default zero optimized" but RUNLOOM_IOURING_LOOP=1

	4. Runloom using io_uring and Cython handlers
		- insecure but fast, handlers completely portable to C
		- hubs = int(os.cpu_count() * 0.7)
		- same code as 2 but handlers use Cython and RUNLOOM_IOURING_LOOP=1 backend.
		- this is VERY important:
			- the actual handler function for the call needs to be written in such a way that it doesnt output any Python objects otherwise you lose all the speed improvements. this needs to be confirmed by looking at the assembly directly. i will provide sample code.

			import os
			import socket as _sk
			import cython
			import runloom
			import runloom_c

			PORT   = 8080
			hubs   = int(os.cpu_count() * 0.7)
			CHUNK  = 1 << 18
			RCVBUF = SNDBUF = 1 << 20
			stop   = bytearray(1)

			def handler(conn):
				buf = bytearray(CHUNK)
				mv  = memoryview(buf)
				n: cython.int
				stop_flag: cython.bint

				while True:
					stop_flag = stop[0]
					if stop_flag:
						break
					n = conn.recv_into(buf, CHUNK)
					if not n:
						break
					conn.send_all(mv[:n])

			def root():
				port, listeners = runloom_c.serve("0.0.0.0", PORT, handler,
												  acceptors=hubs, backlog=4096)
				while True:
					runloom.sleep(3600)

			runloom.run(hubs, root)

	5. Runloom using io_uring and Cython handlers
		do the same as 4 but run optimize(throughput)

	asyncio
		- single threaded
		- use basic canonical python 3 asyncio protocol handlers

	gevent
		- you decide
	uvloop
		- same asyncio
	go
		- cap go's cores to int(os.cpu_count() * 0.7)

Speed benchmark
	- heading row: [runloom, go, asyncio, greenlet, uvloop]
		- time to spawn 1 mil empty fibers / coroutines
		- context switching time
		- http reqs / second (against a go server with int(os.cpu_count() * 0.7) cores
		- network TCP overhead (round trip to loopback) to a go server.

memory benchmark
	- heading: state: [go, runloom python handler, runloom python handler with optimize(memory), runloom c handler]
		- empty just spawned
		- spawned with a socket
		- 1 million

	- make it clear exactly that its used memory, not virtual memory ranges or w/e.

reporting:
	- scale everything down to 1 core e.g. asyncio stays the same but any multi-core programs get divided by core_no
	- sort by best performing to least
	- record hardware / os details for the machine that ran the bench at the top

save all results and code to be reused in the benchmark folder. i want a single consolodated view of all the data in a html file and also generate the relevant sections of the current read me (use a subset of the full data -- choose most relevant.) I want the read me sections to have footnotes that link back to the detailed benchmark html page in the benchmark directory. and the html page should be able to load code portions of the bench mark programs as well as the assumed constraints to produce reliable data. I'd also like it if the html page linked back to the backend profiling work that should already be saved in the pre-existing benchmark dir for (linux, mac, windows.) Save this full text prompt into prompt for archiving.

If you have any ideas for improvements to the benchmark that might improve accuracy before proceeding please interrupt me for ideas / discussions.

---

## Scoping decisions (agreed during setup, 2026-06-19)

1. **req/s payload (Q1 → a):** the headline **req/s** metric uses a **small payload (64 B–1 KB)** so it measures scheduling/syscall overhead, not loopback memcpy bandwidth. The **1.5 MB** buffer is kept as a **separate "bandwidth (GB/s)"** metric. Both are reported, clearly labelled.
2. **Zero-PyObject Cython tier (Q2 → b):** servers 4 & 5 use a **real Cython handler** that `cdef extern`s Runloom's cooperative recv/send as **plain C functions** and calls them **directly** (compiled-in C, no `PyObject_CallMethod`), pulling the connection's C handle once at entry. This requires a small addition to `runloom_c` exposing a C-level recv/send API. The `handler=None` all-C echo is *not* used for tiers 4/5 because it does not demonstrate a Cython-handler optimization. Zero-PyObject in the hot loop is proven via `objdump` of the compiled `.so`.
3. **Topology (Q3 → a):** client and server run in **separate network namespaces joined by a veth pair**, pinned to **disjoint NUMA nodes**. This isolates client/server contention and avoids the loopback fast-path that hides Runloom's io_uring win. Spec sysctls are applied **inside** the server netns (they are namespaced).
4. **Single-threaded baselines (Q4 → a):** asyncio / uvloop / gevent run on **GIL-enabled CPython 3.13** (their best case — no free-threaded atomic-refcount tax); Runloom and Go run on **free-threaded 3.13t**. gevent is installed on the GIL build. The interpreter/build is labelled per row. (Raw `greenlet` for the speed microbenchmark is already present and runs as-is.)
5. **Per-core normalization:** per-core figures are the **saturated M:N throughput divided by hub count** (and Go divided by GOMAXPROCS); single-threaded runtimes are already 1 core. We do **not** measure `run(1)` as "Runloom per core" — that is the M:1 cooperative scheduler, a different runtime than the M:N work-stealer. Raw saturated numbers are shown next to the divided ones so scaling efficiency is visible.
6. **TCP_NODELAY:** set once on listener + client socket templates (uniformly across every backend, outside the per-request hot loop), not via per-connection `setsockopt` in handler code — so it cannot creep in as per-call overhead. (Linux has no kernel-global nodelay sysctl.)
7. **Build:** as-shipped **release** build (`-O2`, fortify on), `RUNLOOM_DEBUG` unset and verified, no sanitizers. Built and run on `~/.pyenv/versions/3.13.13t` (free-threaded, Cython 3.2.5 present).
8. **Validity guards baked in:** geometric connection ladder with a rigorous
   stop rule (a rung "improves" only if its median req/s beats the incumbent
   peak's bootstrap-CI **upper** bound; `PLATEAU_PATIENCE` consecutive misses end
   the sweep), `REPS` independent reps per rung with a nonparametric bootstrap CI
   of the median, full curve + p50/p99/p99.9 reported, and per-core CPU
   utilisation (from `/proc/stat`, server-core group vs client-core group)
   recorded every rung to attribute the bottleneck.

## Implementation-refined behaviours (discovered while building, 2026-06-19)

These refine the decisions above based on what the real runtime/box required:

9. **The spec's API names were idealised; the real ones are used:**
   `runloom.go` -> `runloom.fiber`; `conn.send_all` is the C `TCPConn` method
   (the `runloom.sync.Socket` facade uses `sendall`); `runloom.run(n, main_fn)`.
   `runloom_c.serve`, `runloom.optimize("throughput")`, and
   `RUNLOOM_IOURING_LOOP=1` are all real and used as written. Debug is the
   `RUNLOOM_DEBUG` env var (default build is `-O2 -DNDEBUG` release), not a build
   flag -- the suite clears it and `env.py` records the proof.

10. **Zero-PyObject Cython handler is delivered via a new C-API (decision #2b):**
    `src/runloom_c/runloom_tcp_capi.{h,c.inc}` exposes
    `runloom_tcpconn_c_recv_into` / `_send_all` (the same epoll/io_uring
    cooperative core as the `TCPConn` methods, no Py_buffer/PyArg/PyLong), handed
    to the Cython module through the `runloom_c.__tcp_capi__` PyCapsule. The
    handler is built `freethreading_compatible=True` (or importing it would
    silently re-enable the GIL and kill M:N). `disasm_check.sh` objdumps the
    handler's implementation function and asserts the per-request loop is exactly
    two indirect capi calls + a stack-canary epilogue -- zero PyObject traffic.

11. **The 16-core client (spec: int(cpu*0.25)) cannot saturate the fast servers**
    for a symmetric echo (client does the same recv/send work as the server).
    So at the plateau the *client* is often CPU-bound, not the server. Rather than
    violate the spec's client budget, every rung records both CPU groups, the
    peak is tagged `bottleneck_at_peak` (server/client/neither), and when not
    server-bound a **`server_ceiling_est = peak_rps / server_cpu_util`** is
    reported (extrapolating the server's measured utilisation to 100%). This keeps
    the fast tiers distinguishable instead of all flat-lining at the client's
    limit, and is labelled an estimate, not a measurement.

12. **Topology is concrete (decision #3a):** two real netns (`rl_srv`, `rl_cli`)
    joined by a veth pair (`10.99.0.1` <-> `10.99.0.2`), passwordless `sudo`
    confirmed on the box. The 2-NUMA split is node0=cpu0-31, node1=cpu32-63;
    client pins to cpu0-15, server to cpu16-59 (disjoint, documented NUMA span).
    Spec sysctls (`tcp_wmem`/`tcp_rmem` 2 MB max) are set inside the server netns;
    `ip_local_port_range` is widened in the client netns so >32k connections fit
    under the ephemeral-port ceiling; `fs.nr_open`/`vm.max_map_count`/`somaxconn`
    kernel ceilings are raised; `RLIMIT_NOFILE` is raised per-exec via
    `prlimit` inside the netns (the editor-shell 4096 cap does not propagate).

13. **req/s vs bandwidth use different ladders:** the small-payload req/s metric
    walks connections up to 32768 (scheduling-bound); the 1.5 MB bandwidth metric
    uses a short ladder (1..128 connections) because a handful of streams already
    saturate the path -- and a deep ladder at 1.5 MB/conn would blow out memory.
    The Go loadgen establishes all connections in **parallel** (capped) so a
    high-connection rung does not serialise into a multi-second ramp.

## io_uring & thread-state investigation (2026-06-19, full record in `../IOURING_TSTATE_FINDINGS.md`)

14. **io_uring's loop backend WINS here — once driven through the Stage-2
    proactor.** The first cut showed the io_uring tiers *losing* (runloom_cython
    439k, server-bound) because the capi fell through to the *readiness* path
    (`recv()` + `wait_fd_coop` + the epoll→ring bridge): io_uring's bookkeeping
    with none of its win. The fix routes the capi through
    `runloom_iouring_loop_recv/send` (the proactor) when `RUNLOOM_IOURING_LOOP`
    is on. Result: runloom_cython 1 KiB went **439k → 639k (client-bound), server
    ceiling 533k → 1.16M** = +40% peak / **+2.17× ceiling**, at ~half the server
    CPU. So "io_uring loses on loopback" was an artifact of mis-driving it; the
    corrected suite reports io_uring as a major win via the proactor.

15. **The "+20% over epoll" reference reconciled.** Real but conditional: it's an
    **8-byte ping-pong** on a **tstate-free `c_entry`** fiber, and its writeup
    mis-attributed the mechanism (it credits an inline-drain skip-park in
    `ring_do`; the all-C echo actually calls `loop_io`, which *always* parks —
    the real win is **batching** one `submit_and_wait_timeout` across N parked
    conns). Magnitude is setup-dependent: **+6%** ceiling for the 8-byte all-C
    echo (epoll already near-optimal), **+117%** ceiling for the 1 KiB Cython
    handler. Measured head-to-head in `suite/iouring_compare.py` (8B `handler=None`
    + 1 KiB Cython, each epoll vs `RUNLOOM_IOURING_LOOP=1`).

16. **The tstate "omit-if-absent" optimization (`g->c_entry`) and its cost.**
    `runloom_g_entry` skips ALL Python-frame/tstate setup for a fiber spawned via
    `runloom_mn_fiber_c` (a C function pointer) — the all-C echo's other speed
    half. A Cython handler is a Python callable, so it gets a full Python fiber +
    tstate and pays `tstate_save/restore` on every proactor park. Default tstate
    mode is **per-hub snapshot** (no per-fiber `PyThreadState`); the per-g mode is
    gated off (mimalloc-heap migration SEGV). Measured: a full per-g
    `PyThreadState` is **~18 KB/fiber** (26.7 KB vs 8.8 KB snapshot vs 2.7 KB
    go-goroutine). **Buildable next step**: a `cdef`/C-pointer handler path for
    `serve()` (capsule → `mn_fiber_c` → `c_entry`) would give a *custom* handler
    the tstate-free fast path — should top even the proactor Cython tier on CPU
    and be lighter per fiber. Evidence backs it; not yet built.

17. **Open anomaly (reproducible):** the Cython handler on *epoll* is server-bound
    at 455k (533k ceiling) — *slower* than the Python handler on epoll (988k
    ceiling), despite being zero-PyObject and zero-alloc. It **vanishes under the
    io_uring proactor**, implicating the capi's epoll readiness path specifically.
    Flagged for a focused `perf`/flamegraph pass; not yet explained.

## Follow-up built + investigated (2026-06-19, branch feat/cdef-handler → main)

18. **`cdef`/`c_entry` handler tier BUILT — honest negative on throughput.**
    `serve()` now accepts a `runloom_c.c_handler` PyCapsule (a `cdef` C function)
    and spawns it via `runloom_mn_fiber_c` → the tstate-free `g->c_entry` path
    (raw-fd capi `fd_recv`/`fd_send_all`/`fd_close` + `module_io.c.inc` dispatch +
    `handler_cdef.pyx` + tier `srv_runloom_cdef.py`). Measured: the tstate-bypass
    buys ~nothing — `cdef` vs `cython` ceiling +0.25% at 8 B, +2.3% at 1 KiB (both
    noise). The default per-hub **snapshot** tstate is already cheap (snaps a few
    ints, not a `PyThreadState`), so there's nothing to bypass. Use `handler_cy`
    (Cython `def`) on the proactor; the `cdef` path's value is per-fiber memory.

19. **Anomaly #17 RESOLVED: there is no anomaly.** Arc: looked ~2× (cython epoll
    server-bound 425k vs py client-bound 620k) → called it an artifact → retracted
    when a netns re-measure reproduced 425k → settled definitively by saturation +
    objdump. At **2048 conns** (the load the ladder never reached): cython 604,946
    ≈ py 598,527 — **equal**; the "425k server-bound" was the ladder's plateau label
    misfiring at sub-saturation conn counts. Objdump: the cython capi `send_all` is
    **115 insns, ZERO `Py_` calls** vs the py method's 134 insns + 10 `Py_` calls —
    **no hidden Python object**, the Cython path is strictly leaner. Both hypotheses
    (hidden PyObject; shared-object lock contention) ruled out. **Lesson banked:**
    confirm a "server-bound" peak by saturating past the loadgen knee, don't trust
    the ladder's bottleneck label. Full trace: `../IOURING_TSTATE_FINDINGS.md` +
    `results/anomaly_notes.md`.
