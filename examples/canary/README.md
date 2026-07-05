# The runloom canary

_docs/dev/RELIABILITY_PROGRAM.md R6._  A small but **real** service on runloom,
meant to run continuously for weeks.  It is the highest-fidelity reliability
test there is — a live service under live load — and it is the launch-credibility
artifact: *"a runloom server ran continuously for N days with flat gauges"* is
the only reliability claim users actually trust, and this makes it measurable
instead of a vibe.

## What it is

`server.py` — one runloom process exercising the whole stack at once:

- **echo** (TCP accept/echo — the park/wake + throughput workhorse),
- **chat room** (channels + `select` + timers + TCP: every line is broadcast to
  all joined clients through a per-client channel and a select fan-out, with a
  stoppable keepalive ticker per connection),
- **status endpoint** — send `stats\n`, get one JSON line of `{uptime_s, stats}`
  where `stats` is the R0 gauge surface (`runloom.stats()`).  So the service's
  health is observable from outside, live.

It arms the R5 crash + self-hang telemetry, so a canary wedge produces a
pasteable artifact instead of a silent stall, and it self-samples the R1 CSV so
the slope oracle can judge it.

`client.py` — the fleet driver: steady echo round-trips + chat participation +
a periodic **churn burst** (a wave of short-lived connections) that ages the
connection create/destroy cycle a real service sees daily.  It is itself a
runloom program, so a canary deployment is runloom-on-both-ends.

## Run it as a soak

```sh
examples/canary/run.sh 600 120      # 10-minute soak, 120s warmup
```

Starts server + client, samples every 30 s, then runs the slope oracle and
writes a `REPORT.txt`.  **Warmup matters:** the connection pool + arenas take
~30 s to reach steady state (VMAs/VSZ step up then plateau — pool establishment,
not a leak), so give a generous warmup; a real multi-day canary uses the
standard 600 s soak warmup, where the ramp is negligible.

Verified in-session: at steady state every core gauge is flat — RSS, `g_structs`,
`coro_stack_live`, `netpoll_parked` all plateau — oracle **PASS**.

**Churn bursts step the high-water once, then plateau — that is not a leak.** A
burst of N short-lived connections raises the pool high-water gauges
(`g_structs_total`, `coro_stack_live`, VMAs) to accommodate peak concurrency,
then they plateau at the new level and stay there; the freed structs/stacks are
retained in the pool and reused by the next identical burst (so the *second*
burst does not raise them further). The slope oracle's linear model reads that
one-time step as a slope if the burst lands inside the fitted window, so a
short verification run either avoids a mid-window burst or uses a generous
warmup. The real signal over a multi-day canary is whether the plateau is
**bounded** (healthy) or **rises burst-over-burst** (a genuine per-cycle leak) —
which is exactly what the weekly ledger sampling shows.

## The 21-day gate

The R6 acceptance is **21 consecutive days green** before any "production-ready"
claim in the docs.  Run the service under systemd (see below), sample it into
the soak ledger weekly, and only restart it for a deliberate upgrade (note each
in the ledger).  Any wedge trips the R5 watchdog artifact.

## Run continuously (systemd --user)

`systemd/runloom-canary.service` runs the server; drive load from the same or
another box (or a Windows VM via the SSH tooling, or a lossy netns via
`tools/soak/netns_chaos.sh`).  As with the R4 duty-cycle, **the repo does not
enable it** — install it only on a box whose owner agreed:

```sh
cp examples/canary/systemd/runloom-canary.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now runloom-canary.service
loginctl enable-linger "$USER"
# health, live:
printf 'stats\n' | nc 127.0.0.1 8803
```
