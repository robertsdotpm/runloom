# Nightly reliability duty-cycle — how to enable

_docs/dev/RELIABILITY_PROGRAM.md R4._  These `systemd --user` units run
`tools/soak/duty_cycle.sh` once a night: a load-gated, niced rotation of
`hang_hunter` → `lifefuzz` → (weekly) a soak-matrix preset, filing every finding
into `docs/dev/soak/INBOX.md`.  **No hosted CI** — this is how fuzz-hours and
stress-hours accrue locally when nobody is at the keyboard.

**The repo does NOT install or enable these.** They consume real CPU for hours.
Enable them only on a box whose owner has agreed (they *are* load-gated and
`idle`-priority, so they back off under any foreground work — but still).

## Enable (per-user, no root)

```sh
mkdir -p ~/.config/systemd/user
cp tools/soak/systemd/runloom-duty.service ~/.config/systemd/user/
cp tools/soak/systemd/runloom-duty.timer   ~/.config/systemd/user/
# edit WorkingDirectory in the .service if the repo isn't at ~/projects/pygo
systemctl --user daemon-reload
systemctl --user enable --now runloom-duty.timer
# let it keep running after you log out:
loginctl enable-linger "$USER"
```

## Watch it

```sh
systemctl --user list-timers runloom-duty.timer   # next fire
journalctl --user -u runloom-duty.service -f       # live log
bash tools/soak/status.sh                          # machine-days + inbox + last runs
```

## Run one rotation by hand (no timer)

```sh
tools/soak/duty_cycle.sh --smoke          # seconds, plumbing check
tools/soak/duty_cycle.sh                  # the real nightly durations
tools/soak/duty_cycle.sh --matrix asan-24h  # force a specific weekly slot
```

## Disable

```sh
systemctl --user disable --now runloom-duty.timer
```

## Cron alternative (if you prefer)

```cron
0 2 * * *  cd ~/projects/pygo && nice -n15 ionice -c3 bash tools/soak/duty_cycle.sh >> ~/runloom-duty.log 2>&1
```
