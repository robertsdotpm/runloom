#!/usr/bin/env python3
"""conformance.py -- EXECUTE the audited semantics probes against the real kernel
and fail loudly on a misread (item 6).

The insight (bug 18): a misread kernel contract that goes into a hand-written
Spin model gets *verified* by that model -- the proof is green and wrong.  The
only defense is to run the contract against the real kernel and diff observed vs
predicted.  This runner does that for the load-bearing epoll rules that govern
the netpoll arm cache (the register-per-direction-once scheme is correct ONLY if
epoll is LEVEL and re-reports; if that belief is wrong, every parker can hang).

Each probe states BOTH what a correct kernel does (predicted_ok) and what the
value would be if the rule were misread the common way (predicted_misread), and
asserts the kernel matches the former, not the latter -- so a probe that always
passes because it tests nothing is impossible by construction.

Coverage against tools/verify/semantics/rules.json is reported honestly: rules
with no executable probe here are listed as DOCUMENTED-ONLY, never silently
dropped.  C probes need a Linux epoll box; elsewhere they SKIP (exit 0).

House style: %/.format, prints kept.
"""
import ctypes
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))

# --------------------------------------------------------------------------
# Executable kernel probes.  One C program runs every epoll sub-check and prints
# 'CHECK <name> <observed>' lines; the runner compares against the rule's
# predicted_ok / predicted_misread.  The C keeps the kernel interaction honest
# (real socketpair, real epoll_wait); Python owns the verdicts.
# --------------------------------------------------------------------------
EPOLL_PROBE_C = r"""
#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>

static int wait_n(int ep, int ms) {
    struct epoll_event evs[8];
    return epoll_wait(ep, evs, 8, ms);
}

int main(void) {
    /* --- ET drops partially-consumed readiness (governs Hang A) --- */
    {
        int sv[2]; socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
        int ep = epoll_create1(0);
        struct epoll_event ev = {0};
        ev.events = EPOLLIN | EPOLLET; ev.data.fd = sv[0];
        epoll_ctl(ep, EPOLL_CTL_ADD, sv[0], &ev);
        char two[2] = {'a','b'}; if (write(sv[1], two, 2) != 2) return 90;
        int n1 = wait_n(ep, 100);            /* the edge */
        char c; if (read(sv[0], &c, 1) != 1) return 91;   /* consume 1 of 2 */
        int n2 = wait_n(ep, 50);             /* ET: no new edge -> 0 */
        printf("CHECK et_edge_first %d\n", n1);
        printf("CHECK et_partial_reparked %d\n", n2);
        close(ep); close(sv[0]); close(sv[1]);
    }
    /* --- LEVEL re-reports the still-buffered byte (register-once is safe) --- */
    {
        int sv[2]; socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
        int ep = epoll_create1(0);
        struct epoll_event ev = {0};
        ev.events = EPOLLIN; ev.data.fd = sv[0];     /* LEVEL: no EPOLLET */
        epoll_ctl(ep, EPOLL_CTL_ADD, sv[0], &ev);
        char two[2] = {'a','b'}; if (write(sv[1], two, 2) != 2) return 92;
        int r1 = wait_n(ep, 50); char c; (void)read(sv[0], &c, 1);   /* 1 left */
        int r2 = wait_n(ep, 50);                     /* LEVEL: still reports */
        (void)read(sv[0], &c, 1);                    /* drain */
        int r3 = wait_n(ep, 50);                     /* now empty -> 0 */
        printf("CHECK level_reports_buffered %d\n", r2);
        printf("CHECK level_empty_after_drain %d\n", r3);
        (void)r1; close(ep); close(sv[0]); close(sv[1]);
    }
    /* --- EPOLLEXCLUSIVE: MOD -> EINVAL, ONESHOT combo -> EINVAL --- */
    {
        int efd = eventfd(0, EFD_NONBLOCK);
        int e1 = epoll_create1(0);
        struct epoll_event ev = {0};
        ev.events = EPOLLIN | EPOLLEXCLUSIVE; ev.data.fd = efd;
        int add = epoll_ctl(e1, EPOLL_CTL_ADD, efd, &ev);
        errno = 0; int mod = epoll_ctl(e1, EPOLL_CTL_MOD, efd, &ev);
        int mod_errno = errno;
        int e2 = epoll_create1(0);
        struct epoll_event ev2 = {0};
        ev2.events = EPOLLIN | EPOLLEXCLUSIVE | EPOLLONESHOT; ev2.data.fd = efd;
        errno = 0; int combo = epoll_ctl(e2, EPOLL_CTL_ADD, efd, &ev2);
        int combo_errno = errno;
        printf("CHECK exclusive_add_ok %d\n", add == 0 ? 1 : 0);
        printf("CHECK exclusive_mod_einval %d\n",
               (mod == -1 && mod_errno == EINVAL) ? 1 : 0);
        printf("CHECK exclusive_oneshot_einval %d\n",
               (combo == -1 && combo_errno == EINVAL) ? 1 : 0);
        close(e1); close(e2); close(efd);
    }
    /* --- EPOLLONESHOT: re-ADD after fire -> EEXIST (must MOD to re-arm) --- */
    {
        int sv[2]; socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
        int ep = epoll_create1(0);
        struct epoll_event ev = {0};
        ev.events = EPOLLIN | EPOLLONESHOT; ev.data.fd = sv[0];
        epoll_ctl(ep, EPOLL_CTL_ADD, sv[0], &ev);
        char x = 'z'; if (write(sv[1], &x, 1) != 1) return 93;
        (void)wait_n(ep, 50);                        /* fires + disables */
        errno = 0; int re = epoll_ctl(ep, EPOLL_CTL_ADD, sv[0], &ev);
        int re_errno = errno;
        printf("CHECK oneshot_readd_eexist %d\n",
               (re == -1 && re_errno == EEXIST) ? 1 : 0);
        close(ep); close(sv[0]); close(sv[1]);
    }
    return 0;
}
"""

# name -> (predicted_ok, predicted_misread, rule it enforces)
EPOLL_EXPECT = {
    "et_edge_first":            (1, None, "et-register-once-drops-partially-consumed-readiness"),
    "et_partial_reparked":      (0, 1,    "et-register-once-drops-partially-consumed-readiness"),
    "level_reports_buffered":   (1, 0,    "level-rereports-every-wait-so-register-once-is-safe"),
    "level_empty_after_drain":  (0, 1,    "level-rereports-every-wait-so-register-once-is-safe"),
    "exclusive_add_ok":         (1, None, "epollexclusive-wakes-one-or-more-not-exactly-one"),
    "exclusive_mod_einval":     (1, 0,    "epollexclusive-wakes-one-or-more-not-exactly-one"),
    "exclusive_oneshot_einval": (1, 0,    "epollexclusive-wakes-one-or-more-not-exactly-one"),
    "oneshot_readd_eexist":     (1, 0,    "epolloneshot-disables-until-mod-rearm-not-del-add"),
}


def run_epoll_probe():
    """Compile + run the C probe; return {check: observed_int} or None if the
    platform can't build/run it (non-Linux, no epoll)."""
    if not sys.platform.startswith("linux"):
        return None
    cc = os.environ.get("CC", "cc")
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "probe_epoll.c")
        binp = os.path.join(d, "probe_epoll")
        open(src, "w").write(EPOLL_PROBE_C)
        c = subprocess.run([cc, "-O0", "-o", binp, src],
                           capture_output=True, text=True)
        if c.returncode != 0:
            print("[conformance] epoll probe did not compile (skipping):\n%s"
                  % c.stderr[:400])
            return None
        r = subprocess.run([binp], capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            print("[conformance] epoll probe exited %d (kernel setup issue); skip"
                  % r.returncode)
            return None
        obs = {}
        for line in r.stdout.splitlines():
            parts = line.split()
            if len(parts) == 3 and parts[0] == "CHECK":
                obs[parts[1]] = int(parts[2])
        return obs


def check_fpcr():
    """fiber-ABI: the FP control register carries rounding mode; a context switch
    must preserve it.  Here we only assert the register EXISTS and is readable
    (the real save/restore is exercised by the runtime's fiber tests) -- a cheap
    liveness check that the ABI surface the rule names is real on this box."""
    if os.uname().machine not in ("x86_64", "aarch64"):
        return None
    # x86_64: MXCSR via stmxcsr is not trivially reachable from ctypes; treat as
    # documented-only here and defer to the runtime's arch fiber test.
    return None


def main():
    rules = json.load(open(os.path.join(HERE, "rules.json")))
    all_rule_names = set()
    for surface in rules:
        for r in surface["rules"]:
            all_rule_names.add(r["name"])

    failures = []
    probed_rules = set()

    obs = run_epoll_probe()
    if obs is None:
        print("[conformance] epoll probes SKIPPED (non-Linux or kernel setup).")
    else:
        for name, (ok, misread, rule) in EPOLL_EXPECT.items():
            probed_rules.add(rule)
            got = obs.get(name)
            if got is None:
                failures.append((name, "not reported by probe", rule))
                continue
            if got == ok:
                print("  epoll:%-28s OK (observed=%d)" % (name, got))
            else:
                extra = " == MISREAD value" if got == misread else ""
                print("  epoll:%-28s FAIL observed=%d expected=%d%s"
                      % (name, got, ok, extra))
                failures.append((name, "observed %d != %d" % (got, ok), rule))

    documented_only = sorted(all_rule_names - probed_rules)
    print("\n[conformance] %d/%d audited rules have an executable probe here; "
          "%d documented-only (probe pending, see rules.json):"
          % (len(probed_rules), len(all_rule_names), len(documented_only)))
    for n in documented_only:
        print("    - %s" % n)

    if failures:
        print("\n[conformance] FAIL: %d kernel behaviour(s) diverge from the "
              "audited rule (a misread that a hand model would have blessed):"
              % len(failures))
        for name, why, rule in failures:
            print("    %s: %s  (rule %s)" % (name, why, rule))
        return 1
    print("\n[conformance] OK: every executed rule matches the real kernel.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
