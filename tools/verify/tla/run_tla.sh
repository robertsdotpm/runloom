#!/usr/bin/env bash
# run_tla.sh -- TLC model-check the composed-scheduler TLA+ spec.
#
# Checks the correct protocol (all invariants + the AllComplete liveness
# property hold) and the negative control (Buggy=TRUE drops the pending-wake
# check -> AllComplete MUST be violated by a lost-wake lasso).  Prints a
# "N passed, M failed" line so run_verify.sh can fold it into the suite total.
#
# Needs java; fetches tla2tools.jar on first run (cached next to this script).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
JAR="${TLA_JAR:-$HERE/tla2tools.jar}"
URL="https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar"
JAR_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

echo "-- TLA+ (TLC: composed M:N scheduler, wake/park race) --"
if ! command -v java >/dev/null 2>&1; then
    echo "  (java not found -- skipping TLA+;  apt-get install default-jre)"
    exit 0
fi
# The jar is .gitignored, so a fresh clone fetches it and then EXECUTES it.
# Pin its contents; see tools/fetch_pinned.sh for the provenance caveat (this
# one is trust-on-first-use -- tlaplus publishes no checksums).
. "$(cd "$HERE/../../.." && pwd)/tools/fetch_pinned.sh"
rl_fetch_pinned "$URL" "$JAR_SHA256" "$JAR"
case $? in
    0) : ;;
    1) echo "  (could not fetch tla2tools.jar -- skipping TLA+)"; exit 0 ;;
    2) echo "  tla2tools.jar FAILED its pin -- refusing to run it."; exit 1 ;;
esac

pass=0; fail=0
META="$(mktemp -d /tmp/runloom_tlc.XXXX)"
# -Xmx1g + -workers 4: these models are tiny (<3k states), but TLC's JVM DEFAULT
# max-heap is 25% of physical RAM (~20g on this box) and it spawns nproc (64)
# worker threads -- so under the parallel verify pool (VERIFY_JOBS) several heavy
# jobs at once could OOM-kill a TLC instance, which then emits no result and a
# negative-control grep spuriously FAILs.  The bound is still worth keeping for
# that reason, but note what it did NOT do: the load-only flake it was added to
# fix survived it at ~5% for 380 measured lane runs, because that flake was
# never about memory at all (see -Djava.io.tmpdir below).  Bounding the heap
# also did not make a failure legible: the callers below pipe run_tlc into
# `grep -q` and discard
# everything else, so "TLC was OOM-killed and said nothing" and "TLC ran
# fine and the property HELD" produce the identical one-word FAIL. Those
# are opposite situations -- the first is infrastructure noise, the second
# means a negative control has stopped detecting its bug, which is a real
# regression hiding as a flake. So tee every run to a log and let the FAIL
# branches say which happened.
#
# TLC_XMX / TLC_WORKERS override the bounds without editing this file -- which
# is what tlc_why tells you to do when it classifies a failure as heap
# exhaustion, so it had better be possible.
#
# -Djava.io.tmpdir is NOT a tidiness flag -- it is the fix for the ~5% flake
# that the heap bounds above were originally (wrongly) blamed for.  SANY
# resolves EXTENDS Naturals/Sequences/FiniteSets out of the jar, and
# util.SimpleFilenameToStream.read() extracts each one to a FIXED, unqualified
# path -- java.io.tmpdir + "/" + "Naturals.tla", no pid, no random suffix --
# with a truncating FileOutputStream, then marks it deleteOnExit().  So any two
# TLC JVMs sharing /tmp race on the same three files: one truncates what
# another is parsing, or exits and deletes it mid-parse.  The victim dies with
#   Module-Table lookup failure for module name X derived from X file name
#   java.lang.NullPointerException: Cannot invoke "String.length()" ... "str" is null
# which reads as "the spec is broken" and, through `| grep -q`, as a one-word
# FAIL indistinguishable from a negative control that stopped detecting its
# bug.  Giving each JVM a private tmpdir makes the extraction unshared.
# This is why the flake only ever appeared in the parallel lane, why it struck
# correct-controls and negative-controls alike, and why memory pressure never
# reproduced it.
#
# UPSTREAM STATE.  This is tlaplus/tlaplus#688, "SANY fails randomly when run
# concurrently in several VMs" -- open at the time of writing.  Upstream master
# already fixes it (getTempDirectory() now returns Files.createTempDirectory
# ("tlc-"), unique per process), but that is in NO RELEASED JAR: v1.7.4
# (Aug 2024) is still the latest and still uses the shared fixed name.  So
# bumping the pin is not an option, and this flag is what master does
# internally, applied from outside.  When a release does carry the fix, this
# flag becomes redundant rather than wrong -- check before removing it.
#
# MEASURED.  Concurrent mixed specs, pooled over two experiments: 7/120 parse
# aborts without the flag, 0/120 with (Fisher one-tailed p ~ 0.007).  Neither
# experiment is significant alone; the weight is on the mechanism above, read
# out of the jar's bytecode and confirmed by watching /tmp/{Naturals,Sequences,
# FiniteSets}.tla appear during a run and vanish at exit.  In the lane the
# failure ran at 9 in 380 verify-fast runs -- rare enough to look like noise,
# common enough to erode trust in the gate.
#
# DO NOT REMOVE THIS FLAG AS TIDYING.  Besides the flake, the shared path is a
# local security defect: CWE-377 (predictable temp filename) plus CWE-59
# (FileOutputStream follows symlinks).  Any local user who can write /tmp can
# pre-create /tmp/Naturals.tla as a symlink and make TLC, running as you,
# truncate and overwrite an arbitrary file you own.  Demonstrated:
#     $ ln -s ~/victim.txt /tmp/Naturals.tla
#     $ java -cp tla2tools.jar tlc2.TLC RunloomWake.tla   # tlc-tmpdir-lint: quoted
#       ^ omits the flag ON PURPOSE; the marker keeps the lint off this line
#     $ head -1 ~/victim.txt
#     -------------------------------- MODULE Naturals ------------------------
# A private tmpdir closes that too, since the path stops being predictable.
# Irrelevant on a single-user box; not irrelevant on a shared one.
# tools/verify/tlc_tmpdir_lint.py enforces the flag on every TLC call site.
run_tlc() {
    local _log="$META/$1.log"
    mkdir -p "$META/$1.tmp"
    # Postmortem flags, all no-cost on a passing run:
    #   HeapDumpOnOutOfMemoryError -- the OOM path is the documented cause of
    #     the load-only flake here, and "it OOMed" is a much weaker finding
    #     than a heap dump showing WHAT filled 1g on a <3k-state model.
    #   ExitOnOutOfMemoryError is deliberately NOT set: we want the dump and
    #     the stack trace in the log, not a silent exit.
    #   TLC_JDWP=1 opens a debugger port (non-suspending) for a live attach.
    # Expanded as ${jdwp[@]+"${jdwp[@]}"}, not "${jdwp[@]}": under `set -u`
    # bash 3.2 -- which is what macOS ships, and the only bash the macOS CI
    # legs have -- treats expanding an EMPTY array as an unbound variable and
    # aborts.  bash 4.4+ does not, so this passed on Linux and failed every
    # macOS leg with `jdwp[@]: unbound variable`, which surfaced as TLC
    # "produced no recognised verdict" and a wall of spurious spec FAILs.
    local jdwp=()
    [ "${TLC_JDWP:-}" = "1" ] && jdwp=(-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:${TLC_JDWP_PORT:-5005})
    ( cd "$HERE" && java "-Xmx${TLC_XMX:-1g}" "-Djava.io.tmpdir=$META/$1.tmp" \
        -XX:+HeapDumpOnOutOfMemoryError "-XX:HeapDumpPath=$META/$1.hprof" \
        ${jdwp[@]+"${jdwp[@]}"} -cp "$JAR" tlc2.TLC \
        -workers "${TLC_WORKERS:-4}" -metadir "$META/$1" "${@:2}" 2>&1 ) \
        | tee "$_log"
}

# Classify the last TLC run for a FAIL branch: did it finish and disagree,
# or did it never get to an answer?
# Takes the CHECK TAG (run_tlc's first arg), not a variable.
#
# It used to read a global $LAST_TLC_LOG that run_tlc assigned -- which never
# worked: every caller invokes `run_tlc ... | grep -q ...`, and a function in a
# pipeline runs in a SUBSHELL, so the assignment never reached the parent.
# tlc_why therefore reported "(no TLC output captured)" on every failure while
# the log sat on disk the whole time. Caught by an actual reproduction.
tlc_why() {
    local log="$META/$1.log"
    [ -f "$log" ] || { echo "         (no TLC output captured)"; return; }
    if grep -qiE "OutOfMemoryError|Java heap space|GC overhead" "$log"; then
        echo "         cause: TLC ran out of heap -- INFRASTRUCTURE, not the model."
        echo "         Retry serially (VERIFY_JOBS=1) or raise the bound:"
        echo "           TLC_XMX=4g scripts/check_all.sh verify-fast"
    elif grep -q "No error has been found" "$log"; then
        echo "         cause: TLC COMPLETED and found no violation."
        echo "         For a negative control that is a REAL regression: the"
        echo "         injected bug is no longer detected. Do not retry past it."
    elif grep -q "Module-Table lookup failure\|tla2sany.semantic.AbortException" "$log"; then
        echo "         cause: SANY could not parse a spec it parses fine alone."
        echo "         Almost certainly the shared-/tmp standard-module race:"
        echo "         another TLC JVM truncated or deleteOnExit()-removed"
        echo "         /tmp/{Naturals,Sequences,FiniteSets}.tla mid-parse."
        echo "         The -Djava.io.tmpdir above exists to prevent exactly this;"
        echo "         check whether some other TLC is running WITHOUT it."
        grep -iE "^Error|Exception|failed" "$log" | head -3 | sed "s/^/           /"
    elif grep -qiE "^Error|Exception|Parsing or semantic analysis failed" "$log"; then
        echo "         cause: TLC errored before reaching a verdict:"
        grep -iE "^Error|Exception|failed" "$log" | head -3 | sed "s/^/           /"
    else
        echo "         cause: unclear -- TLC produced no recognised verdict."
    fi
    echo "         full log: $log"
}

printf '  [tlc] %-28s ' "RunloomSched (correct)"
if run_tlc ok -config RunloomSched.cfg RunloomSched.tla | grep -q "No error has been found"; then
    echo "PASS -- TypeOK/NoDoubleRun/DoneIsTerminal + AllComplete (liveness)"; pass=$((pass+1))
else
    tlc_why ok; echo "FAIL -- correct spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomSched (Buggy=TRUE)"
if run_tlc bug -deadlock -config RunloomSched_bug.cfg RunloomSched.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS lost wakeup -> AllComplete violated"; pass=$((pass+1))
else
    tlc_why bug; echo "FAIL -- the injected lost-wake bug should violate AllComplete"; fail=$((fail+1))
fi

# ---- Netpoll-drain wake protocol: the layer RunloomSched does NOT cover -- the
# single-thread drain's decide-to-block window vs a foreign thread's append+poke,
# and the f214341 2ms FOREIGN-THREAD WAKE BACKSTOP.  Correct = a possibly-lost
# poke self-heals via the bounded re-poll while EITHER backstop term is armed
# (bp_inflight>0 OR foreign_park_inflight>0 -- both load-bearing; TLC found the
# lasso when only the first armed it).  Negative control: Backstop=FALSE leaves
# the block unbounded, so a poke lost after a peek-empty strands the fiber.
printf '  [tlc] %-28s ' "RunloomWake (correct)"
if run_tlc wkok -config RunloomWake.cfg RunloomWake.tla | grep -q "No error has been found"; then
    echo "PASS -- TypeOK/ResumeIsTerminal + AllWoken (2ms backstop closes the lost-poke window)"; pass=$((pass+1))
else
    tlc_why wkok; echo "FAIL -- the backstopped wake protocol should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomWake (Backstop=FALSE)"
if run_tlc wkbug -deadlock -config RunloomWake_bug.cfg RunloomWake.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the lost-wakeup lasso (unbounded block strands a poked-but-lost fiber)"; pass=$((pass+1))
else
    tlc_why wkbug; echo "FAIL -- no backstop should violate AllWoken (liveness)"; fail=$((fail+1))
fi

# ---- M:N hub-submit wake (route A, default mode): the SIBLING of RunloomWake for
# an M:N hub.  A foreign waker appends g to the OWNER hub's per-hub sub_head and
# fires TWO free-delivery kicks (idle_cond + wake-pump eventfd); an idle M:N hub
# never blocks unbounded -- it polls ~1ms and re-drains sub_head -- so even with
# BOTH kicks lost the appended fiber self-heals.  Negative control: BoundedPoll=
# FALSE (the unbounded-block regression) strands it -> the M:N lost-wakeup lasso.
printf '  [tlc] %-28s ' "RunloomMNWake (correct)"
if run_tlc mnwkok -config RunloomMNWake.cfg RunloomMNWake.tla | grep -q "No error has been found"; then
    echo "PASS -- TypeOK/ResumeIsTerminal + AllWoken (~1ms bounded poll closes the lost-kick window)"; pass=$((pass+1))
else
    tlc_why mnwkok; echo "FAIL -- the bounded-poll M:N wake protocol should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMNWake (Bounded=FALSE)"
if run_tlc mnwkbug -deadlock -config RunloomMNWake_bug.cfg RunloomMNWake.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the M:N lost-wakeup lasso (unbounded hub block + both kicks lost)"; pass=$((pass+1))
else
    tlc_why mnwkbug; echo "FAIL -- no bounded poll should violate AllWoken (liveness)"; fail=$((fail+1))
fi

# ---- io_uring CQE wake: a fiber submits an SQE and parks; the kernel posts a CQE
# and signals the registered eventfd -- EXCEPT for a completion forced into the CQ
# overflow backlog, which the kernel does NOT re-signal (the lost-wakeup).  Correct
# = while inflight>0 the drain runs the GETEVENTS overflow flush FIRST (drain-first,
# drain.c:155-158) so the backlogged completion becomes visible and is consumed.
# Negative control: Heal=FALSE drops the flush -> a stranded completion is lost.
printf '  [tlc] %-28s ' "RunloomIouringWake (correct)"
if run_tlc iouwkok -config RunloomIouringWake.cfg RunloomIouringWake.tla | grep -q "No error has been found"; then
    echo "PASS -- TypeOK/ResumeIsTerminal/NoStrandedCompletion + AllWoken (drain-first flush closes the overflow window)"; pass=$((pass+1))
else
    tlc_why iouwkok; echo "FAIL -- the drain-first-flush iouring wake protocol should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomIouringWake (Heal=FALSE)"
if run_tlc iouwkbug -deadlock -config RunloomIouringWake_bug.cfg RunloomIouringWake.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the CQ-overflow lost-wakeup lasso (no flush strands a backlogged completion)"; pass=$((pass+1))
else
    tlc_why iouwkbug; echo "FAIL -- no overflow flush should violate AllWoken (liveness)"; fail=$((fail+1))
fi

# ---- Controlled M:N scheduler (RUNLOOM_MN_SEED experiment): the baton +
# rendezvous protocol.  Correct = mutual-exclusion + deadlock-free +
# deterministic grant.  Two negative controls model the two real obstacles:
# no preemption -> a CPU-bound hub starves all (the deadlock fixed by keeping
# preemption on); no barrier -> a grant over a partial requester set (the
# residual nondeterminism the rendezvous removes).
printf '  [tlc] %-28s ' "RunloomMNControl (correct)"
if run_tlc mnok -config RunloomMNControl.cfg RunloomMNControl.tla | grep -q "No error has been found"; then
    echo "PASS -- MutualExclusion/BatonConsistent/DeterministicGrant + AllRun (no deadlock)"; pass=$((pass+1))
else
    tlc_why mnok; echo "FAIL -- controlled-baton+rendezvous spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMNControl (Preempt=FALSE)"
if run_tlc mnnp -config RunloomMNControl_nopreempt.cfg RunloomMNControl.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the baton deadlock (CPU-bound hub starves all) without preemption"; pass=$((pass+1))
else
    tlc_why mnnp; echo "FAIL -- no preemption should violate AllRun (liveness)"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMNControl (Barrier=FALSE)"
if run_tlc mnnb -config RunloomMNControl_nobarrier.cfg RunloomMNControl.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS a grant over a partial requester set (nondeterminism) without the rendezvous"; pass=$((pass+1))
else
    tlc_why mnnb; echo "FAIL -- no barrier should violate DeterministicGrant"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMNControl (timers+clock)"
if run_tlc mntm -config RunloomMNControl_timer.cfg RunloomMNControl.tla | grep -q "No error has been found"; then
    echo "PASS -- logical clock: DeterministicTick + MutualExclusion + AllRun hold with timer waits"; pass=$((pass+1))
else
    tlc_why mntm; echo "FAIL -- timers + logical clock spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMNControl (LogicalClock=F)"
if run_tlc mnlc -config RunloomMNControl_nologicalclock.cfg RunloomMNControl.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS a later timer firing before an earlier deadline (nondeterminism) without the logical clock"; pass=$((pass+1))
else
    tlc_why mnlc; echo "FAIL -- no logical clock should violate DeterministicTick"; fail=$((fail+1))
fi

# ---- CPython STW boundary (RunloomCPythonSTW): the contract between runloom's
# hubs and free-threaded CPython's stop-the-world machinery (M1 attach/detach +
# M2 stop_the_world, read from Python/pystate.c -- see docs/dev/cpython_boundary.md).
# Correct = STWExclusive (no non-requester hub is ATTACHED while the world is
# stopped).  Negative control models bug 2 / contract C3: re-attaching a SUSPENDED
# tstate without the wait_attach gate -> a hub attached during a stopped world.
printf '  [tlc] %-28s ' "RunloomCPythonSTW (correct)"
if run_tlc stwok -config RunloomCPythonSTW.cfg RunloomCPythonSTW.tla | grep -q "No error has been found"; then
    echo "PASS -- STWExclusive + RequesterAttached hold (STW reclaims with all others suspended)"; pass=$((pass+1))
else
    tlc_why stwok; echo "FAIL -- correct STW-boundary spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomCPythonSTW (Bypass=T)"
if run_tlc stwbug -deadlock -config RunloomCPythonSTW_bug.cfg RunloomCPythonSTW.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS the handoff re-attach (a hub ATTACHED while the world is stopped) -> STWExclusive violated"; pass=$((pass+1))
else
    tlc_why stwbug; echo "FAIL -- re-attaching a suspended tstate mid-STW should violate STWExclusive"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomCPythonSTW (liveness)"
if run_tlc stwlive -config RunloomCPythonSTW_live.cfg RunloomCPythonSTW.tla | grep -q "No error has been found"; then
    echo "PASS -- STWCompletes: every requested stop-the-world eventually completes"; pass=$((pass+1))
else
    tlc_why stwlive; echo "FAIL -- a correctly-detaching system should always complete STW"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomCPythonSTW (BlockAttach)"
if run_tlc stwlb -deadlock -config RunloomCPythonSTW_livebug.cfg RunloomCPythonSTW.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the STW-monopoly hang (a hub blocks while attached) -> STWCompletes violated"; pass=$((pass+1))
else
    tlc_why stwlb; echo "FAIL -- a hub blocked-while-attached should wedge stop-the-world"; fail=$((fail+1))
fi

# ---- M4: the GILState-TSS binding (RunloomGilstate): the teardown contract C6,
# the bug the --with-pydebug oracle found (pystate.c:345).  Correct = each hub
# deletes its own tstate on its own thread.  Negative control deletes hub tstates
# from the main thread -> the assert fires + the binding is corrupted.
printf '  [tlc] %-28s ' "RunloomGilstate (correct)"
if run_tlc gilok -config RunloomGilstate.cfg RunloomGilstate.tla | grep -q "No error has been found"; then
    echo "PASS -- GilstateContract + GilBindingConsistent hold (hub deletes its own tstate on its own thread)"; pass=$((pass+1))
else
    tlc_why gilok; echo "FAIL -- correct gilstate teardown should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomGilstate (wrong thread)"
if run_tlc gilbug -deadlock -config RunloomGilstate_bug.cfg RunloomGilstate.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS the pystate.c:345 abort (deleting a hub tstate from the main thread) -> GilstateContract violated"; pass=$((pass+1))
else
    tlc_why gilbug; echo "FAIL -- deleting a gilstate-bound tstate from the wrong thread should violate the contract"; fail=$((fail+1))
fi

# ---- Tier-1 #2: the per-g tstate / mimalloc-heap MIGRATION hazard
# (RunloomTstateMigration): models mimalloc's per-PAGE owner thread and proves
# the abandon-on-detach + adopt-on-attach handshake is NECESSARY to migrate a
# tstate hub->hub without a cross-thread page op (the SEGV gated off in 70e6ddb).
# Correct = the handshake keeps owner == operating thread; the negative control
# drops it -> a page allocated on hub A is operated on hub B.
printf '  [tlc] %-28s ' "RunloomTstateMigration (handshake)"
if run_tlc mig -config RunloomTstateMigration.cfg RunloomTstateMigration.tla | grep -q "No error has been found"; then
    echo "PASS -- NoCrossThreadPageOp + NoForeignOwnerWhileAttached hold (abandon/adopt keeps page owner == operating hub)"; pass=$((pass+1))
else
    tlc_why mig; echo "FAIL -- the abandon/adopt handshake spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomTstateMigration (no h/shake)"
if run_tlc migbug -deadlock -config RunloomTstateMigration_bug.cfg RunloomTstateMigration.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS the mimalloc heap migrating hub->hub (a page owned by hub A operated on hub B) -> NoForeignOwnerWhileAttached violated"; pass=$((pass+1))
else
    tlc_why migbug; echo "FAIL -- migrating a tstate without the abandon/adopt handshake should violate page ownership"; fail=$((fail+1))
fi

# ---- Tier-2 #6: the runloom_g_t REFCOUNT LEDGER composed with the wake_state
# machine (RunloomGRefcount).  wake_state.pml proves the entry/owner discipline;
# this proves the integer refcount stays consistent with it (rc == scheduler ref +
# the 0/1 global-runq queue ref), so a g is freed exactly once and never while a
# queue entry could still resume it.  Negative control drops the QUEUED->RUNNING
# decref (a consumed queue ref leaked).
printf '  [tlc] %-28s ' "RunloomGRefcount (correct)"
if run_tlc grcok -config RunloomGRefcount.cfg RunloomGRefcount.tla | grep -q "No error has been found"; then
    echo "PASS -- Ledger + RcNonNeg + FreedConsistent hold (refcount tracks the wake_state)"; pass=$((pass+1))
else
    tlc_why grcok; echo "FAIL -- the refcount-ledger spec should hold"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomGRefcount (lost decref)"
if run_tlc grcbug -deadlock -config RunloomGRefcount_bug.cfg RunloomGRefcount.tla | grep -q "is violated"; then
    echo "PASS -- correctly DETECTS a consumed global-runq entry that forgets runloom_g_decref -> the queue ref leaks (Ledger violated, g never freed)"; pass=$((pass+1))
else
    tlc_why grcbug; echo "FAIL -- a lost queue-ref decref should violate the refcount ledger"; fail=$((fail+1))
fi

# ---- Tier-2 #9: the mn_fini TEARDOWN stop-signal handshake (RunloomMnFini).  The
# known flaky mn_fini hang: a hub idle-parked in its condvar misses the stop signal
# -> pthread_join blocks.  Correct = signal idle_cond UNDER idle_lock (no lost
# wakeup); the control signals without the lock -> the wakeup is lost -> the hub
# waits forever (JoinCompletes liveness violated).
printf '  [tlc] %-28s ' "RunloomMnFini (under lock)"
if run_tlc finiok -config RunloomMnFini.cfg RunloomMnFini.tla | grep -q "No error has been found"; then
    echo "PASS -- MutexOK + JoinCompletes hold (under-lock stop signal -> join always completes)"; pass=$((pass+1))
else
    tlc_why finiok; echo "FAIL -- the under-lock teardown signal should always join"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMnFini (no lock)"
if run_tlc finibug -deadlock -config RunloomMnFini_bug.cfg RunloomMnFini.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the lost stop-signal (signal without idle_lock) -> hub waits forever -> JoinCompletes violated"; pass=$((pass+1))
else
    tlc_why finibug; echo "FAIL -- signalling without the lock should lose the wakeup and hang the join"; fail=$((fail+1))
fi

# ---- WHOLE-PROGRAM LIVENESS (RunloomComposite): the scheduler + every wake
# source (channels / netpoll / timers / foreign) composed, checked for NoHang
# (every goroutine eventually completes).  Real hangs live in the seams between
# subsystems; the two negative controls are the two ways the shared route-to-home
# + don't-idle-past-a-wake machinery breaks.
printf '  [tlc] %-28s ' "RunloomComposite (correct)"
if run_tlc compok -config RunloomComposite.cfg RunloomComposite.tla | grep -q "No error has been found"; then
    echo "PASS -- NoHang holds: every g completes across the channel + external (fd/timer/foreign) seams"; pass=$((pass+1))
else
    tlc_why compok; echo "FAIL -- the composed scheduler should be hang-free"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomComposite (Quiesce)"
if run_tlc compq -deadlock -config RunloomComposite_quiesce.cfg RunloomComposite.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the census-idle wake-guard hang (a hub idles past a wake) -> NoHang violated"; pass=$((pass+1))
else
    tlc_why compq; echo "FAIL -- removing the wake-guard should hang"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomComposite (Route)"
if run_tlc compr -deadlock -config RunloomComposite_route.cfg RunloomComposite.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the wake-misrouting hang (external wake to the wrong hub) -> NoHang violated"; pass=$((pass+1))
else
    tlc_why compr; echo "FAIL -- misrouting an external wake should hang"; fail=$((fail+1))
fi

# ---- FV gap #2: the mn_run DEADLOCK CENSUS + STALL-KICK liveness backstop
# (RunloomMnRun, mn_sched_init_fini.c.inc 937-1072).  Correct = kick_all_hubs()
# fires ALL THREE wake paths (ring + idle_cond + pump), so a stranded-runnable g
# is always recovered; the control fires idle_cond ONLY -> a ring/pump-blocked
# hub is missed -> permanent lost wakeup (the cov_workload --hubs 4 hang).
printf '  [tlc] %-28s ' "RunloomMnRun (backstop)"
if run_tlc mrok -config RunloomMnRun.cfg RunloomMnRun.tla | grep -q "No error has been found"; then
    echo "PASS -- NoFalseDeadlock + SubsetOK + EventuallyRun hold (all-mode kick recovers the stranded g)"; pass=$((pass+1))
else
    tlc_why mrok; echo "FAIL -- the all-mode-kick backstop should recover the stranded g"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMnRun (idle-only kick)"
if run_tlc mrbug -deadlock -config RunloomMnRun_bug.cfg RunloomMnRun.tla | grep -q "Temporal properties were violated"; then
    echo "PASS -- correctly DETECTS the idle_cond-only kick missing a ring/pump-blocked hub -> EventuallyRun violated (permanent lost wakeup)"; pass=$((pass+1))
else
    tlc_why mrbug; echo "FAIL -- an idle-only kick should strand a ring/pump-blocked hub's g"; fail=$((fail+1))
fi

printf '  [tlc] %-28s ' "RunloomMnRun (deadlock witness)"
if run_tlc mrsafe -deadlock -config RunloomMnRun_safety.cfg RunloomMnRun.tla | grep -q "is violated"; then
    echo "PASS -- census fires a real DEADLOCK verdict only under ~has_wakeable_work (NoFalseDeadlock non-vacuous)"; pass=$((pass+1))
else
    tlc_why mrsafe; echo "FAIL -- the genuine-deadlock branch should reach a verdict (non-vacuous safety)"; fail=$((fail+1))
fi

"$(command -v safe-rm || echo rm)" -rf "$META"
echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
