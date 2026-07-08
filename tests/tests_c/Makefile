# tests_c/Makefile -- pure-C bench harness for runloom_c
#
# Builds against the runloom_c .so already produced by `python3 setup.py
# build_ext --inplace`.  We link against the .so so the test harness
# exercises exactly the code path that Python sees.
#
# Targets:
#   bench_mn             release build (pthread echo server + runloom clients)
#   bench_mn-asan        AddressSanitizer (catches use-after-free, OOB)
#   bench_mn-tsan        ThreadSanitizer  (catches data races)
#   bench_mn-ubsan       UndefinedBehaviorSanitizer
#   bench_server_runloom    canonical scaling bench (server AND client in runloom)
#   bench_server_runloom-{asan,tsan,ubsan}
#   clean
#
# Default Python is 3.13t (free-threaded), since that's where the
# residual M:N races reproduce.  Override with PYTHON= on the command
# line for 3.12 etc.

PYTHON ?= /home/x/.pyenv/versions/3.14.4t/bin/python3
PY_VERSION  := $(shell $(PYTHON) -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_ABIFLAGS := $(shell $(PYTHON) -c 'import sys; print(sys.abiflags)')
PY_INC      := $(shell $(PYTHON) -c 'import sysconfig; print(sysconfig.get_path("include"))')
PY_LIBDIR   := $(shell $(PYTHON) -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')
PY_LIBNAME  := python$(PY_VERSION)$(PY_ABIFLAGS)

# runloom_c .so suffix matches the Python build.
RUNLOOM_SO_NAME := runloom_c.cpython-$(subst .,,$(PY_VERSION))$(PY_ABIFLAGS)-x86_64-linux-gnu.so
RUNLOOM_SO      := $(abspath ../../src/$(RUNLOOM_SO_NAME))
RUNLOOM_SO_DIR  := $(abspath ../../src)

CFLAGS_BASE := -g -O2 -Wall -Wextra -Wno-unused-parameter \
               -I$(PY_INC) -I../../src/runloom_c
# Link against the absolute path of the .so (so DT_NEEDED records the
# absolute path; otherwise the loader looks for the relative path
# verbatim from the program's CWD).  Plus an explicit RPATH for ldd.
LDFLAGS_BASE := -L$(PY_LIBDIR) -Wl,-rpath,$(PY_LIBDIR) -l$(PY_LIBNAME) \
                -pthread \
                -Wl,-rpath,$(RUNLOOM_SO_DIR) \
                -Wl,--no-as-needed $(RUNLOOM_SO)

# Make sure the .so exists before we try to link.
$(RUNLOOM_SO):
	$(PYTHON) -c 'import sys; sys.exit(0)'    # ensure interpreter usable
	cd .. && $(PYTHON) setup.py build_ext --inplace

bench_mn: bench_mn.c $(RUNLOOM_SO)
	$(CC) $(CFLAGS_BASE) bench_mn.c -o bench_mn $(LDFLAGS_BASE)

bench_mn-asan: bench_mn.c $(RUNLOOM_SO)
	$(CC) $(CFLAGS_BASE) -fsanitize=address bench_mn.c -o bench_mn-asan \
	    $(LDFLAGS_BASE) -fsanitize=address

bench_mn-tsan: bench_mn.c $(RUNLOOM_SO)
	$(CC) $(CFLAGS_BASE) -fsanitize=thread bench_mn.c -o bench_mn-tsan \
	    $(LDFLAGS_BASE) -fsanitize=thread

bench_mn-ubsan: bench_mn.c $(RUNLOOM_SO)
	$(CC) $(CFLAGS_BASE) -fsanitize=undefined bench_mn.c -o bench_mn-ubsan \
	    $(LDFLAGS_BASE) -fsanitize=undefined

# bench_server_runloom: both the echo server AND the client live in runloom
# goroutines (no pthread-per-conn ceiling).  This is the canonical
# scaling benchmark -- use it as the primary measurement going forward.
bench_server_runloom: bench_server_runloom.c $(RUNLOOM_SO)
	$(CC) $(CFLAGS_BASE) bench_server_runloom.c -o bench_server_runloom $(LDFLAGS_BASE)

bench_server_runloom-asan: bench_server_runloom.c $(RUNLOOM_SO)
	$(CC) $(CFLAGS_BASE) -fsanitize=address bench_server_runloom.c \
	    -o bench_server_runloom-asan $(LDFLAGS_BASE) -fsanitize=address

bench_server_runloom-tsan: bench_server_runloom.c $(RUNLOOM_SO)
	$(CC) $(CFLAGS_BASE) -fsanitize=thread bench_server_runloom.c \
	    -o bench_server_runloom-tsan $(LDFLAGS_BASE) -fsanitize=thread

bench_server_runloom-ubsan: bench_server_runloom.c $(RUNLOOM_SO)
	$(CC) $(CFLAGS_BASE) -fsanitize=undefined bench_server_runloom.c \
	    -o bench_server_runloom-ubsan $(LDFLAGS_BASE) -fsanitize=undefined

# test_cldeque: pure-C concurrency stress for the Chase-Lev work-stealing
# deque.  No Python -- compiles cldeque.c directly, so it builds + runs
# anywhere with a C compiler + pthreads.  Run the -tsan build under
# `setarch -R` on kernels with high-entropy ASLR (else TSan aborts with
# "unexpected memory mapping").  NOTE: cldeque.c's standalone SEQ_CST
# fence in steal() is not modelled by TSan (it warns); that ordering is
# covered exhaustively by verify/cbmc + verify/spin instead.
CLDEQUE_SRC  := test_cldeque.c ../../src/runloom_c/cldeque.c
CLDEQUE_CF   := -g -Wall -Wextra -Wno-unused-parameter -I../../src/runloom_c -pthread

test_cldeque: $(CLDEQUE_SRC)
	$(CC) -O2 $(CLDEQUE_CF) $(CLDEQUE_SRC) -o test_cldeque

test_cldeque-asan: $(CLDEQUE_SRC)
	$(CC) -O1 -fsanitize=address $(CLDEQUE_CF) $(CLDEQUE_SRC) -o test_cldeque-asan

test_cldeque-tsan: $(CLDEQUE_SRC)
	$(CC) -O1 -fsanitize=thread $(CLDEQUE_CF) $(CLDEQUE_SRC) -o test_cldeque-tsan

test_cldeque-ubsan: $(CLDEQUE_SRC)
	$(CC) -O1 -fsanitize=undefined $(CLDEQUE_CF) $(CLDEQUE_SRC) -o test_cldeque-ubsan

# rl_handle generation-stamped handle substrate (item 3).  test_rl_handle.c
# #includes rl_handle.c directly (its deps are header-inline), so no .so link.
RL_HANDLE_SRC := test_rl_handle.c
RL_HANDLE_CF  := -std=gnu11 -D_GNU_SOURCE -Wall -Wextra -Wno-unused-parameter -I../../src/runloom_c -pthread

test_rl_handle: $(RL_HANDLE_SRC)
	$(CC) -O2 $(RL_HANDLE_CF) $(RL_HANDLE_SRC) -o test_rl_handle
test_rl_handle-asan: $(RL_HANDLE_SRC)
	$(CC) -O1 -g -fsanitize=address $(RL_HANDLE_CF) $(RL_HANDLE_SRC) -o test_rl_handle-asan
test_rl_handle-tsan: $(RL_HANDLE_SRC)
	$(CC) -O1 -g -fsanitize=thread $(RL_HANDLE_CF) $(RL_HANDLE_SRC) -o test_rl_handle-tsan

# Barrier-synchronized race amplification (QA-steal: jcstress @Actor idiom).
# #includes rl_handle.c directly + links cldeque.c; drives both primitives
# through a reusable pthread_barrier harness.  Run under TSan (now fiber-correct)
# for a reliable per-primitive race finder.
BARRIER_SRC := test_barrier_race.c ../../src/runloom_c/cldeque.c
BARRIER_CF  := -std=gnu11 -D_GNU_SOURCE -Wall -Wextra -Wno-unused-parameter -I../../src/runloom_c -pthread

test_barrier_race: $(BARRIER_SRC)
	$(CC) -O2 $(BARRIER_CF) $(BARRIER_SRC) -o test_barrier_race
test_barrier_race-asan: $(BARRIER_SRC)
	$(CC) -O1 -g -fsanitize=address $(BARRIER_CF) $(BARRIER_SRC) -o test_barrier_race-asan
test_barrier_race-tsan: $(BARRIER_SRC)
	$(CC) -O1 -g -fsanitize=thread $(BARRIER_CF) $(BARRIER_SRC) -o test_barrier_race-tsan

clean:
	rm -f bench_mn bench_mn-asan bench_mn-tsan bench_mn-ubsan \
	      bench_server_runloom bench_server_runloom-asan \
	      bench_server_runloom-tsan bench_server_runloom-ubsan \
	      test_cldeque test_cldeque-asan test_cldeque-tsan test_cldeque-ubsan \
	      test_barrier_race test_barrier_race-asan test_barrier_race-tsan

.PHONY: clean
