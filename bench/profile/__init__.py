"""Profiling drivers for the runloom perf campaign.

These don't time anything themselves -- the external tool (perf, bpftrace,
cProfile, strace, valgrind) is the instrument.  run_workload.py provides a
single runloom workload as a clean process to attach to.
"""
