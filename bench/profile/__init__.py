"""Profiling drivers for the pygo perf campaign.

These don't time anything themselves -- the external tool (perf, bpftrace,
cProfile, strace, valgrind) is the instrument.  run_workload.py provides a
single pygo workload as a clean process to attach to.
"""
