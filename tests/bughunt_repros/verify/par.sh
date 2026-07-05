#!/bin/bash
PY=/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo/.venv/bin/python
REPRO=/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/repros/verify/repro.py
BATCHES=${1:-10}
PAR=${2:-16}
miss=0; hit=0
for b in $(seq 1 $BATCHES); do
  pids=()
  for j in $(seq 1 $PAR); do
    ( out=$(RUNLOOM_DEADLOCK=raise timeout 8 $PY $REPRO 2>&1); code=$?
      if echo "$out" | grep -q "deadlock"; then exit 0; else echo "MISS batch=$b j=$j code=$code out=[$(echo "$out" | head -3 | tr -d '\0')]"; exit 1; fi ) &
    pids+=($!)
  done
  for p in "${pids[@]}"; do if wait $p; then hit=$((hit+1)); else miss=$((miss+1)); fi; done
done
echo "hits=$hit misses=$miss"
