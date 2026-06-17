#!/usr/bin/env bash
# build_patched_rr.sh -- build + install rr with the vPMU min-period clamp so it
# records on this VMware vPMU.  The vPMU rejects sample_period < 32 (EINVAL); rr's
# self-check uses period=1, so stock rr aborts.  The patch (tools/rr_vpmu_min_
# period.patch) floors the period at 32 -- safe because the retired-branch count
# is exactly reproducible here (verified, delta=0/12 runs) and rr single-steps the
# sub-skid remainder.  Full rationale + validation in docs/dev/rr_vpmu_status.md.
#
# Validated: records + replays single-threaded, multi-threaded (contended locks),
# and a runloom goroutine workload (epoll backend) with ZERO divergence.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SRC="${RR_SRC:-$HOME/projects/rr-src}"
PATCH="$HERE/rr_vpmu_min_period.patch"

echo "=== build deps ==="
sudo -n apt-get install -y -q cmake g++ pkg-config zlib1g-dev libcapnp-dev capnproto 2>&1 | tail -1

if [ ! -d "$SRC/.git" ]; then
  git clone --depth 1 --branch 5.7.0 https://github.com/rr-debugger/rr.git "$SRC" || exit 1
fi
cd "$SRC" || exit 1
git checkout -- src/PerfCounters.cc 2>/dev/null
echo "=== apply vPMU min-period patch ==="
git apply --check "$PATCH" 2>/dev/null && git apply "$PATCH" || { echo "patch did not apply cleanly"; exit 1; }

mkdir -p obj && cd obj
echo "=== cmake + make (-Ddisable32bit avoids multilib) ==="
cmake -DCMAKE_BUILD_TYPE=Release -Ddisable32bit=ON .. >/dev/null 2>&1 || { echo "cmake failed"; exit 1; }
make -j"$(nproc)" || { echo "make failed"; exit 1; }
echo "=== install (/usr/local/bin/rr shadows the unpatched system rr) ==="
sudo -n make install >/dev/null 2>&1

echo "=== verify ==="
which rr; rr --version | head -1
rr record -n /bin/true >/dev/null 2>&1 && echo "OK: patched rr records on this vPMU" \
    || { echo "FAIL: rr still cannot record -- re-run tools/rr_vpmu_probe to check the vPMU minimum period"; exit 1; }
