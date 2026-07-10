#!/usr/bin/env bash
# scan.sh -- supply-chain / backdoor scan of the runloom source tree.
#
# Complements the existing `security` phase (which fuzzes the RUNTIME): this looks
# for a BACKDOOR planted in the tree or a compromised dependency.  Four OSS tools,
# each SELF-SKIPPING if not installed (like the infer/valgrind phases), each tuned
# to be GREEN on the clean tree so a NEW finding reds the gate:
#
#   semgrep      backdoor-SHAPE rules (obfuscated exec, fetch-and-run, C2 IP,
#                setup.py network exfil, C system()/dlopen of a computed string) --
#                custom low-FP ruleset (semgrep_backdoor.yml), C + Python. OFFLINE.
#   gitleaks     planted secret / C2 credential / private key in the working tree,
#                minus confirmed FPs (gitleaks.toml allowlist). OFFLINE.
#   bandit       Python security AST patterns in src/, NEW vs bandit_baseline.json
#                (the current legit exec/subprocess uses are baselined). OFFLINE.
#   osv-scanner  known-vulnerable / malicious dependencies vs the OSV DB, on the
#                resolved dev/test env. NETWORK -- opt-in via RUNLOOM_SC_DEPS=1.
#
# NOT a guarantee: pattern scanners catch KNOWN-SHAPED backdoors (exec/exfil/
# obfuscation/planted-creds/known-bad-deps).  A subtle LOGIC backdoor (a weakened
# check, an off-by-one that leaks) is only caught by diff review + the adversarial
# testing the rest of check_all already does.
#
# Env:
#   RUNLOOM_SC_FAST=1   offline subset only (semgrep+gitleaks+bandit); no dep audit.
#   RUNLOOM_SC_DEPS=1   also run osv-scanner (needs network).
#   RUNLOOM_PYTHON=...  interpreter whose env's deps to audit (default 3.14.4t).
#
# Updating a baseline after a legitimately-new finding:
#   bandit:   bandit -r src/ -q -ll -f json -o tools/supplychain/bandit_baseline.json
#   gitleaks: add a regex/path to the [allowlist] in tools/supplychain/gitleaks.toml
set +e
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
cd "$ROOT" || exit 9

# Make user-installed (pip --user) and go-installed scanners findable.
export PATH="$HOME/.local/bin:$(go env GOPATH 2>/dev/null)/bin:$PATH"

PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
FAST="${RUNLOOM_SC_FAST:-0}"
DEPS="${RUNLOOM_SC_DEPS:-0}"
[ "$FAST" = 1 ] && DEPS=0

rc=0; ran=0; skipped=0
note() { printf '  %s\n' "$*"; }
hr()   { printf '\n== %s ==\n' "$1"; }
skip_tool() { note "SKIP $1 -- not installed (install: $2)"; skipped=$((skipped + 1)); }

# ---- 1. semgrep: custom backdoor-shape rules (offline) ------------------
hr "semgrep -- backdoor-pattern rules (src/ setup.py tools/)"
if command -v semgrep >/dev/null 2>&1; then
  ran=$((ran + 1))
  if semgrep --config "$HERE/semgrep_backdoor.yml" --error --quiet \
             --metrics off --disable-version-check src/ setup.py tools/; then
    note "OK  no backdoor-shape matches"
  else
    note ">>> semgrep flagged a backdoor shape (above) -- explain it, or add a"
    note "    '# nosemgrep: <rule-id>  <reason>' comment if it is a legitimate case"
    rc=1
  fi
else
  skip_tool semgrep "python3 -m pip install --user semgrep"
fi

# ---- 2. gitleaks: planted secrets in the working tree (offline) --------
hr "gitleaks -- planted secrets / credentials (working tree)"
if command -v gitleaks >/dev/null 2>&1; then
  ran=$((ran + 1))
  if gitleaks detect --no-git --source . --config "$HERE/gitleaks.toml" \
              --redact --no-banner >/dev/null 2>&1; then
    note "OK  no new secrets"
  else
    note ">>> gitleaks found a secret.  Reproduce:"
    note "    gitleaks detect --no-git --config tools/supplychain/gitleaks.toml -v"
    note "    -> a planted credential, or a new FP to allowlist in gitleaks.toml"
    rc=1
  fi
else
  skip_tool gitleaks "go install github.com/zricethezav/gitleaks/v8@latest"
fi

# ---- 3. bandit: Python patterns vs baseline (offline) ------------------
hr "bandit -- Python security patterns in src/ (NEW vs baseline)"
if command -v bandit >/dev/null 2>&1; then
  ran=$((ran + 1))
  if bandit -r src/ -q -ll -b "$HERE/bandit_baseline.json" >/dev/null 2>&1; then
    note "OK  no new findings vs baseline"
  else
    note ">>> bandit found a NEW issue not in the baseline.  Reproduce:"
    note "    bandit -r src/ -ll -b tools/supplychain/bandit_baseline.json"
    note "    -> review; if legitimate, regenerate the baseline (see header)"
    rc=1
  fi
else
  skip_tool bandit "python3 -m pip install --user bandit"
fi

# ---- 4. osv-scanner: dependency advisories (network, opt-in) -----------
if [ "$DEPS" = 1 ]; then
  hr "osv-scanner -- vulnerable/malicious dependencies (OSV DB, network)"
  if command -v osv-scanner >/dev/null 2>&1; then
    ran=$((ran + 1))
    REQ="$(mktemp)"
    "$PY" -m pip freeze 2>/dev/null | grep -viE "^-e |^runloom" > "$REQ"
    ndeps="$(wc -l < "$REQ")"
    out="$(osv-scanner scan --lockfile="requirements.txt:$REQ" 2>&1)"; osvrc=$?
    if [ "$osvrc" = 0 ]; then
      note "OK  no advisories for $ndeps resolved deps"
    elif [ "$osvrc" = 1 ]; then
      printf '%s\n' "$out" | tail -20
      note ">>> osv-scanner reported a dependency advisory (above)"
      rc=1
    else
      note "SKIP osv-scanner -- scan error (network?), rc=$osvrc; not failing the gate"
      ran=$((ran - 1)); skipped=$((skipped + 1))
    fi
    rm -f "$REQ"
  else
    skip_tool osv-scanner "go install github.com/google/osv-scanner/cmd/osv-scanner@latest"
  fi
else
  note "(dep audit off -- set RUNLOOM_SC_DEPS=1 for the osv-scanner network audit)"
fi

hr "supply-chain scan: $ran tool(s) ran, $skipped skipped, rc=$rc"
exit $rc
