runloom supply-chain / backdoor scan
====================================

Complements the `security` phase (which fuzzes the RUNTIME).  This phase looks for
a BACKDOOR planted in the source tree or a compromised dependency, using four OSS
tools -- each SELF-SKIPPING if not installed, each tuned GREEN on the clean tree so
a NEW finding reds the gate:

  semgrep      backdoor-SHAPE rules (semgrep_backdoor.yml): obfuscated exec
               (base64/marshal/zlib -> exec/eval), fetch-and-run, hardcoded public
               C2 IP + connect, network calls in setup.py, C system()/dlopen of a
               computed string.  C + Python.  OFFLINE.
  gitleaks     planted secret / C2 credential / private key in the working tree,
               minus confirmed FPs (gitleaks.toml allowlist).  OFFLINE.
  bandit       Python security AST patterns in src/, NEW vs bandit_baseline.json
               (the current legit exec/subprocess uses are baselined).  OFFLINE.
  osv-scanner  vulnerable / malicious dependencies vs the OSV DB, on the resolved
               dev/test env.  NETWORK -- opt-in (RUNLOOM_SC_DEPS=1).

Run
---
  tools/supplychain/scan.sh                    # offline 3 + (dep audit off)
  RUNLOOM_SC_DEPS=1 tools/supplychain/scan.sh  # + osv-scanner (network)
  RUNLOOM_SC_FAST=1 tools/supplychain/scan.sh  # offline subset only

Wired into check_all as the `supplychain` phase (extensive, with the dep audit) and
`supplychain-fast` (offline subset, in check_all_fast).  Any tool absent -> that
line is SKIPped with an install hint; the phase never hard-fails on a missing tool.

Install the scanners
--------------------
  # Python tools (any standard python3; they scan text, not the 3.14t runtime):
  python3 -m pip install --user semgrep bandit
  # Go single-binaries (this repo already ships Go):
  go install github.com/zricethezav/gitleaks/v8@latest
  go install github.com/google/osv-scanner/cmd/osv-scanner@latest
  # scan.sh adds ~/.local/bin and $(go env GOPATH)/bin to PATH automatically.

Updating a baseline after a legitimately-new finding
----------------------------------------------------
  bandit:   regenerate ->
            bandit -r src/ -q -ll -f json -o tools/supplychain/bandit_baseline.json
  gitleaks: add a regex/path to the [allowlist] in gitleaks.toml (with a reason)
  semgrep:  add a '# nosemgrep: <rule-id>  <reason>' comment at the flagged line

Optional deeper (noisier) scan, not gated
------------------------------------------
  semgrep --config p/security-audit --config p/secrets src/ tools/   # community rules
  guarddog pypi verify <pkg>                                         # malicious-package heuristics
  capa <built .so>                                                   # capability detection on the artifact

Limits (be honest)
------------------
Pattern scanners catch KNOWN-SHAPED backdoors (exec/exfil/obfuscation/planted-creds/
known-bad-deps).  A subtle LOGIC backdoor (a weakened bounds check, an off-by-one
that leaks) is caught only by diff review + the adversarial testing the rest of
check_all already does (linz, GenMC, sanitizers, differential oracles).  This is a
net, not a guarantee.
