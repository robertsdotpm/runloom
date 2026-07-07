"""Server-list loader + opt-in gate + finding emitter for the remote suite.

Importing this module performs NO network I/O (the fetch happens only inside
load()), so accidental test collection can never touch the wire.  Three things
live here:

  * enabled()  -- the opt-in gate.  The whole suite is a no-op unless the
                  environment sets RUNLOOM_NET_TESTS=1.  Every program calls it
                  first and SKIPs before any scheduler/network start otherwise.
  * load()/servers_for() -- fetch the user's curated list at
                  http://ovh1.p2pd.net:8000/servers (plain HTTP, port 8000), with
                  a tiny PINNED fallback so a dead list-host (a known SPOF) only
                  DEGRADES coverage, never fails the run.  Score-ordered.
  * write_finding() -- emit a hang_hunter-format finding file so the daemon's
                  existing inbox loop picks it up unchanged.  ENV skips write
                  NOTHING, so an all-skip night is a clean no-op.
"""
import json
import os

# Verdict / exit codes (shared by every n0* program and run_all_net.py).
PASS = 0
FINDING = 1        # identity-matched corrupted response -> real transport bug
CRASH = 2          # pygo-side non-OSError exception / abnormal exit
HANG = 3           # watchdog fired -> lost wake on the real netpoll path
SKIP = 77          # ENV: all servers down / no route / list-host down / opt-out

SERVERS_URL = "http://ovh1.p2pd.net:8000/servers"

# Category / address-family / protocol keys, exactly as the endpoint returns them.
CAT_STUN = "STUN(see_ip)"
CAT_STUN_NAT = "STUN(test_nat)"
CAT_MQTT = "MQTT"
CAT_TURN = "TURN"
CAT_NTP = "NTP"


def enabled():
    """The opt-in gate: True only when RUNLOOM_NET_TESTS=1.  The suite is inert
    (never touches the network, never starts a scheduler) unless this holds."""
    return os.environ.get("RUNLOOM_NET_TESTS") == "1"


# A tiny PINNED fallback keyed [category][af][proto] -> list of (host, port).
# Hosts are FQDNs (resolved at connect time) so a rotated IP self-heals.  This
# is a floor, not a substitute: if the list-host is down AND every pinned server
# is unreachable, the program SKIPs -- it can never manufacture a finding.
PINNED = {
    CAT_STUN: {
        "IPv4": {"TCP": [("stun.annatel.net", 3478), ("stun.axialys.net", 3478)],
                 "UDP": [("stun.l.google.com", 19302), ("stun.annatel.net", 3478)]},
        "IPv6": {"TCP": [("stun.chatous.com", 3478), ("stun.tula.nu", 3478)],
                 "UDP": [("stun.chatous.com", 3478)]},
    },
    CAT_STUN_NAT: {
        "IPv4": {"UDP": [("stun.l.google.com", 19302), ("stun.axialys.net", 3478)]},
        "IPv6": {"UDP": []},
    },
    CAT_NTP: {
        "IPv4": {"UDP": [("time.cloudflare.com", 123), ("pool.ntp.org", 123),
                         ("time.google.com", 123)]},
        "IPv6": {"UDP": [("time.cloudflare.com", 123), ("time.google.com", 123)]},
    },
    CAT_MQTT: {
        "IPv4": {"TCP": [("test.mosquitto.org", 1883), ("broker.emqx.io", 1883)]},
        "IPv6": {"TCP": []},
    },
}


def _pinned(category, af, proto):
    entries = PINNED.get(category, {}).get(af, {}).get(proto, [])
    return [{"ip": h, "port": p, "fqns": [h], "user": None, "password": None,
             "score": 0.0, "pinned": True} for (h, p) in entries]


def load(timeout=4.0, log=None):
    """Fetch the server list; return the parsed dict, or None on ANY failure
    (unreachable SPOF, timeout, non-200, bad JSON).  Never raises.  When it
    returns None, callers fall back to _pinned()."""
    try:
        import urllib.request
        with urllib.request.urlopen(SERVERS_URL, timeout=timeout) as r:
            if getattr(r, "status", 200) not in (200, None):
                raise OSError("http %s" % r.status)
            data = json.loads(r.read().decode("utf-8", "replace"))
        if not isinstance(data, dict):
            raise ValueError("top-level not a dict")
        return data
    except Exception as exc:                       # noqa: BLE001 - degrade, never fail
        if log:
            log("list-host down (%s: %s) -- using pinned fallback"
                % (type(exc).__name__, exc))
        return None


def servers_for(data, category, af, proto, top=32, log=None):
    """Return up to `top` normalized server dicts {ip,port,fqns,user,password,
    score} for (category, af, proto), highest score first.  Always appends the
    PINNED entries as a floor (deduped by ip:port), so even a partial/empty list
    still has something to try.  `data` may be None (list-host down) -> pinned
    only."""
    out = []
    seen = set()

    def add(entry):
        key = (str(entry.get("ip")), entry.get("port"))
        if key in seen or not entry.get("ip") or not entry.get("port"):
            return
        seen.add(key)
        out.append({"ip": entry["ip"], "port": entry["port"],
                    "fqns": entry.get("fqns") or [],
                    "user": entry.get("user"), "password": entry.get("password"),
                    "score": entry.get("score") or 0.0,
                    "pinned": entry.get("pinned", False)})

    if isinstance(data, dict):
        try:
            raw = data[category][af][proto]
        except (KeyError, TypeError):
            raw = []
        # Each inner element is a 1-element list [{...}]; flatten defensively.
        flat = []
        for item in raw:
            if isinstance(item, list) and item:
                flat.append(item[0])
            elif isinstance(item, dict):
                flat.append(item)
        flat.sort(key=lambda e: -(e.get("score") or 0.0))
        for e in flat:
            add(e)
    for e in _pinned(category, af, proto):
        add(e)
    if log and not out:
        log("no servers for %s/%s/%s (list + pinned both empty)"
            % (category, af, proto))
    return out[:top]


def write_finding(report_dir, kind, key, text):
    """Write a hang_hunter-format finding file under report_dir/findings/ so the
    duty_cycle inbox loop ingests it unchanged.  `kind` is the inbox category
    (net-protocol / net-crash / net-hang); `key` dedups recurring findings."""
    import hashlib
    fdir = os.path.join(report_dir, "findings")
    os.makedirs(fdir, exist_ok=True)
    sig = hashlib.sha1(("%s|%s" % (kind, key)).encode()).hexdigest()[:12]
    path = os.path.join(fdir, "%s_%s.txt" % (kind, sig))
    with open(path, "w") as f:
        f.write("KIND: %s\n" % kind)
        f.write("KEY: %s\n" % key)
        f.write("\n")
        f.write(text)
        if not text.endswith("\n"):
            f.write("\n")
    return path


def _selftest():
    # Offline: pinned fallback works with data=None, score-orders, dedups.
    s = servers_for(None, CAT_STUN, "IPv4", "TCP", top=32)
    assert s and all(e["port"] == 3478 for e in s), s
    assert all(e["pinned"] for e in s)
    # A synthetic list is score-ordered above pinned and deduped.
    fake = {CAT_NTP: {"IPv4": {"UDP": [
        [{"ip": "1.2.3.4", "port": 123, "score": 0.5, "fqns": ["a"]}],
        [{"ip": "5.6.7.8", "port": 123, "score": 0.9, "fqns": ["b"]}],
    ]}}}
    r = servers_for(fake, CAT_NTP, "IPv4", "UDP", top=32)
    assert r[0]["ip"] == "5.6.7.8" and r[1]["ip"] == "1.2.3.4", r  # score desc
    assert any(e["pinned"] for e in r)                              # pinned floor
    # write_finding emits a hang_hunter-format file.
    import tempfile
    d = tempfile.mkdtemp(prefix="nettest_")
    p = write_finding(d, "net-protocol", "n01|1.2.3.4|txid-corrupt", "body")
    txt = open(p).read()
    assert txt.startswith("KIND: net-protocol\nKEY: n01|") and txt.endswith("body\n")
    import shutil
    shutil.rmtree(d, ignore_errors=True)
    print("netlist.py self-test: OK")


if __name__ == "__main__":
    _selftest()
