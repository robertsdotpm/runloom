"""Async UDP DNS resolver + socket.getaddrinfo/gethostby* patches:
resolv.conf/hosts config, A/AAAA query, result cache."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .dns_proto import _DNS_PORT, _QTYPE_A, _QTYPE_AAAA, _build_query, _is_ip_literal, _parse_dns_answer  # noqa: F401

# ============================================================
# DNS -- pure async UDP resolver (no thread pool)
#
# Modeled on Go's `netgo` resolver: parse /etc/resolv.conf and
# /etc/hosts at first use, send UDP queries via the cooperatively
# patched socket layer (so recvfrom parks on wait_fd, not a thread),
# parse A/AAAA records.  Result cache amortises repeat lookups to
# microseconds.  A and AAAA queries fire in parallel goroutines so
# dual-stack hosts get both families in one round-trip.
# ============================================================

_DNS_TIMEOUT_S = 2.0
_DNS_CACHE_TTL = 60.0

_resolvers_cache = None
_hosts_cache     = None
_dns_result_cache = {}    # (lowername, qtype) -> (addrs, expire_ts)


def _resolv_conf_paths():
    """Candidate paths for resolver config, in order of preference.

    POSIX: /etc/resolv.conf is universal.
    Windows: no plain text equivalent (DNS settings live in the registry
        via GetNetworkParams); we return empty here and let the caller
        fall back to libc getaddrinfo via the backend pool.
    """
    if _IS_WINDOWS:
        return ()
    return ("/etc/resolv.conf",)


def _hosts_file_paths():
    """Candidate paths for the static hosts file."""
    if _IS_WINDOWS:
        # %SystemRoot% defaults to C:\Windows; SystemDrive is the C: part.
        sysroot = os.environ.get("SystemRoot", r"C:\Windows")
        return (os.path.join(sysroot, "System32", "drivers", "etc", "hosts"),)
    return ("/etc/hosts",)


def _read_small_file(path):
    """Read a config file without going through patched os.read."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return ""
    try:
        chunks = []
        while True:
            chunk = _raw_os_read(fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        try: os.close(fd)
        except OSError: pass
    try:
        return b"".join(chunks).decode("utf-8", "replace")
    except Exception:
        return ""


def _load_resolvers():
    """Return list of nameserver IPs.  Empty list -> no usable config;
    the resolver will fall back to libc getaddrinfo via the backend."""
    nss = []
    for path in _resolv_conf_paths():
        for line in _read_small_file(path).splitlines():
            line = line.split("#", 1)[0].split(";", 1)[0].strip()
            if line.startswith("nameserver"):
                parts = line.split()
                if len(parts) >= 2:
                    nss.append(parts[1])
        if nss:
            break
    return nss


def _load_hosts():
    hosts = {}
    for path in _hosts_file_paths():
        text = _read_small_file(path)
        if not text:
            continue
        for line in text.splitlines():
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            addr = parts[0]
            for nm in parts[1:]:
                hosts.setdefault(nm.lower(), []).append(addr)
        if hosts:
            break
    return hosts


def _get_resolvers():
    global _resolvers_cache
    if _resolvers_cache is None:
        _resolvers_cache = _load_resolvers()
    return _resolvers_cache


def _get_hosts():
    global _hosts_cache
    if _hosts_cache is None:
        _hosts_cache = _load_hosts()
    return _hosts_cache



def _query_nameserver(packet, txn, ns, timeout):
    """Single UDP round trip.  Uses cooperatively patched socket."""
    af = _is_ip_literal(ns)
    if af is None:
        raise OSError("non-IP nameserver: " + ns)
    s = socket.socket(af, socket.SOCK_DGRAM)
    try:
        s.settimeout(timeout)
        s.sendto(packet, (ns, _DNS_PORT))
        data, _ = s.recvfrom(4096)
        return _parse_dns_answer(data, txn)
    finally:
        s.close()


def _resolve_via_libc(name, qtype):
    """Fall back to the platform getaddrinfo, dispatched through the
    blocking-call backend so other goroutines keep running while libc's
    blocking resolver is in flight.  Used when we have no usable
    /etc/resolv.conf (Windows; chrooted POSIX without DNS config)."""
    af = socket.AF_INET if qtype == _QTYPE_A else socket.AF_INET6
    try:
        infos = _blocking_call(_orig_getaddrinfo, name, 0, af,
                               socket.SOCK_STREAM, 0, 0)
    except socket.gaierror:
        return []
    addrs = []
    for info in infos:
        sa = info[4]
        addrs.append(sa[0])
    return addrs


def _resolve_qtype(name, qtype):
    """Resolve one query type with cache + nameserver fall-through.

    Falls back to libc getaddrinfo (via backend pool) when no resolver
    config is available -- the Windows case, where DNS settings live in
    the registry rather than in /etc/resolv.conf."""
    key = (name.lower(), qtype)
    now = time.monotonic()
    cached = _dns_result_cache.get(key)
    if cached is not None and cached[1] > now:
        return cached[0]
    resolvers = _get_resolvers()
    if not resolvers:
        addrs = _resolve_via_libc(name, qtype)
        _dns_result_cache[key] = (addrs, now + _DNS_CACHE_TTL)
        return addrs
    txn, packet = _build_query(name, qtype)
    last_err = None
    for ns in resolvers:
        try:
            addrs = _query_nameserver(packet, txn, ns, _DNS_TIMEOUT_S)
            _dns_result_cache[key] = (addrs, now + _DNS_CACHE_TTL)
            return addrs
        except (OSError, socket.timeout) as e:
            last_err = e
            continue
    # All configured nameservers failed -- try libc as a last resort
    # rather than surfacing the per-server error, which is usually
    # more confusing than just answering through the OS.
    addrs = _resolve_via_libc(name, qtype)
    if addrs:
        _dns_result_cache[key] = (addrs, now + _DNS_CACHE_TTL)
        return addrs
    if last_err is not None:
        raise last_err
    raise OSError("no DNS nameservers")


def _resolve_dual(name, want_v4, want_v6):
    """Concurrent A + AAAA queries when both wanted."""
    if want_v4 and not want_v6:
        return [(socket.AF_INET,  a) for a in _resolve_qtype(name, _QTYPE_A)]
    if want_v6 and not want_v4:
        return [(socket.AF_INET6, a) for a in _resolve_qtype(name, _QTYPE_AAAA)]
    # Both -- fire in parallel goroutines, gather via Parker
    results = [None, None]
    parker = _Parker()
    remaining = [2]
    def runner(idx, qtype):
        try:
            results[idx] = _resolve_qtype(name, qtype)
        except Exception:
            results[idx] = []
        remaining[0] -= 1
        if remaining[0] == 0:
            parker.unpark()
    runloom_c.go(lambda: runner(0, _QTYPE_A))
    runloom_c.go(lambda: runner(1, _QTYPE_AAAA))
    parker.park()
    parker.release()
    out = []
    for a in results[0] or ():
        out.append((socket.AF_INET, a))
    for a in results[1] or ():
        out.append((socket.AF_INET6, a))
    return out


_orig_getaddrinfo      = None
_orig_gethostbyname    = None
_orig_gethostbyname_ex = None
_orig_getnameinfo      = None
_orig_gethostbyaddr    = None


def _af_wanted(family, candidate):
    return family == 0 or family == candidate or family == socket.AF_UNSPEC


def _patched_getaddrinfo(host, port=0, family=0, type=0, proto=0, flags=0):
    # Numeric-port-only path: hand "service name" lookups to libc by
    # offloading the full call (rare in normal apps).
    if isinstance(port, str):
        try:
            port = int(port)
        except ValueError:
            return _blocking_call(_orig_getaddrinfo,
                                  host, port, family, type, proto, flags)

    if host is None or host == "":
        host = "::" if family == socket.AF_INET6 else "0.0.0.0"

    # IP literal -- no DNS round trip.
    lit_af = _is_ip_literal(host)
    if lit_af is not None:
        if not _af_wanted(family, lit_af):
            raise socket.gaierror(socket.EAI_FAMILY,
                                  "Address family mismatch")
        pairs = [(lit_af, host)]
    else:
        # AI_NUMERICHOST: the host must already be a numeric address; libc
        # performs NO name resolution (no DNS, no /etc/hosts) and returns
        # EAI_NONAME immediately.  Match that -- otherwise a caller using the
        # flag as a cheap "is this a literal IP?" check issues a real query
        # that hangs offline instead of fast-failing.
        if flags & socket.AI_NUMERICHOST:
            raise socket.gaierror(socket.EAI_NONAME,
                                  "Name or service not known")
        # /etc/hosts -- skip DNS if we have a static entry.
        hosts = _get_hosts()
        local = hosts.get(host.lower())
        if local is not None:
            pairs = []
            for a in local:
                aaf = _is_ip_literal(a)
                if aaf is None:
                    continue
                if _af_wanted(family, aaf):
                    pairs.append((aaf, a))
        else:
            want_v4 = _af_wanted(family, socket.AF_INET)
            want_v6 = _af_wanted(family, socket.AF_INET6)
            try:
                pairs = _resolve_dual(host, want_v4, want_v6)
            except OSError as e:
                raise socket.gaierror(socket.EAI_NONAME, str(e))
            if not pairs:
                raise socket.gaierror(socket.EAI_NONAME,
                                      "Name or service not known")

    st = type if type else socket.SOCK_STREAM
    result = []
    for aaf, a in pairs:
        if aaf == socket.AF_INET6:
            sa = (a, port, 0, 0)
        else:
            sa = (a, port)
        result.append((aaf, st, proto, "", sa))
    return result


def _patched_gethostbyname(name):
    infos = _patched_getaddrinfo(name, 0, socket.AF_INET)
    return infos[0][4][0]


def _patched_gethostbyname_ex(name):
    infos = _patched_getaddrinfo(name, 0, socket.AF_INET)
    addrs = [info[4][0] for info in infos]
    return (name, [], addrs)


def _patched_getnameinfo(*args, **kw):
    # Reverse lookup -- not worth re-implementing for v0.  Off-thread.
    return _blocking_call(_orig_getnameinfo, *args, **kw)


def _patched_gethostbyaddr(*args, **kw):
    return _blocking_call(_orig_gethostbyaddr, *args, **kw)


def _patch_dns():
    global _orig_getaddrinfo, _orig_gethostbyname, _orig_gethostbyname_ex
    global _orig_getnameinfo, _orig_gethostbyaddr
    _orig_getaddrinfo      = socket.getaddrinfo
    _orig_gethostbyname    = socket.gethostbyname
    _orig_gethostbyname_ex = socket.gethostbyname_ex
    _orig_getnameinfo      = socket.getnameinfo
    _orig_gethostbyaddr    = socket.gethostbyaddr
    socket.getaddrinfo      = _patched_getaddrinfo
    socket.gethostbyname    = _patched_gethostbyname
    socket.gethostbyname_ex = _patched_gethostbyname_ex
    socket.getnameinfo      = _patched_getnameinfo
    socket.gethostbyaddr    = _patched_gethostbyaddr


def _unpatch_dns():
    socket.getaddrinfo      = _orig_getaddrinfo
    socket.gethostbyname    = _orig_gethostbyname
    socket.gethostbyname_ex = _orig_gethostbyname_ex
    socket.getnameinfo      = _orig_getnameinfo
    socket.gethostbyaddr    = _orig_gethostbyaddr
