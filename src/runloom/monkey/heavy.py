"""Size-gated auto-offload of CPU-bound stdlib C calls."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ============================================================
# heavy -- size-gated auto-offload of CPU-bound stdlib C calls
#
# hashlib.sha*/md5/blake2 + zlib/gzip/bz2/lzma compress/decompress burn CPU in
# a tight C loop with no yield point: a fiber can't hand off mid-sha256, so
# it pins the scheduler thread until the call returns (and -- being a C loop
# with no Python frames -- the sysmon preemptor can't interrupt it either).
# These can't be made cooperative, only RELOCATED: run them on the backend pool
# so the fiber parks and its siblings keep running (and under free-threaded
# 3.13t the offload is real parallelism).
#
# The catch is overhead vs benefit: offloading a 32-byte hash is pure loss.  So
# auto-offload only kicks in above a size threshold (RUNLOOM_OFFLOAD_BYTES, default
# 256 KiB) -- small payloads run inline with just a len()+compare; big ones
# offload.  KDFs (pbkdf2_hmac / scrypt) are *always* heavy (cost is iterations,
# not size) so they always offload.  This is the answer to "developers forget
# to wrap the heavy ones": for the common, size-measurable offenders they don't
# have to -- it just happens.  runloom.monkey.offload() remains for everything else
# (numpy, Pillow, C DB drivers, ...) that isn't size-measurable up front.
#
# Patches module-level functions (assignable), not the immutable hash/compressor
# types; streaming .update()/.compress() on a live object isn't auto-offloaded
# (use offload() for a huge single chunk).  Patch early -- `from hashlib import
# sha256` before patch() keeps the original.
# ============================================================
def _heavy_threshold():
    try:
        return int(os.environ.get("RUNLOOM_OFFLOAD_BYTES", 256 * 1024))
    except (ValueError, TypeError):
        return 256 * 1024


_HEAVY_THRESHOLD = _heavy_threshold()

# (module, func, data_index, data_kwnames, always)
_HEAVY_TABLE = (
    ("hashlib", "sha1",     0, ("string", "data"), False),
    ("hashlib", "sha224",   0, ("string", "data"), False),
    ("hashlib", "sha256",   0, ("string", "data"), False),
    ("hashlib", "sha384",   0, ("string", "data"), False),
    ("hashlib", "sha512",   0, ("string", "data"), False),
    ("hashlib", "sha3_256", 0, ("string", "data"), False),
    ("hashlib", "sha3_512", 0, ("string", "data"), False),
    ("hashlib", "md5",      0, ("string", "data"), False),
    ("hashlib", "blake2b",  0, ("string", "data"), False),
    ("hashlib", "blake2s",  0, ("string", "data"), False),
    ("hashlib", "new",      1, ("data", "string"), False),
    ("hashlib", "pbkdf2_hmac", 0, (), True),      # KDF: cost is iterations
    ("hashlib", "scrypt",      0, (), True),      # KDF
    ("zlib", "compress",   0, ("data",), False),
    ("zlib", "decompress", 0, ("data",), False),
    ("gzip", "compress",   0, ("data",), False),
    ("gzip", "decompress", 0, ("data",), False),
    ("bz2", "compress",    0, ("data",), False),
    ("bz2", "decompress",  0, ("data",), False),
    ("lzma", "compress",   0, ("data",), False),
    ("lzma", "decompress", 0, ("data",), False),
)

_orig_heavy = {}      # (module_name, func_name) -> original callable


def _heavy_len(x):
    try:
        return len(x)
    except TypeError:
        return None


def _make_heavy(orig, data_index, data_kwnames, always):
    def wrapper(*args, **kwargs):
        if not _in_fiber():
            return orig(*args, **kwargs)
        if always:
            return _blocking_call(orig, *args, **kwargs)
        if len(args) > data_index:
            data = args[data_index]
        else:
            data = None
            for kw in data_kwnames:
                if kw in kwargs:
                    data = kwargs[kw]
                    break
        n = _heavy_len(data)
        if n is not None and n >= _HEAVY_THRESHOLD:
            return _blocking_call(orig, *args, **kwargs)   # offload the big one
        return orig(*args, **kwargs)                       # small -> inline
    wrapper.__name__ = getattr(orig, "__name__", "heavy")
    wrapper.__qualname__ = getattr(orig, "__qualname__", wrapper.__name__)
    wrapper.__doc__ = getattr(orig, "__doc__", None)
    wrapper.__wrapped__ = orig          # so inspect.unwrap() / tests can see it
    wrapper.__runloom_heavy__ = True       # marker for tests / introspection
    return wrapper


def _patch_heavy():
    global _HEAVY_THRESHOLD
    _HEAVY_THRESHOLD = _heavy_threshold()
    seen = set()
    for modname, func, didx, dkw, always in _HEAVY_TABLE:
        if modname in seen:
            mod = sys.modules.get(modname)
        else:
            seen.add(modname)
            try:
                mod = __import__(modname)
            except ImportError:
                mod = None
        if mod is None or not hasattr(mod, func):
            continue
        orig = getattr(mod, func)
        if not callable(orig):
            continue
        _orig_heavy[(modname, func)] = orig
        setattr(mod, func, _make_heavy(orig, didx, dkw, always))


def _unpatch_heavy():
    for (modname, func), orig in list(_orig_heavy.items()):
        mod = sys.modules.get(modname)
        if mod is not None:
            setattr(mod, func, orig)
    _orig_heavy.clear()


# ============================================================
# compile -- offload the parse/compile of a fiber's source
#
# compile() (and ast.parse(), which calls builtins.compile) recurse one C-stack
# frame per nesting level of the SOURCE, ~1.5 KB/level -- enough to overflow a
# fiber's 32 KB C stack past ~18-deep nesting (a guard-page SEGV), and it
# happens BEFORE CPython's recursion counter (sized for the 8 MB main stack)
# can fire a clean RecursionError.  This is the one C-recursion the stdlib has
# that crashes a fiber rather than raising: json/pickle/marshal/deepcopy
# cost only ~60-80 B/level, so their counter fires first and they stay safe (see
# docs/cooperative_stdlib_coverage.md).  compile is pure -- source in, code
# object out, no thread affinity -- so relocate it (like the heavy table above)
# to the backend pool's full-size thread stack when called inside a fiber:
# the deep recursion runs where it fits and the fiber parks.
#
# Cheap: compiles overwhelmingly happen at import on the MAIN thread, where
# _in_fiber() is false -> straight passthrough; only an in-fiber compile
# takes the pool round-trip.  Covers builtins.compile directly, ast.parse()
# (bare `compile` -> builtins), and source imports that compile via the builtin.
# NOT covered: eval(str)/exec(str), which compile internally in C (not via
# builtins.compile) and need the caller's namespace -- a fiber that evals/
# execs deeply-nested untrusted source should use runloom.monkey.offload() (or a
# roomier g-stack via runloom_c.go(fn, stack_size=...)).
# ============================================================
_orig_compile = None


def _patched_compile(*args, **kwargs):
    if not _in_fiber():
        return _orig_compile(*args, **kwargs)
    return _get_backend().submit(_orig_compile, args, kwargs)


def _patch_compile():
    global _orig_compile
    _orig_compile = builtins.compile
    builtins.compile = _patched_compile


def _unpatch_compile():
    if _orig_compile is not None:
        builtins.compile = _orig_compile
