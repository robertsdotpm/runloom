"""Compile live Python functions to a native Cython module, on the fly.

`compile_funcs([f, g, ...])` takes function *objects*, pulls their source with
inspect.getsource (no hand-port), writes one .pyx, builds it into a real
extension module, imports it, and returns the native callables in order.

Why a module build instead of `cython.compile`:
  - cython.compile() uses cython_inline, which type-infers the *arguments* at
    call time and crashes on extension types (TCPConn) -- `safe_type` ->
    'NoneType has no attribute is_type'. A module keeps every name as a Python
    object, so any argument type works and there is no per-signature rebuild.

Free-threading note: the .pyx declares `freethreading_compatible=True`. Without
it, importing the compiled .so under PYTHON_GIL=0 RE-ENABLES the GIL (CPython
treats an unmarked C ext as GIL-requiring), which would silently destroy M:N
parallelism. We assert the GIL stays disabled after import.
"""
import hashlib
import importlib
import inspect
import os
import sys
import sysconfig
import textwrap


def _build_dir():
    d = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cyc_build")
    os.makedirs(d, exist_ok=True)
    return d


def compile_funcs(funcs, preamble="", directives=None):
    """Compile `funcs` into one native module; return the compiled functions.

    preamble: extra source prepended (module-level constants the funcs need).
    directives: extra `# cython:` directives (dict).
    """
    srcs = [textwrap.dedent(inspect.getsource(f)) for f in funcs]
    body = (preamble + "\n\n" if preamble else "") + "\n\n".join(srcs)

    header = [
        "# cython: language_level=3",
        "# cython: freethreading_compatible=True",
        "# cython: annotation_typing=True",
        "cimport cython",
    ]
    for k, v in (directives or {}).items():
        header.append("# cython: %s=%s" % (k, v))
    source = "\n".join(header) + "\n\n" + body

    digest = hashlib.sha1(source.encode("utf-8")).hexdigest()[:16]
    modname = "cyc_" + digest
    blddir = _build_dir()
    pyx = os.path.join(blddir, modname + ".pyx")
    if not os.path.exists(pyx):
        with open(pyx, "w") as fh:
            fh.write(source)

    if blddir not in sys.path:
        sys.path.insert(0, blddir)

    # Build the extension in-place if not already built for this digest.
    so_glob = os.path.join(blddir, modname + "*.so")
    import glob
    if not glob.glob(so_glob):
        _cythonize_and_build(pyx, blddir)

    gil_before = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else None
    mod = importlib.import_module(modname)
    gil_after = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else None
    if gil_before is False and gil_after is True:
        raise RuntimeError(
            "importing compiled %s re-enabled the GIL -- the module is not "
            "marked free-threading compatible" % modname)

    return [getattr(mod, f.__name__) for f in funcs]


def _cythonize_and_build(pyx, blddir):
    from Cython.Build import cythonize
    from setuptools import Extension
    from setuptools.dist import Distribution

    modname = os.path.splitext(os.path.basename(pyx))[0]
    ext = Extension(
        modname, [pyx],
        extra_compile_args=["-O3", "-fno-strict-aliasing"],
    )
    ext_modules = cythonize([ext], quiet=True,
                            compiler_directives={"language_level": 3})

    dist = Distribution({"ext_modules": ext_modules})
    dist.script_args = ["build_ext", "--inplace",
                        "--build-temp", os.path.join(blddir, "tmp"),
                        "--build-lib", blddir]
    cwd = os.getcwd()
    try:
        os.chdir(blddir)
        dist.parse_command_line()
        dist.run_commands()
    finally:
        os.chdir(cwd)
