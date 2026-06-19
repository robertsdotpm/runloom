#!/usr/bin/env python3
"""Compile centry_probe.pyx -> centry_probe*.so.

    PYTHONPATH=<repo>/src python3.13t build_centry.py build_ext --inplace

No include_dirs / no link libs: the two runloom_mn_* symbols are declared
`cdef extern from *` (no header) and left UNDEFINED in the .so -- they are
resolved at runtime once the driver promotes runloom_c.so to RTLD_GLOBAL (both
are exported `T` in runloom_c.so). A shared object with undefined symbols is
valid; resolution is deferred to load time.
"""
import os
import sys

from setuptools import setup, Extension
from Cython.Build import cythonize

HERE = os.path.dirname(os.path.abspath(__file__))

ext = Extension(
    "centry_probe",
    sources=[os.path.join(HERE, "centry_probe.pyx")],
    extra_compile_args=["-O3", "-fno-strict-aliasing", "-g"],
)

if len(sys.argv) == 1:
    sys.argv += ["build_ext", "--inplace"]

setup(
    name="centry_probe",
    ext_modules=cythonize([ext], force=True, compiler_directives={"language_level": "3"}),
    script_args=sys.argv[1:],
)
