#!/usr/bin/env python3
"""Compile handler_cdef.pyx -> handler_cdef*.so against the runloom_c C-API
header (same pattern as build_cy.py)."""
import os
import sys

from setuptools import setup, Extension
from Cython.Build import cythonize

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
INC = os.path.join(REPO, "src", "runloom_c")

if not os.path.exists(os.path.join(INC, "runloom_tcp_capi.h")):
    sys.exit("cannot find runloom_tcp_capi.h under %s -- build runloom_c first" % INC)

ext = Extension(
    "handler_cdef",
    sources=[os.path.join(HERE, "handler_cdef.pyx")],
    include_dirs=[INC],
    extra_compile_args=["-O3", "-fno-strict-aliasing", "-g"],
)

if len(sys.argv) == 1:
    sys.argv += ["build_ext", "--inplace"]

setup(
    name="handler_cdef",
    ext_modules=cythonize([ext], force=True, annotate=True,
                          compiler_directives={"language_level": "3"}),
    script_args=sys.argv[1:],
)
