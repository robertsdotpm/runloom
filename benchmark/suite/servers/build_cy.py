#!/usr/bin/env python3
"""Compile handler_cy.pyx -> handler_cy*.so against the runloom_c C-API header.

Run with the free-threaded interpreter:
    PYTHONPATH=<repo>/src python3.13t build_cy.py build_ext --inplace

The only non-default knob is include_dirs (so `#include "runloom_tcp_capi.h"`
resolves) and -O3.  freethreading_compatible / boundscheck etc. live in the
.pyx header line so they travel with the source.
"""
import os
import sys

from setuptools import setup, Extension
from Cython.Build import cythonize

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))  # servers->suite->benchmark->repo
INC = os.path.join(REPO, "src", "runloom_c")

if not os.path.exists(os.path.join(INC, "runloom_tcp_capi.h")):
    sys.exit("cannot find runloom_tcp_capi.h under %s -- build runloom_c first" % INC)

ext = Extension(
    "handler_cy",
    sources=[os.path.join(HERE, "handler_cy.pyx")],
    include_dirs=[INC],
    extra_compile_args=["-O3", "-fno-strict-aliasing", "-g"],
    # No libraries: the C functions are reached through the runloom_c.__tcp_capi__
    # capsule at import time, so handler_cy.so has no undefined runloom symbol.
)

if len(sys.argv) == 1:
    sys.argv += ["build_ext", "--inplace"]

setup(
    name="handler_cy",
    ext_modules=cythonize(
        [ext],
        force=True,
        annotate=True,           # emit handler_cy.html (yellow = Python interaction)
        compiler_directives={"language_level": "3"},
    ),
    script_args=sys.argv[1:],
)
