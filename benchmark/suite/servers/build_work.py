#!/usr/bin/env python3
"""Compile work_cy.pyx -> work_cy*.so for the work-curve experiment.

    PYTHONPATH=<repo>/src python3.13t build_work.py build_ext --inplace

Unlike build_cy.py this needs NO include_dirs and NO runloom header: work_cy is
self-contained inline arithmetic (an FNV-1a byte loop), reaching nothing in the
runloom C ext. -O3 so the compiled loop is a fair "what does native buy" twin of
the interpreted py_fnv(). annotate=True emits work_cy.html (yellow = Python
interaction -- the hot loop must be white).
"""
import os
import sys

from setuptools import setup, Extension
from Cython.Build import cythonize

HERE = os.path.dirname(os.path.abspath(__file__))

ext = Extension(
    "work_cy",
    sources=[os.path.join(HERE, "work_cy.pyx")],
    extra_compile_args=["-O3", "-fno-strict-aliasing", "-g"],
)

if len(sys.argv) == 1:
    sys.argv += ["build_ext", "--inplace"]

setup(
    name="work_cy",
    ext_modules=cythonize(
        [ext],
        force=True,
        annotate=True,
        compiler_directives={"language_level": "3"},
    ),
    script_args=sys.argv[1:],
)
