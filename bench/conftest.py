"""pytest-side guards for the benchmark suite.

pytest-benchmark's collection path imports a Brotli codec whose C extension
(`_brotli`) has no free-threading opt-in, which silently re-enables the GIL.
We cannot safely re-exec pytest from here, so instead we FAIL LOUDLY: if the
GIL is on at session start, the whole session errors out with instructions,
rather than quietly recording GIL-on numbers as free-threaded.

Always launch the pytest bench with the GIL forced off::

    PYTHONPATH=src PYTHON_GIL=0 python3 -m pytest bench/test_bench.py ...
"""
import pytest

from bench.gil import assert_nogil, is_free_threaded_build


@pytest.fixture(scope="session", autouse=True)
def require_nogil():
    # Runs after collection (where brotli would have flipped the GIL) but
    # before any benchmark executes -- the right place to catch a forgotten
    # PYTHON_GIL=0 before a single number is recorded.
    if is_free_threaded_build():
        assert_nogil("pytest session start")
    yield
