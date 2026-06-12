"""Cooperative time.sleep."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ============================================================
# time
# ============================================================
_orig_time_sleep = None



def _patched_time_sleep(seconds):
    if _in_fiber():
        _co_sleep(seconds)
    else:
        _orig_time_sleep(seconds)


def _patch_time():
    global _orig_time_sleep
    _orig_time_sleep = time.sleep
    time.sleep = _patched_time_sleep


def _unpatch_time():
    time.sleep = _orig_time_sleep
