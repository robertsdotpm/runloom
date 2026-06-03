"""RunloomEventLoop -- asyncio.AbstractEventLoop assembled from the loop_* mixins.

The event loop is large, so its methods live in cohesive mixin modules
(lifecycle, scheduling, I/O, networking, subprocess, signals, the run loop).
This module just composes them into the concrete class -- exactly the shape
CPython's asyncio uses (BaseEventLoop + selector/proactor mixins).  The method
names are disjoint across the mixins, so MRO order is immaterial; the mixins
share state only through ``self`` (every attribute is set in __init__).
"""
from ._base import *  # noqa: F401,F403  (asyncio + shared foundation)
from .loop_core import _LoopCoreMixin
from .loop_schedule import _LoopScheduleMixin
from .loop_io import _LoopIOMixin
from .loop_net import _LoopNetMixin
from .loop_subprocess import _LoopSubprocessMixin
from .loop_signals import _LoopSignalMixin
from .loop_run import _LoopRunMixin


class RunloomEventLoop(_LoopCoreMixin, _LoopScheduleMixin, _LoopIOMixin,
                    _LoopNetMixin, _LoopSubprocessMixin, _LoopSignalMixin,
                    _LoopRunMixin, asyncio.AbstractEventLoop):
    """asyncio.AbstractEventLoop with everything the runloom bridge needs.

    __init__ and the lifecycle/debug/exception-handler methods come from
    _LoopCoreMixin; see the loop_* modules for the rest.
    """
