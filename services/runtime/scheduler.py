"""Background scheduling abstraction and threading.Timer adapter."""

from __future__ import annotations

from threading import Timer
from typing import Any, Callable, Protocol, runtime_checkable


@runtime_checkable
class RuntimeJobHandle(Protocol):
    """Minimal handle for a scheduled in-process job."""

    def cancel(self) -> None:
        ...


@runtime_checkable
class RuntimeScheduler(Protocol):
    """Abstract scheduler for in-process delayed callbacks."""

    def schedule(self, delay_seconds: float, callback: Callable[[], Any], *, daemon: bool = True) -> RuntimeJobHandle:
        ...


class _TimerJobHandle:
    def __init__(self, timer: Timer) -> None:
        self._timer = timer

    def cancel(self) -> None:
        self._timer.cancel()


class ThreadingTimerScheduler:
    """Adapter that keeps the current threading.Timer behavior intact."""

    def __init__(self, timer_factory: Callable[[float, Callable[[], Any]], Any] | None = None) -> None:
        self._timer_factory = timer_factory or Timer

    def schedule(self, delay_seconds: float, callback: Callable[[], Any], *, daemon: bool = True) -> RuntimeJobHandle:
        timer = self._timer_factory(delay_seconds, callback)
        timer.daemon = daemon
        timer.start()
        return _TimerJobHandle(timer)