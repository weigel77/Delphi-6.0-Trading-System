"""Application lifecycle coordination for local and future hosted runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Optional, Protocol, runtime_checkable

from .launch import LaunchBehavior
from .profile import RuntimeProfile
from .scheduler import RuntimeJobHandle, RuntimeScheduler, ThreadingTimerScheduler


@dataclass(frozen=True)
class RuntimeComponent:
    """Named runtime component with startup and shutdown hooks."""

    name: str
    startup: Callable[[], None] | None = None
    shutdown: Callable[[], None] | None = None


@runtime_checkable
class RuntimeLifecycleCoordinator(Protocol):
    """Abstract lifecycle boundary for runtime-managed services."""

    def register_component(self, component: RuntimeComponent) -> None:
        ...

    def start_runtime(self) -> None:
        ...

    def stop_runtime(self) -> None:
        ...

    def schedule_launch(self) -> None:
        ...


class LocalRuntimeLifecycleCoordinator:
    """Local lifecycle coordinator that preserves current startup and shutdown behavior."""

    def __init__(
        self,
        profile: RuntimeProfile,
        *,
        launch_behavior: LaunchBehavior,
        scheduler: RuntimeScheduler | None = None,
        shutdown_registrar: Callable[[Callable[[], None]], object] | None = None,
    ) -> None:
        self.profile = profile
        self.launch_behavior = launch_behavior
        self.scheduler = scheduler or ThreadingTimerScheduler()
        self._shutdown_registrar = shutdown_registrar
        self._components: List[RuntimeComponent] = []
        self._shutdown_registered = False
        self._launch_handle: RuntimeJobHandle | None = None

    def register_component(self, component: RuntimeComponent) -> None:
        self._components.append(component)

    def start_runtime(self) -> None:
        if self.profile.register_shutdown_hooks and not self._shutdown_registered and self._shutdown_registrar is not None:
            self._shutdown_registrar(self.stop_runtime)
            self._shutdown_registered = True
        if not self.profile.auto_start_background_services:
            return
        for component in self._components:
            if component.startup is not None:
                component.startup()

    def stop_runtime(self) -> None:
        if self._launch_handle is not None:
            self._launch_handle.cancel()
            self._launch_handle = None
        for component in reversed(self._components):
            if component.shutdown is not None:
                component.shutdown()

    def schedule_launch(self) -> None:
        if not self.profile.auto_open_browser:
            return
        self._launch_handle = self.scheduler.schedule(
            self.profile.browser_launch_delay_seconds,
            lambda: self.launch_behavior.launch(self.profile),
            daemon=True,
        )