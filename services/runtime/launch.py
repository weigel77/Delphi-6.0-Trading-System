"""Launch behavior abstraction and local browser adapter."""

from __future__ import annotations

import webbrowser
from typing import Callable, Protocol, runtime_checkable

from .profile import RuntimeProfile


@runtime_checkable
class LaunchBehavior(Protocol):
    """Abstract launch behavior for runtime entrypoints."""

    def launch(self, profile: RuntimeProfile) -> None:
        ...


class WebBrowserLaunchBehavior:
    """Adapter that preserves the current local browser-launch behavior."""

    def __init__(self, opener: Callable[[str], bool] | None = None) -> None:
        self._opener = opener or webbrowser.open_new

    def launch(self, profile: RuntimeProfile) -> None:
        self._opener(profile.launch_url)