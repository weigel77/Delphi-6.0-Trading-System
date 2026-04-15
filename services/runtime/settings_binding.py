"""Config and environment binding for local and hosted Delphi runtime targets."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol, runtime_checkable

from flask import Flask

from config import (
    AppConfig,
    HOSTED_APP_DISPLAY_NAME,
    HOSTED_APP_PAGE_KICKER,
    HOSTED_APP_VERSION_LABEL,
    HOSTED_OAUTH_SESSION_NAMESPACE,
    HOSTED_RUNTIME_BASE_URL,
    HOSTED_SESSION_COOKIE_NAME,
)


@dataclass(frozen=True)
class RuntimeSettings:
    """Normalized runtime-facing settings resolved from config and app overrides."""

    runtime_target: str
    hosted_public_base_url: str
    flask_secret_key: str
    session_cookie_name: str
    oauth_session_namespace: str
    app_display_name: str
    app_page_kicker: str
    app_version_label: str

    def apply_to_app(self, app: Flask) -> None:
        if not app.config.get("SECRET_KEY"):
            app.config["SECRET_KEY"] = self.flask_secret_key
        app.secret_key = app.config["SECRET_KEY"]
        if self.runtime_target == "hosted":
            app.config["SESSION_COOKIE_NAME"] = self.session_cookie_name
            app.config["OAUTH_SESSION_NAMESPACE"] = self.oauth_session_namespace
            app.config["APP_DISPLAY_NAME"] = self.app_display_name
            app.config["APP_PAGE_KICKER"] = self.app_page_kicker
            app.config["APP_VERSION_LABEL"] = self.app_version_label
        elif app.config.get("SESSION_COOKIE_NAME") in {None, "", "session"}:
            app.config["SESSION_COOKIE_NAME"] = self.session_cookie_name
        if self.runtime_target != "hosted" and not app.config.get("OAUTH_SESSION_NAMESPACE"):
            app.config["OAUTH_SESSION_NAMESPACE"] = self.oauth_session_namespace
        if self.runtime_target != "hosted" and not app.config.get("APP_DISPLAY_NAME"):
            app.config["APP_DISPLAY_NAME"] = self.app_display_name
        if self.runtime_target != "hosted" and not app.config.get("APP_PAGE_KICKER"):
            app.config["APP_PAGE_KICKER"] = self.app_page_kicker
        if self.runtime_target != "hosted" and not app.config.get("APP_VERSION_LABEL"):
            app.config["APP_VERSION_LABEL"] = self.app_version_label
        if not app.config.get("RUNTIME_TARGET"):
            app.config["RUNTIME_TARGET"] = self.runtime_target
        if self.hosted_public_base_url and not app.config.get("HOSTED_PUBLIC_BASE_URL"):
            app.config["HOSTED_PUBLIC_BASE_URL"] = self.hosted_public_base_url


@runtime_checkable
class SettingsBinding(Protocol):
    """Resolve runtime settings from environment-backed config and app overrides."""

    def resolve(self, app: Flask) -> RuntimeSettings:
        ...


class LocalEnvironmentSettingsBinding:
    """Local settings binder that preserves current environment-driven behavior."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def resolve(self, app: Flask) -> RuntimeSettings:
        runtime_target = str(app.config.get("RUNTIME_TARGET") or self.config.runtime_target or "local").strip().lower() or "local"
        hosted_public_base_url = str(
            app.config.get("HOSTED_PUBLIC_BASE_URL") or self.config.hosted_public_base_url or ""
        ).strip()
        return RuntimeSettings(
            runtime_target=runtime_target,
            hosted_public_base_url=hosted_public_base_url,
            flask_secret_key=self.config.flask_secret_key,
            session_cookie_name=self.config.session_cookie_name,
            oauth_session_namespace=self.config.oauth_session_namespace,
            app_display_name=self.config.app_display_name,
            app_page_kicker=self.config.app_page_kicker,
            app_version_label=self.config.app_version_label,
        )


class HostedEnvironmentSettingsBindingSkeleton(LocalEnvironmentSettingsBinding):
    """Hosted settings binder skeleton that keeps hosted selection explicit but inactive by default."""

    def resolve(self, app: Flask) -> RuntimeSettings:
        settings = super().resolve(app)
        hosted_public_base_url = settings.hosted_public_base_url or (HOSTED_RUNTIME_BASE_URL if app.config.get("TESTING") else "")
        return replace(
            settings,
            runtime_target="hosted",
            hosted_public_base_url=hosted_public_base_url,
            session_cookie_name=HOSTED_SESSION_COOKIE_NAME,
            oauth_session_namespace=HOSTED_OAUTH_SESSION_NAMESPACE,
            app_display_name=HOSTED_APP_DISPLAY_NAME,
            app_page_kicker=HOSTED_APP_PAGE_KICKER,
            app_version_label=HOSTED_APP_VERSION_LABEL,
        )