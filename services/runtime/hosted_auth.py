"""Supabase-compatible hosted auth and private-access foundation for Delphi."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol, runtime_checkable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from flask import Flask, current_app, has_request_context, request

from config import AppConfig

from .private_access import (
    AuthenticationRequiredError,
    PrivateAccessDeniedError,
    PrivateAccessGate,
    RequestIdentity,
    RequestIdentityResolver,
    SessionInvalidator,
    anonymous_request_identity,
)
from .supabase_integration import SupabaseRuntimeContext


@dataclass(frozen=True)
class HostedAuthConfig:
    """Hosted auth configuration resolved from environment and runtime context."""

    allowed_emails: tuple[str, ...]
    access_token_cookie_name: str
    refresh_token_cookie_name: str
    project_cookie_name: str

    @classmethod
    def resolve(cls, app: Flask, config: AppConfig, context: SupabaseRuntimeContext | None) -> "HostedAuthConfig":
        configured_emails = str(app.config.get("DELPHI_HOSTED_ALLOWED_EMAILS") or config.hosted_private_allowed_emails or "")
        allowed_emails = tuple(sorted({item.strip().lower() for item in configured_emails.split(",") if item.strip()}))
        project_cookie_name = f"sb-{context.config.project_ref}-auth-token" if context is not None and context.config.project_ref else "sb-auth-token"
        return cls(
            allowed_emails=allowed_emails,
            access_token_cookie_name=str(
                app.config.get("DELPHI_HOSTED_ACCESS_TOKEN_COOKIE_NAME") or config.hosted_access_token_cookie_name or "delphi_hosted_access_token"
            ).strip()
            or "delphi_hosted_access_token",
            refresh_token_cookie_name=str(
                app.config.get("DELPHI_HOSTED_REFRESH_TOKEN_COOKIE_NAME") or config.hosted_refresh_token_cookie_name or "delphi_hosted_refresh_token"
            ).strip()
            or "delphi_hosted_refresh_token",
            project_cookie_name=project_cookie_name,
        )

    @property
    def cookie_names(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((self.access_token_cookie_name, self.refresh_token_cookie_name, self.project_cookie_name, "sb-access-token")))


@dataclass(frozen=True)
class HostedBrowserSession:
    """Hosted browser session established from Supabase Auth."""

    user_id: str
    email: str
    display_name: str
    access_token: str
    refresh_token: str
    expires_in: int | None = None
    token_type: str = "bearer"
    payload: dict[str, Any] = field(default_factory=dict)

    def to_request_identity(self) -> RequestIdentity:
        return RequestIdentity(
            user_id=self.user_id,
            email=self.email,
            display_name=self.display_name,
            authenticated=bool(self.user_id and self.access_token),
            private_access_granted=False,
            auth_source="supabase-hosted",
            claims=dict(self.payload),
        )


@runtime_checkable
class HostedSessionAuthenticator(Protocol):
    """Create and persist hosted browser sessions for the Delphi 5.0 shell."""

    def sign_in(self, *, email: str, password: str) -> HostedBrowserSession:
        ...

    def establish_response_session(self, response: Any, session: HostedBrowserSession) -> None:
        ...


class NoopHostedSessionAuthenticator:
    """Fallback authenticator used when hosted sign-in is not active."""

    def sign_in(self, *, email: str, password: str) -> HostedBrowserSession:
        raise AuthenticationRequiredError("Hosted sign-in is not enabled for this runtime.")

    def establish_response_session(self, response: Any, session: HostedBrowserSession) -> None:
        return None


class SupabaseEmailPasswordAuthenticator(HostedSessionAuthenticator):
    """Supabase-compatible email/password browser sign-in for hosted Delphi."""

    def __init__(
        self,
        *,
        context: SupabaseRuntimeContext | None,
        auth_config: HostedAuthConfig,
        requester: Callable[..., Any] | None = None,
        secure_cookies: bool | None = None,
    ) -> None:
        self.context = context
        self.auth_config = auth_config
        self._requester = requester or urlopen
        self.secure_cookies = secure_cookies

    def sign_in(self, *, email: str, password: str) -> HostedBrowserSession:
        if self.context is None or not self.context.configured:
            raise AuthenticationRequiredError("Hosted sign-in is not configured for this Delphi runtime.")
        normalized_email = str(email or "").strip().lower()
        if not normalized_email or not str(password or ""):
            raise AuthenticationRequiredError("Email and password are required for hosted sign-in.")
        request = Request(
            f"{self.context.auth_url.rstrip('/')}/token?grant_type=password",
            data=json.dumps({"email": normalized_email, "password": str(password)}).encode("utf-8"),
            headers={
                "apikey": self.context.config.publishable_key or self.context.config.secret_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            response = self._requester(request, timeout=5.0)
            raw_body = response.read() if hasattr(response, "read") else b"{}"
            if hasattr(response, "close"):
                response.close()
            payload = json.loads((raw_body or b"{}").decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else ""
            if exc.code in {400, 401, 422}:
                raise AuthenticationRequiredError("Invalid hosted email/password credentials.") from exc
            raise AuthenticationRequiredError(f"Hosted sign-in failed: {detail or exc.reason}") from exc
        except URLError as exc:
            raise AuthenticationRequiredError(f"Hosted sign-in is unavailable: {exc.reason}") from exc

        user_payload = payload.get("user") or {}
        access_token = str(payload.get("access_token") or "").strip()
        refresh_token = str(payload.get("refresh_token") or "").strip()
        user_id = str(user_payload.get("id") or payload.get("user_id") or "").strip()
        email_value = str(user_payload.get("email") or normalized_email).strip().lower()
        user_metadata = user_payload.get("user_metadata") or {}
        display_name = str(user_metadata.get("full_name") or user_metadata.get("name") or email_value or user_id).strip()
        if not access_token or not refresh_token or not user_id:
            raise AuthenticationRequiredError("Hosted sign-in did not return a usable Supabase session.")
        return HostedBrowserSession(
            user_id=user_id,
            email=email_value,
            display_name=display_name,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=int(payload.get("expires_in")) if payload.get("expires_in") not in {None, ""} else None,
            token_type=str(payload.get("token_type") or "bearer").strip() or "bearer",
            payload=dict(payload),
        )

    def establish_response_session(self, response: Any, session: HostedBrowserSession) -> None:
        max_age = int(session.expires_in) if session.expires_in is not None else None
        secure = self.secure_cookies
        if has_request_context():
            forwarded_proto = str(request.headers.get("X-Forwarded-Proto") or "").split(",", 1)[0].strip().lower()
            secure = request.is_secure or forwarded_proto == "https"
        if secure is None:
            hosted_public_base_url = str(current_app.config.get("HOSTED_PUBLIC_BASE_URL") or "").strip().lower() if current_app else ""
            if hosted_public_base_url:
                secure = hosted_public_base_url.startswith("https://")
            else:
                secure = bool(self.context and str(self.context.config.url or "").startswith("https://"))
        cookie_kwargs = {
            "httponly": True,
            "samesite": "Lax",
            "secure": bool(secure),
            "path": "/",
        }
        if max_age is not None:
            cookie_kwargs["max_age"] = max_age
        response.set_cookie(self.auth_config.access_token_cookie_name, session.access_token, **cookie_kwargs)
        response.set_cookie(self.auth_config.refresh_token_cookie_name, session.refresh_token, **cookie_kwargs)
        response.set_cookie(
            self.auth_config.project_cookie_name,
            json.dumps({"access_token": session.access_token, "refresh_token": session.refresh_token}),
            **cookie_kwargs,
        )
        response.set_cookie("sb-access-token", session.access_token, **cookie_kwargs)
        if not secure and hasattr(response, "headers") and hasattr(response.headers, "getlist") and hasattr(response.headers, "setlist"):
            cookie_headers = response.headers.getlist("Set-Cookie")
            response.headers.setlist(
                "Set-Cookie",
                [header.replace("; Secure", "").replace("; secure", "") for header in cookie_headers],
            )


class SupabaseHostedIdentityResolver(RequestIdentityResolver):
    """Resolve a hosted request identity from Supabase Auth bearer or cookie tokens."""

    def __init__(
        self,
        *,
        context: SupabaseRuntimeContext | None,
        auth_config: HostedAuthConfig,
        requester: Callable[..., Any] | None = None,
    ) -> None:
        self.context = context
        self.auth_config = auth_config
        self._requester = requester or urlopen

    def resolve_request_identity(self, request: Any) -> RequestIdentity:
        if self.context is None or not self.context.configured:
            return anonymous_request_identity(auth_source="hosted")
        access_token = self._extract_access_token(request)
        if not access_token:
            return anonymous_request_identity(auth_source="hosted")
        try:
            payload = self._load_user_payload(access_token)
        except Exception:
            return anonymous_request_identity(auth_source="hosted")
        email = str(payload.get("email") or "").strip().lower()
        user_metadata = payload.get("user_metadata") or {}
        display_name = str(user_metadata.get("full_name") or user_metadata.get("name") or email or payload.get("id") or "").strip()
        return RequestIdentity(
            user_id=str(payload.get("id") or "").strip(),
            email=email,
            display_name=display_name,
            authenticated=bool(payload.get("id")),
            private_access_granted=False,
            auth_source="supabase-hosted",
            claims=dict(payload),
        )

    def _load_user_payload(self, access_token: str) -> dict[str, Any]:
        target_url = f"{self.context.auth_url.rstrip('/')}/user"
        api_key = self.context.config.publishable_key or self.context.config.secret_key
        request = Request(
            target_url,
            headers={
                "apikey": api_key,
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
            method="GET",
        )
        try:
            response = self._requester(request, timeout=5.0)
            raw_body = response.read() if hasattr(response, "read") else b"{}"
            if hasattr(response, "close"):
                response.close()
            return json.loads((raw_body or b"{}").decode("utf-8"))
        except HTTPError as exc:
            raise AuthenticationRequiredError(f"Supabase auth rejected the request: {exc.code}") from exc
        except URLError as exc:
            raise AuthenticationRequiredError(f"Supabase auth lookup failed: {exc.reason}") from exc

    def _extract_access_token(self, request: Any) -> str:
        authorization = str(getattr(request, "headers", {}).get("Authorization") or "").strip()
        if authorization.lower().startswith("bearer "):
            return authorization[7:].strip()
        cookies = getattr(request, "cookies", {}) or {}
        for cookie_name in self.auth_config.cookie_names:
            token = self._coerce_cookie_token(cookies.get(cookie_name))
            if token:
                return token
        return ""

    @staticmethod
    def _coerce_cookie_token(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if text.count(".") >= 2:
            return text
        try:
            payload = json.loads(text)
        except (TypeError, ValueError):
            return text
        if isinstance(payload, dict):
            candidate = str(payload.get("access_token") or "").strip()
            return candidate
        if isinstance(payload, list) and payload:
            candidate = str(payload[0] or "").strip()
            return candidate
        return ""


class SupabasePrivateAccessGate(PrivateAccessGate):
    """Bill-only private-access gate for the hosted Delphi runtime."""

    def __init__(self, auth_config: HostedAuthConfig) -> None:
        self.auth_config = auth_config

    def require_private_access(self, identity: RequestIdentity) -> RequestIdentity:
        if not identity.authenticated:
            raise AuthenticationRequiredError("Hosted access requires an authenticated Supabase user.")
        if not identity.email or identity.email.lower() not in self.auth_config.allowed_emails:
            raise PrivateAccessDeniedError("This hosted Delphi runtime is restricted to approved private-access users.")
        return RequestIdentity(
            user_id=identity.user_id,
            email=identity.email,
            display_name=identity.display_name,
            authenticated=True,
            private_access_granted=True,
            auth_source=identity.auth_source,
            claims=dict(identity.claims),
        )


class SupabaseSessionInvalidator(SessionInvalidator):
    """Clear hosted auth cookies during logout without touching local Flask workflow state."""

    def __init__(self, auth_config: HostedAuthConfig) -> None:
        self.auth_config = auth_config

    def invalidate_response(self, response: Any) -> None:
        for cookie_name in self.auth_config.cookie_names:
            response.delete_cookie(cookie_name)