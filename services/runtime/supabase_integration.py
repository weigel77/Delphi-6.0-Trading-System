"""Supabase foundation boundary for hosted Delphi runtime initialization."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Protocol, runtime_checkable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from flask import Flask

from config import AppConfig


@dataclass(frozen=True)
class SupabaseConfig:
    """Resolved Supabase project configuration."""

    url: str
    publishable_key: str
    secret_key: str

    @property
    def is_configured(self) -> bool:
        return bool(self.url and (self.secret_key or self.publishable_key))

    @property
    def project_ref(self) -> str:
        host = self.url.replace("https://", "").replace("http://", "").split("/", 1)[0].strip()
        return host.split(".", 1)[0] if host else ""

    def preferred_key(self) -> str:
        return self.secret_key or self.publishable_key

    @classmethod
    def resolve(cls, app: Flask, config: AppConfig) -> "SupabaseConfig":
        return cls(
            url=str(app.config.get("SUPABASE_URL") or config.supabase_url or "").strip(),
            publishable_key=str(app.config.get("SUPABASE_PUBLISHABLE_KEY") or config.supabase_publishable_key or "").strip(),
            secret_key=str(app.config.get("SUPABASE_SECRET_KEY") or config.supabase_secret_key or "").strip(),
        )


@dataclass(frozen=True)
class SupabaseConnectivityResult:
    """Result of a safe Supabase project health check."""

    ok: bool
    endpoint: str
    status_code: int | None
    detail: str


class SupabaseRequestError(RuntimeError):
    """Raised when a Supabase PostgREST request fails."""


@runtime_checkable
class SupabaseTableGateway(Protocol):
    """Minimal table-level gateway used by Supabase-backed repositories."""

    def select(
        self,
        table: str,
        *,
        filters: dict[str, str] | None = None,
        order: str | None = None,
        limit: int | None = None,
        columns: str = "*",
    ) -> list[dict[str, Any]]:
        ...

    def insert(self, table: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        ...

    def update(self, table: str, payload: dict[str, Any], *, filters: dict[str, str]) -> list[dict[str, Any]]:
        ...

    def delete(self, table: str, *, filters: dict[str, str]) -> None:
        ...

    def rpc(self, function_name: str, payload: dict[str, Any] | None = None) -> Any:
        ...


@dataclass(frozen=True)
class SupabaseRuntimeContext:
    """Hosted runtime Supabase context initialized for Delphi."""

    config: SupabaseConfig
    rest_url: str
    auth_url: str
    healthcheck_path: str
    configured: bool


class SupabaseProjectIntegration:
    """Supabase initialization and readiness checks for hosted Delphi runtime assembly."""

    def __init__(
        self,
        config: SupabaseConfig,
        *,
        requester: Callable[..., Any] | None = None,
    ) -> None:
        self.config = config
        self._requester = requester or urlopen

    def initialize_context(self) -> SupabaseRuntimeContext:
        base_url = self.config.url.rstrip("/")
        return SupabaseRuntimeContext(
            config=self.config,
            rest_url=f"{base_url}/rest/v1" if base_url else "",
            auth_url=f"{base_url}/auth/v1" if base_url else "",
            healthcheck_path="/auth/v1/settings",
            configured=self.config.is_configured,
        )

    def create_table_gateway(self) -> "SupabasePostgrestClient":
        return SupabasePostgrestClient(self.initialize_context(), requester=self._requester)

    def check_connectivity(self, *, timeout_seconds: float = 5.0) -> SupabaseConnectivityResult:
        context = self.initialize_context()
        if not context.configured:
            return SupabaseConnectivityResult(
                ok=False,
                endpoint=context.healthcheck_path,
                status_code=None,
                detail="Supabase environment is not fully configured.",
            )

        key = self.config.preferred_key()
        headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
        }
        endpoints = [
            ("/auth/v1/settings", headers),
            ("/rest/v1/", {**headers, "Accept": "application/openapi+json, application/json"}),
        ]
        last_failure = SupabaseConnectivityResult(
            ok=False,
            endpoint=context.healthcheck_path,
            status_code=None,
            detail="Supabase connectivity was not attempted.",
        )
        for endpoint, endpoint_headers in endpoints:
            target_url = urljoin(f"{self.config.url.rstrip('/')}/", endpoint.lstrip("/"))
            request = Request(target_url, headers=endpoint_headers, method="GET")
            try:
                response = self._requester(request, timeout=timeout_seconds)
                status_code = int(getattr(response, "status", None) or response.getcode())
                if hasattr(response, "close"):
                    response.close()
                return SupabaseConnectivityResult(
                    ok=200 <= status_code < 400,
                    endpoint=endpoint,
                    status_code=status_code,
                    detail="Supabase connectivity check succeeded." if 200 <= status_code < 400 else "Supabase returned a non-success status.",
                )
            except HTTPError as exc:
                last_failure = SupabaseConnectivityResult(
                    ok=False,
                    endpoint=endpoint,
                    status_code=exc.code,
                    detail=f"Supabase HTTP error: {exc.reason}",
                )
            except URLError as exc:
                last_failure = SupabaseConnectivityResult(
                    ok=False,
                    endpoint=endpoint,
                    status_code=None,
                    detail=f"Supabase network error: {exc.reason}",
                )
            except Exception as exc:  # pragma: no cover - defensive transport failure guard
                last_failure = SupabaseConnectivityResult(
                    ok=False,
                    endpoint=endpoint,
                    status_code=None,
                    detail=f"Supabase connectivity error: {exc}",
                )
        return last_failure


class SupabasePostgrestClient:
    """Small PostgREST gateway for Delphi hosted repositories."""

    def __init__(
        self,
        context: SupabaseRuntimeContext,
        *,
        requester: Callable[..., Any] | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.context = context
        self._requester = requester or urlopen
        self.timeout_seconds = timeout_seconds

    def select(
        self,
        table: str,
        *,
        filters: dict[str, str] | None = None,
        order: str | None = None,
        limit: int | None = None,
        columns: str = "*",
    ) -> list[dict[str, Any]]:
        query: list[tuple[str, str]] = [("select", columns)]
        query.extend((key, value) for key, value in (filters or {}).items())
        if order:
            query.append(("order", order))
        if limit is not None:
            query.append(("limit", str(int(limit))))
        payload = self._request_json("GET", table, query=query)
        return payload if isinstance(payload, list) else []

    def insert(self, table: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        response_payload = self._request_json(
            "POST",
            table,
            payload=payload,
            headers={"Prefer": "return=representation"},
        )
        return response_payload if isinstance(response_payload, list) else []

    def update(self, table: str, payload: dict[str, Any], *, filters: dict[str, str]) -> list[dict[str, Any]]:
        response_payload = self._request_json(
            "PATCH",
            table,
            query=list(filters.items()),
            payload=payload,
            headers={"Prefer": "return=representation"},
        )
        return response_payload if isinstance(response_payload, list) else []

    def delete(self, table: str, *, filters: dict[str, str]) -> None:
        self._request_json("DELETE", table, query=list(filters.items()), headers={"Prefer": "return=minimal"})

    def rpc(self, function_name: str, payload: dict[str, Any] | None = None) -> Any:
        return self._request_json(
            "POST",
            f"rpc/{function_name}",
            payload=payload or {},
            headers={"Prefer": "return=representation"},
        )

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        query: list[tuple[str, str]] | None = None,
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        if not self.context.configured:
            raise SupabaseRequestError("Supabase context is not configured.")
        target_url = self._build_url(path, query=query)
        body = None
        request_headers = self._base_headers()
        if headers:
            request_headers.update(headers)
        if payload is not None:
            body = json.dumps(payload, default=str).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")
        request = Request(target_url, data=body, headers=request_headers, method=method)
        try:
            response = self._requester(request, timeout=self.timeout_seconds)
            raw_body = response.read() if hasattr(response, "read") else b""
            if hasattr(response, "close"):
                response.close()
            if not raw_body:
                return None
            return json.loads(raw_body.decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
            raise SupabaseRequestError(f"Supabase HTTP error {exc.code}: {detail or exc.reason}") from exc
        except URLError as exc:
            raise SupabaseRequestError(f"Supabase network error: {exc.reason}") from exc

    def _build_url(self, path: str, *, query: list[tuple[str, str]] | None = None) -> str:
        base_url = self.context.rest_url.rstrip("/")
        target_url = urljoin(f"{base_url}/", path.lstrip("/"))
        if not query:
            return target_url
        return f"{target_url}?{urlencode(query, doseq=True, safe='.*(),:-')}"

    def _base_headers(self) -> dict[str, str]:
        key = self.context.config.preferred_key()
        return {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
        }