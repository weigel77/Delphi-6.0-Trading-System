"""Identity and private-access contracts for local and hosted Delphi runtimes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class RequestIdentity:
    """Resolved request identity used by hosted access gates."""

    user_id: str = ""
    email: str = ""
    display_name: str = ""
    authenticated: bool = False
    private_access_granted: bool = False
    auth_source: str = "anonymous"
    claims: dict[str, Any] = field(default_factory=dict)


class AuthenticationRequiredError(PermissionError):
    """Raised when hosted access requires an authenticated identity."""


class PrivateAccessDeniedError(PermissionError):
    """Raised when the authenticated identity is not allowed through the private gate."""


@runtime_checkable
class RequestIdentityResolver(Protocol):
    """Resolve the current request identity from the active runtime transport."""

    def resolve_request_identity(self, request: Any) -> RequestIdentity:
        ...


@runtime_checkable
class PrivateAccessGate(Protocol):
    """Authorize access to hosted-only protected routes and pages."""

    def require_private_access(self, identity: RequestIdentity) -> RequestIdentity:
        ...


@runtime_checkable
class SessionInvalidator(Protocol):
    """Invalidate hosted auth/session artifacts on logout."""

    def invalidate_response(self, response: Any) -> None:
        ...


def anonymous_request_identity(*, auth_source: str = "anonymous") -> RequestIdentity:
    return RequestIdentity(authenticated=False, private_access_granted=False, auth_source=auth_source)


class LocalRequestIdentityResolver:
    """Local resolver that preserves current unauthenticated desktop behavior."""

    def resolve_request_identity(self, request: Any) -> RequestIdentity:
        return anonymous_request_identity(auth_source="local")


class LocalPrivateAccessGate:
    """Local no-op gate used while hosted auth remains inactive for the default runtime."""

    def require_private_access(self, identity: RequestIdentity) -> RequestIdentity:
        raise AuthenticationRequiredError("Hosted private access is not enabled for the local runtime.")


class NoopSessionInvalidator:
    """Local session invalidator that leaves current Flask session behavior untouched."""

    def invalidate_response(self, response: Any) -> None:
        return None