"""OAuth helpers for Schwab authorization and token refresh."""

from __future__ import annotations

import base64
import logging
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests

from config import AppConfig

from .token_store import JsonTokenStore
from .providers.base_provider import (
    ProviderAuthRequiredError,
    ProviderConfigurationError,
    ProviderReauthenticationRequiredError,
)


LOGGER = logging.getLogger(__name__)


class SchwabAuthService:
    """Manage Schwab OAuth URLs, token exchange, and refresh operations."""

    def __init__(self, config: AppConfig, token_store: Optional[JsonTokenStore] = None) -> None:
        self.config = config
        self.token_store = token_store or JsonTokenStore(config.schwab_token_path)

    def build_state_token(self) -> str:
        """Return a random state token for the OAuth redirect."""
        return secrets.token_urlsafe(32)

    def build_authorization_url(self, state: Optional[str] = None) -> str:
        """Build the Schwab OAuth authorization URL."""
        self._validate_oauth_config()
        query = {
            "client_id": self.config.schwab_client_id,
            "redirect_uri": self.config.schwab_redirect_uri,
            "response_type": "code",
        }
        if state:
            query["state"] = state
        return f"{self.config.schwab_auth_url}?{urlencode(query)}"

    def exchange_code_for_tokens(self, authorization_code: str) -> Dict[str, Any]:
        """Exchange an OAuth authorization code for access and refresh tokens."""
        self._validate_oauth_config()
        response = requests.post(
            self.config.schwab_token_url,
            data={
                "grant_type": "authorization_code",
                "code": authorization_code,
                "redirect_uri": self.config.schwab_redirect_uri,
                "client_id": self.config.schwab_client_id,
                "client_secret": self.config.schwab_client_secret,
            },
            headers=self._build_token_headers(),
            timeout=30,
        )
        return self._store_token_response(response)

    def refresh_access_token(self) -> Dict[str, Any]:
        """Refresh the Schwab access token using the stored refresh token."""
        self._validate_oauth_config()
        tokens = self.token_store.load()
        if not tokens or not tokens.get("refresh_token"):
            raise ProviderAuthRequiredError("Click login to connect to Schwab")

        response = requests.post(
            self.config.schwab_token_url,
            data={
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
                "client_id": self.config.schwab_client_id,
                "client_secret": self.config.schwab_client_secret,
            },
            headers=self._build_token_headers(),
            timeout=30,
        )
        return self._store_token_response(response, existing_refresh_token=tokens.get("refresh_token"))

    def get_valid_access_token(self) -> str:
        """Return a valid access token, refreshing it if needed."""
        tokens = self.token_store.load()
        if not tokens:
            raise ProviderAuthRequiredError("Click login to connect to Schwab")

        expires_at = self._parse_datetime(tokens.get("expires_at"))
        if expires_at is None or datetime.utcnow() >= expires_at:
            try:
                tokens = self.refresh_access_token()
            except ProviderAuthRequiredError:
                raise
            except Exception as exc:
                self.token_store.clear()
                raise ProviderReauthenticationRequiredError("Schwab authentication expired. Please log in again.") from exc

        access_token = tokens.get("access_token")
        if not access_token:
            raise ProviderAuthRequiredError("Click login to connect to Schwab")

        return access_token

    def is_authenticated(self) -> bool:
        """Return whether a token payload currently exists."""
        tokens = self.token_store.load()
        return bool(tokens and tokens.get("access_token"))

    def clear_tokens(self) -> None:
        """Remove any stored tokens."""
        self.token_store.clear()

    def _store_token_response(
        self,
        response: requests.Response,
        existing_refresh_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Validate a token response and persist the normalized payload."""
        if response.status_code >= 400:
            self._log_auth_event("token_write_failed", level=logging.WARNING, status_code=response.status_code)
            raise ProviderReauthenticationRequiredError(
                f"Unable to authenticate with Schwab right now ({response.status_code}). Please log in again."
            )

        payload = response.json()
        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token") or existing_refresh_token
        expires_in = int(payload.get("expires_in", 1800))
        expires_at = datetime.utcnow() + timedelta(seconds=max(expires_in - 60, 60))

        normalized = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": expires_at.isoformat(),
            "token_type": payload.get("token_type", "Bearer"),
            "scope": payload.get("scope"),
        }
        try:
            self.token_store.save(normalized)
        except OSError as exc:
            self._log_auth_event("token_write_failed", level=logging.WARNING, reason=type(exc).__name__)
            raise ProviderReauthenticationRequiredError(
                "Schwab authentication succeeded, but the token file could not be written."
            ) from exc
        self._log_auth_event("token_write_success")
        return normalized

    def _validate_oauth_config(self) -> None:
        """Validate the required Schwab OAuth settings."""
        missing = [
            name
            for name, value in {
                "SCHWAB_CLIENT_ID": self.config.schwab_client_id,
                "SCHWAB_CLIENT_SECRET": self.config.schwab_client_secret,
                "SCHWAB_REDIRECT_URI": self.config.schwab_redirect_uri,
                "SCHWAB_AUTH_URL": self.config.schwab_auth_url,
                "SCHWAB_TOKEN_URL": self.config.schwab_token_url,
            }.items()
            if not value
        ]
        if missing:
            raise ProviderConfigurationError(
                f"Missing Schwab OAuth configuration: {', '.join(missing)}. Update your environment variables and try again."
            )

    def _build_token_headers(self) -> Dict[str, str]:
        """Build token endpoint headers with a basic authorization header."""
        encoded = base64.b64encode(
            f"{self.config.schwab_client_id}:{self.config.schwab_client_secret}".encode("utf-8")
        ).decode("utf-8")
        return {
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        """Parse an ISO timestamp if present."""
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _log_auth_event(self, event: str, *, level: int = logging.INFO, **details: Any) -> None:
        payload = {
            "env": self.config.app_display_name,
            "port": self.config.app_port,
            "redirect_uri": self.config.schwab_redirect_uri,
            "token_target_path": str(self.token_store.file_path),
        }
        payload.update(details)
        LOGGER.log(level, "Schwab auth %s | %s", event, " | ".join(f"{key}={value}" for key, value in payload.items()))