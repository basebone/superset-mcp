"""Shared token bookkeeping records for the OAuth provider and token stores.

Kept in their own module so ``auth`` and ``token_store`` can both import them
without a circular dependency.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class _StoredRefreshToken:
    token: str
    client_id: str
    scopes: list[str]
    expires_at: float
    # Email of the Google-authenticated user, when the token was issued via the
    # federated login flow. None for static API tokens and legacy auto-approval.
    user_email: str | None = None


@dataclass(slots=True)
class _StoredAccessToken:
    token: str
    client_id: str
    scopes: list[str]
    expires_at: float
    user_email: str | None = None
