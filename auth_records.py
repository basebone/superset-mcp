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


@dataclass(slots=True)
class _StoredAccessToken:
    token: str
    client_id: str
    scopes: list[str]
    expires_at: float
