"""Persistence backends for OAuth-issued access and refresh tokens.

The OAuth provider issues bearer tokens at runtime when users complete the
authorization flow.  By default these live only in memory, so a server restart
forces every connected client to re-authorize.  ``SqliteTokenStore`` persists
them to a local SQLite file so they survive restarts.

Tokens are stored *hashed* (SHA-256) at rest — the raw bearer token only ever
exists in the response handed back to the client.  A leaked database file
therefore cannot be replayed against the server.

Only dynamically-issued OAuth tokens flow through a store.  Short-lived
authorization codes and static config-defined API tokens are deliberately kept
in memory: codes because they live for minutes, API tokens because config is
their source of truth (persisting them would let a token linger after it is
removed from the config).
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path

from auth_records import _StoredAccessToken, _StoredRefreshToken

LOG = logging.getLogger("superset_mcp.token_store")


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class TokenStore(ABC):
    """Interface for persisting OAuth access and refresh tokens."""

    @abstractmethod
    def put_access(self, rec: _StoredAccessToken) -> None: ...

    @abstractmethod
    def get_access(self, token: str) -> _StoredAccessToken | None: ...

    @abstractmethod
    def delete_access(self, token: str) -> None: ...

    @abstractmethod
    def put_refresh(self, rec: _StoredRefreshToken) -> None: ...

    @abstractmethod
    def get_refresh(self, token: str) -> _StoredRefreshToken | None: ...

    @abstractmethod
    def delete_refresh(self, token: str) -> None: ...

    def prune_expired(self) -> int:
        """Remove expired rows. Returns the number removed."""
        return 0

    def close(self) -> None:  # noqa: D401 - optional hook
        """Release any underlying resources."""


class InMemoryTokenStore(TokenStore):
    """Non-persistent store — preserves the original in-memory behaviour."""

    def __init__(self) -> None:
        self._access: dict[str, _StoredAccessToken] = {}
        self._refresh: dict[str, _StoredRefreshToken] = {}

    def put_access(self, rec: _StoredAccessToken) -> None:
        self._access[rec.token] = rec

    def get_access(self, token: str) -> _StoredAccessToken | None:
        rec = self._access.get(token)
        if rec is None:
            return None
        if time.time() > rec.expires_at:
            self._access.pop(token, None)
            return None
        return rec

    def delete_access(self, token: str) -> None:
        self._access.pop(token, None)

    def put_refresh(self, rec: _StoredRefreshToken) -> None:
        self._refresh[rec.token] = rec

    def get_refresh(self, token: str) -> _StoredRefreshToken | None:
        rec = self._refresh.get(token)
        if rec is None:
            return None
        if time.time() > rec.expires_at:
            self._refresh.pop(token, None)
            return None
        return rec

    def delete_refresh(self, token: str) -> None:
        self._refresh.pop(token, None)

    def prune_expired(self) -> int:
        now = time.time()
        removed = 0
        for store in (self._access, self._refresh):
            expired = [k for k, v in store.items() if now > v.expires_at]
            for k in expired:
                store.pop(k, None)
            removed += len(expired)
        return removed


class SqliteTokenStore(TokenStore):
    """SQLite-backed store keyed by the SHA-256 hash of each token.

    A single connection is shared across threads under a lock; token traffic is
    low volume so this is more than adequate and keeps writes atomic.
    """

    _SCHEMA = """
        CREATE TABLE IF NOT EXISTS oauth_tokens (
            token_hash TEXT NOT NULL,
            kind       TEXT NOT NULL,
            client_id  TEXT NOT NULL,
            scopes     TEXT NOT NULL,
            expires_at REAL NOT NULL,
            PRIMARY KEY (token_hash, kind)
        )
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path).expanduser()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self._path), check_same_thread=False, isolation_level=None
        )
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(self._SCHEMA)
        try:
            self._path.chmod(0o600)
        except OSError:
            LOG.warning("Could not set 0600 permissions on token DB %s", self._path)
        LOG.info("Persisting OAuth tokens to %s", self._path)

    # -- writes -------------------------------------------------------------

    def _put(self, kind: str, token: str, client_id: str, scopes: list[str], expires_at: float) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO oauth_tokens "
                "(token_hash, kind, client_id, scopes, expires_at) VALUES (?, ?, ?, ?, ?)",
                (_hash(token), kind, client_id, " ".join(scopes), expires_at),
            )

    def put_access(self, rec: _StoredAccessToken) -> None:
        self._put("access", rec.token, rec.client_id, rec.scopes, rec.expires_at)

    def put_refresh(self, rec: _StoredRefreshToken) -> None:
        self._put("refresh", rec.token, rec.client_id, rec.scopes, rec.expires_at)

    # -- reads --------------------------------------------------------------

    def _get(self, kind: str, token: str) -> tuple[str, list[str], float] | None:
        h = _hash(token)
        with self._lock:
            row = self._conn.execute(
                "SELECT client_id, scopes, expires_at FROM oauth_tokens "
                "WHERE token_hash = ? AND kind = ?",
                (h, kind),
            ).fetchone()
            if row is None:
                return None
            client_id, scopes_str, expires_at = row
            if time.time() > expires_at:
                self._conn.execute("DELETE FROM oauth_tokens WHERE token_hash = ?", (h,))
                return None
        scopes = scopes_str.split() if scopes_str else []
        return client_id, scopes, expires_at

    def get_access(self, token: str) -> _StoredAccessToken | None:
        row = self._get("access", token)
        if row is None:
            return None
        client_id, scopes, expires_at = row
        # The raw token is reconstructed from the caller's input — it is never
        # persisted, only its hash is.
        return _StoredAccessToken(token=token, client_id=client_id, scopes=scopes, expires_at=expires_at)

    def get_refresh(self, token: str) -> _StoredRefreshToken | None:
        row = self._get("refresh", token)
        if row is None:
            return None
        client_id, scopes, expires_at = row
        return _StoredRefreshToken(token=token, client_id=client_id, scopes=scopes, expires_at=expires_at)

    # -- deletes ------------------------------------------------------------

    def _delete(self, kind: str, token: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM oauth_tokens WHERE token_hash = ? AND kind = ?",
                (_hash(token), kind),
            )

    def delete_access(self, token: str) -> None:
        self._delete("access", token)

    def delete_refresh(self, token: str) -> None:
        self._delete("refresh", token)

    # -- housekeeping -------------------------------------------------------

    def prune_expired(self) -> int:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM oauth_tokens WHERE expires_at < ?", (time.time(),)
            )
            return cur.rowcount or 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()
