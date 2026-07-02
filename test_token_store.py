"""Tests for OAuth token persistence stores."""

import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from auth import MCPOAuthProvider, OAuthClientEntry
from auth_records import _StoredAccessToken, _StoredRefreshToken
from token_store import (
    InMemoryTokenStore,
    SqliteTokenStore,
    _hash,
)


def _access(token="a-tok", client="c1", scopes=None, ttl=3600):
    return _StoredAccessToken(
        token=token,
        client_id=client,
        scopes=scopes or ["read"],
        expires_at=time.time() + ttl,
    )


def _refresh(token="r-tok", client="c1", scopes=None, ttl=86400):
    return _StoredRefreshToken(
        token=token,
        client_id=client,
        scopes=scopes or ["read"],
        expires_at=time.time() + ttl,
    )


class _StoreContractMixin:
    """Behaviour both store implementations must satisfy."""

    def make_store(self):  # pragma: no cover - overridden
        raise NotImplementedError

    def test_access_roundtrip(self):
        store = self.make_store()
        store.put_access(_access())
        got = store.get_access("a-tok")
        self.assertIsNotNone(got)
        self.assertEqual("a-tok", got.token)
        self.assertEqual("c1", got.client_id)
        self.assertEqual(["read"], got.scopes)

    def test_refresh_roundtrip(self):
        store = self.make_store()
        store.put_refresh(_refresh(scopes=["read", "write"]))
        got = store.get_refresh("r-tok")
        self.assertIsNotNone(got)
        self.assertEqual(["read", "write"], got.scopes)

    def test_missing_token_returns_none(self):
        store = self.make_store()
        self.assertIsNone(store.get_access("nope"))
        self.assertIsNone(store.get_refresh("nope"))

    def test_kind_is_isolated(self):
        store = self.make_store()
        store.put_access(_access(token="shared"))
        store.put_refresh(_refresh(token="shared"))
        # A token stored as one kind must not be readable as the other's twin
        # unless it was explicitly stored that way.
        self.assertIsNotNone(store.get_access("shared"))
        self.assertIsNotNone(store.get_refresh("shared"))
        store.delete_access("shared")
        self.assertIsNone(store.get_access("shared"))
        self.assertIsNotNone(store.get_refresh("shared"))

    def test_delete(self):
        store = self.make_store()
        store.put_access(_access())
        store.delete_access("a-tok")
        self.assertIsNone(store.get_access("a-tok"))

    def test_expired_access_is_dropped(self):
        store = self.make_store()
        store.put_access(_access(ttl=-1))
        self.assertIsNone(store.get_access("a-tok"))

    def test_prune_expired(self):
        store = self.make_store()
        store.put_access(_access(token="live", ttl=3600))
        store.put_access(_access(token="dead", ttl=-1))
        store.put_refresh(_refresh(token="dead-r", ttl=-1))
        removed = store.prune_expired()
        self.assertEqual(2, removed)
        self.assertIsNotNone(store.get_access("live"))


class InMemoryStoreTests(_StoreContractMixin, unittest.TestCase):
    def make_store(self):
        return InMemoryTokenStore()


class SqliteStoreTests(_StoreContractMixin, unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.path = Path(self._dir.name) / "tokens.db"

    def tearDown(self):
        self._dir.cleanup()

    def make_store(self):
        return SqliteTokenStore(self.path)

    def test_persists_across_reopen(self):
        store = self.make_store()
        store.put_refresh(_refresh(ttl=86400))
        store.close()

        reopened = SqliteTokenStore(self.path)
        got = reopened.get_refresh("r-tok")
        self.assertIsNotNone(got)
        self.assertEqual("c1", got.client_id)

    def _all_db_bytes(self) -> bytes:
        # WAL mode writes to sidecar files, so scan the whole family.
        data = b""
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(self.path) + suffix)
            if p.exists():
                data += p.read_bytes()
        return data

    def test_raw_token_not_stored(self):
        store = self.make_store()
        store.put_access(_access(token="super-secret-token"))
        contents = self._all_db_bytes()
        self.assertNotIn(b"super-secret-token", contents)
        # The hash, however, is present.
        self.assertIn(_hash("super-secret-token").encode(), contents)

    def test_file_permissions_are_owner_only(self):
        store = self.make_store()
        store.put_access(_access())
        mode = self.path.stat().st_mode & 0o777
        self.assertEqual(0o600, mode)


class ProviderPersistenceTests(unittest.TestCase):
    """End-to-end: tokens issued by the provider survive a 'restart'."""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.path = Path(self._dir.name) / "tokens.db"

    def tearDown(self):
        self._dir.cleanup()

    def _make_provider(self):
        return MCPOAuthProvider(
            clients=[OAuthClientEntry(client_id="c1", client_secret="s")],
            token_store=SqliteTokenStore(self.path),
        )

    def test_access_token_survives_restart(self):
        async def scenario():
            provider = self._make_provider()
            provider.token_store.put_access(_access(client="c1"))
            provider.token_store.close()

            # Simulate a restart: brand new provider, same DB file.
            restarted = self._make_provider()
            return await restarted.load_access_token("a-tok")

        loaded = asyncio.run(scenario())
        self.assertIsNotNone(loaded)
        self.assertEqual("c1", loaded.client_id)

    def test_api_tokens_are_not_persisted(self):
        # Static API tokens come from config and must not leak into the DB.
        provider = MCPOAuthProvider(
            clients=[OAuthClientEntry(client_id="c1", client_secret="s")],
            api_tokens=["static-api-token"],
            token_store=SqliteTokenStore(self.path),
        )
        loaded = asyncio.run(provider.load_access_token("static-api-token"))
        self.assertIsNotNone(loaded)
        # Not written to disk.
        self.assertNotIn(_hash("static-api-token").encode(), self.path.read_bytes())


if __name__ == "__main__":
    unittest.main()
