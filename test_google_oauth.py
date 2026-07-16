"""Tests for Google-federated authentication and the access policy."""

import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from auth import MCPOAuthProvider, OAuthClientEntry
from google_oauth import (
    GOOGLE_AUTH_ENDPOINT,
    GoogleAuthError,
    GoogleIdentity,
    GoogleOAuthConfig,
    build_authorization_url,
    check_identity_allowed,
)
from token_store import SqliteTokenStore


def _cfg(allowed_emails=None):
    return GoogleOAuthConfig(
        client_id="cid.apps.googleusercontent.com",
        client_secret="secret",
        allowed_domain="basebone.com",
        allowed_emails=allowed_emails or [],
    )


def _claims(**overrides):
    base = {
        "email": "user@basebone.com",
        "email_verified": True,
        "hd": "basebone.com",
        "sub": "12345",
        "iss": "https://accounts.google.com",
        "name": "Test User",
    }
    base.update(overrides)
    return base


class PolicyTests(unittest.TestCase):
    def test_accepts_verified_domain_user(self):
        identity = check_identity_allowed(_claims(), _cfg())
        self.assertEqual("user@basebone.com", identity.email)
        self.assertEqual("12345", identity.sub)

    def test_rejects_unverified_email(self):
        with self.assertRaises(GoogleAuthError):
            check_identity_allowed(_claims(email_verified=False), _cfg())

    def test_rejects_wrong_domain(self):
        with self.assertRaises(GoogleAuthError):
            check_identity_allowed(_claims(email="user@gmail.com", hd=None), _cfg())

    def test_rejects_domain_string_in_local_part(self):
        # An address like "basebone.com@evil.com" must not slip through.
        with self.assertRaises(GoogleAuthError):
            check_identity_allowed(_claims(email="basebone.com@evil.com", hd=None), _cfg())

    def test_rejects_hd_mismatch(self):
        # Email domain matches but the workspace hd claim does not.
        with self.assertRaises(GoogleAuthError):
            check_identity_allowed(_claims(hd="other.com"), _cfg())

    def test_allowlist_enforced(self):
        cfg = _cfg(allowed_emails=["boss@basebone.com"])
        with self.assertRaises(GoogleAuthError):
            check_identity_allowed(_claims(email="user@basebone.com"), cfg)
        # Listed user passes.
        identity = check_identity_allowed(_claims(email="boss@basebone.com"), cfg)
        self.assertEqual("boss@basebone.com", identity.email)

    def test_email_case_insensitive(self):
        identity = check_identity_allowed(_claims(email="User@Basebone.COM"), _cfg())
        self.assertEqual("user@basebone.com", identity.email)


class AuthorizationUrlTests(unittest.TestCase):
    def test_url_has_expected_params(self):
        url = build_authorization_url(_cfg(), "https://mcp.x/auth/google/callback", "nonce123")
        self.assertTrue(url.startswith(GOOGLE_AUTH_ENDPOINT))
        qs = parse_qs(urlparse(url).query)
        self.assertEqual(["nonce123"], qs["state"])
        self.assertEqual(["basebone.com"], qs["hd"])
        self.assertEqual(["code"], qs["response_type"])
        self.assertIn("openid", qs["scope"][0])
        self.assertEqual(["https://mcp.x/auth/google/callback"], qs["redirect_uri"])


class _FakeClient:
    """Minimal stand-in for OAuthClientInformationFull."""

    def __init__(self, client_id="superset-mcp"):
        self.client_id = client_id


class _Params:
    def __init__(self, redirect_uri, state="clientstate"):
        self.redirect_uri = redirect_uri
        self.redirect_uri_provided_explicitly = True
        self.code_challenge = "challenge"
        self.scopes = ["claudeai"]
        self.state = state


class ProviderFederationTests(unittest.TestCase):
    def _provider(self, store=None):
        return MCPOAuthProvider(
            clients=[OAuthClientEntry(client_id="superset-mcp", client_secret="s")],
            google_oauth=_cfg(),
            google_redirect_uri="https://mcp.x/auth/google/callback",
            **({"token_store": store} if store else {}),
        )

    def test_authorize_redirects_to_google(self):
        provider = self._provider()
        url = asyncio.run(provider.authorize(_FakeClient(), _Params("https://claude.ai/cb")))
        self.assertTrue(url.startswith(GOOGLE_AUTH_ENDPOINT))
        # A pending authorization was parked under the nonce (the Google state).
        nonce = parse_qs(urlparse(url).query)["state"][0]
        self.assertIn(nonce, provider._pending)

    def test_callback_completion_binds_identity_to_token(self):
        provider = self._provider()
        url = asyncio.run(provider.authorize(_FakeClient(), _Params("https://claude.ai/cb")))
        nonce = parse_qs(urlparse(url).query)["state"][0]

        identity = GoogleIdentity(email="user@basebone.com", sub="1", hd="basebone.com")
        redirect = provider.complete_google_authorization(nonce, identity)

        # Redirect goes back to the original client with a code + preserved state.
        self.assertTrue(redirect.startswith("https://claude.ai/cb"))
        qs = parse_qs(urlparse(redirect).query)
        self.assertEqual(["clientstate"], qs["state"])
        code = qs["code"][0]

        # Exchanging the code yields a token stamped with the user's email.
        client = _FakeClient()
        ac = asyncio.run(provider.load_authorization_code(client, code))
        token = asyncio.run(provider.exchange_authorization_code(client, ac))
        loaded = asyncio.run(provider.load_access_token(token.access_token))
        self.assertEqual("user@basebone.com", loaded.user_email)

    def test_expired_nonce_rejected(self):
        provider = self._provider()
        url = asyncio.run(provider.authorize(_FakeClient(), _Params("https://claude.ai/cb")))
        nonce = parse_qs(urlparse(url).query)["state"][0]
        # Force-expire the pending entry.
        provider._pending[nonce].expires_at = time.time() - 1
        with self.assertRaises(KeyError):
            provider.complete_google_authorization(
                nonce, GoogleIdentity(email="user@basebone.com", sub="1")
            )

    def test_identity_survives_refresh_rotation(self):
        provider = self._provider()
        url = asyncio.run(provider.authorize(_FakeClient(), _Params("https://claude.ai/cb")))
        nonce = parse_qs(urlparse(url).query)["state"][0]
        redirect = provider.complete_google_authorization(
            nonce, GoogleIdentity(email="user@basebone.com", sub="1")
        )
        code = parse_qs(urlparse(redirect).query)["code"][0]
        client = _FakeClient()
        ac = asyncio.run(provider.load_authorization_code(client, code))
        token = asyncio.run(provider.exchange_authorization_code(client, ac))

        rt = asyncio.run(provider.load_refresh_token(client, token.refresh_token))
        rotated = asyncio.run(provider.exchange_refresh_token(client, rt, []))
        loaded = asyncio.run(provider.load_access_token(rotated.access_token))
        self.assertEqual("user@basebone.com", loaded.user_email)

    def test_identity_persists_across_restart(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "tokens.db"

            provider = self._provider(store=SqliteTokenStore(path))
            url = asyncio.run(provider.authorize(_FakeClient(), _Params("https://claude.ai/cb")))
            nonce = parse_qs(urlparse(url).query)["state"][0]
            redirect = provider.complete_google_authorization(
                nonce, GoogleIdentity(email="user@basebone.com", sub="1")
            )
            code = parse_qs(urlparse(redirect).query)["code"][0]
            client = _FakeClient()
            ac = asyncio.run(provider.load_authorization_code(client, code))
            token = asyncio.run(provider.exchange_authorization_code(client, ac))
            provider.token_store.close()

            # Simulate a restart: new provider, same DB.
            restarted = self._provider(store=SqliteTokenStore(path))
            loaded = asyncio.run(restarted.load_access_token(token.access_token))
            self.assertIsNotNone(loaded)
            self.assertEqual("user@basebone.com", loaded.user_email)

    def test_no_google_config_falls_back_to_auto_approve(self):
        # Backwards compatibility: without google_oauth, authorize() returns the
        # client redirect directly (no Google hop).
        provider = MCPOAuthProvider(
            clients=[OAuthClientEntry(client_id="superset-mcp", client_secret="s")],
        )
        url = asyncio.run(provider.authorize(_FakeClient(), _Params("https://claude.ai/cb")))
        self.assertTrue(url.startswith("https://claude.ai/cb"))
        self.assertIn("code=", url)


if __name__ == "__main__":
    unittest.main()
