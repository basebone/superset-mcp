"""In-memory OAuth 2.0 authorization server provider for MCP servers.

Implements the MCP SDK's OAuthAuthorizationServerProvider protocol with a simple
in-memory store.  Clients are pre-registered via configuration (client_id +
client_secret); dynamic registration is disabled.
"""

from __future__ import annotations

import logging
import secrets
import time
from dataclasses import dataclass, field

from mcp.server.auth.middleware.bearer_auth import AccessToken
from pydantic import AnyUrl

from mcp.server.auth.provider import (
    AuthorizationCode,
    AuthorizationParams,
    OAuthClientInformationFull,
    OAuthToken,
)

from auth_records import _StoredAccessToken, _StoredRefreshToken
from google_oauth import GoogleIdentity, GoogleOAuthConfig, build_authorization_url
from token_store import InMemoryTokenStore, TokenStore


LOG = logging.getLogger("superset_mcp.auth")


class SupersetAccessToken(AccessToken):
    """Access token that also carries the authenticated user's email.

    Returned from :meth:`MCPOAuthProvider.load_access_token` so tool handlers
    can attribute (and audit) requests to a specific Google user via
    ``mcp.server.auth.middleware.auth_context.get_access_token()``.
    """

    user_email: str | None = None

# Token lifetimes ---------------------------------------------------------
ACCESS_TOKEN_TTL = 3600  # 1 hour
REFRESH_TOKEN_TTL = 86400 * 30  # 30 days
AUTH_CODE_TTL = 300  # 5 minutes


def _random_token(nbytes: int = 32) -> str:
    return secrets.token_urlsafe(nbytes)


# Internal bookkeeping records live in auth_records (imported above) so they can
# be shared with token_store without a circular import.


# Provider -----------------------------------------------------------------

@dataclass
class OAuthClientEntry:
    """A client_id / client_secret pair to pre-register."""
    client_id: str
    client_secret: str


@dataclass(slots=True)
class _PendingAuthorization:
    """An MCP authorization request parked while the user signs in with Google.

    Keyed by a random ``nonce`` that is round-tripped as the Google ``state``
    parameter, so the callback can resume the original request.
    """
    client_id: str
    redirect_uri: AnyUrl
    redirect_uri_provided_explicitly: bool
    code_challenge: str
    scopes: list[str]
    client_state: str | None
    expires_at: float


@dataclass
class MCPOAuthProvider:
    """Minimal OAuth 2.0 AS provider backed by in-memory stores.

    Pre-seeds one or more clients from the supplied *clients* list.
    """

    clients: list[OAuthClientEntry] = field(default_factory=list)
    api_tokens: list[str] = field(default_factory=list)
    # Persistence backend for OAuth-issued access/refresh tokens.  Defaults to a
    # non-persistent in-memory store; pass a SqliteTokenStore to survive restarts.
    token_store: TokenStore = field(default_factory=InMemoryTokenStore)
    # When set, user authentication is federated to Google instead of auto-approved.
    google_oauth: GoogleOAuthConfig | None = None
    # Public URL Google redirects back to after login (issuer_url + callback path).
    google_redirect_uri: str | None = None

    # internal stores (keyed by token/code string)
    _clients: dict[str, OAuthClientInformationFull] = field(default_factory=dict, repr=False)
    _auth_codes: dict[str, AuthorizationCode] = field(default_factory=dict, repr=False)
    # Static config-defined API tokens are kept in memory (config is their source
    # of truth) and are never written to the persistent store.
    _api_tokens: dict[str, _StoredAccessToken] = field(default_factory=dict, repr=False)
    # Authorizations parked mid-flight while the user signs in with Google.
    _pending: dict[str, _PendingAuthorization] = field(default_factory=dict, repr=False)
    # Maps a freshly-issued authorization code to the Google-verified email, so
    # the identity can be stamped onto the tokens minted at code exchange.
    _code_identity: dict[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        # Seed pre-configured static API tokens as permanent access tokens.
        for token in self.api_tokens:
            self._api_tokens[token] = _StoredAccessToken(
                token=token,
                client_id="api-token",
                scopes=[],
                expires_at=float("inf"),  # never expires
            )
        if self.api_tokens:
            LOG.info("Loaded %d static API token(s)", len(self.api_tokens))

        # Drop any tokens that expired while the server was down.
        pruned = self.token_store.prune_expired()
        if pruned:
            LOG.info("Pruned %d expired persisted token(s)", pruned)

        # Seed the pre-configured OAuth clients.
        for entry in self.clients:
            self._clients[entry.client_id] = OAuthClientInformationFull(
                client_id=entry.client_id,
                client_secret=entry.client_secret,
                client_id_issued_at=int(time.time()),
                client_secret_expires_at=0,  # never expires
                redirect_uris=[
                    "http://localhost:0/callback",
                    "https://callback.mistral.ai/v1/integrations_auth/oauth2_callback",
                    "https://claude.ai/api/mcp/auth_callback",
                    "https://chatgpt.com/aip/g-callback",
                    "https://chatgpt.com/api/mcp/auth_callback",
                    "https://chat.openai.com/aip/g-callback",
                    "https://gemini.google.com/mcp/callback",
                ],
                token_endpoint_auth_method="client_secret_post",
                scope="claudeai openai mistral",
                grant_types=["authorization_code", "refresh_token"],
                response_types=["code"],
                client_name=f"mcp-{entry.client_id}",
            )
        LOG.info("Registered %d OAuth client(s)", len(self.clients))

    # -- Client management --------------------------------------------------

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        return self._clients.get(client_id)

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        raise NotImplementedError("Dynamic client registration is disabled")

    # -- Authorization ------------------------------------------------------

    async def authorize(
        self,
        client: OAuthClientInformationFull,
        params: AuthorizationParams,
    ) -> str:
        """Begin authorization for *client*.

        When Google federation is configured, park the request and redirect the
        user to Google's consent screen; the flow resumes in
        :meth:`complete_google_authorization` once Google calls back.  Otherwise
        fall back to auto-approval (redirect straight back with a code).
        """
        if self.google_oauth is not None:
            if self.google_redirect_uri is None:
                raise RuntimeError("google_redirect_uri must be set when google_oauth is configured")
            # Drop abandoned logins so the pending map cannot grow unbounded.
            now = time.time()
            self._pending = {n: p for n, p in self._pending.items() if p.expires_at > now}
            nonce = _random_token()
            self._pending[nonce] = _PendingAuthorization(
                client_id=client.client_id,
                redirect_uri=params.redirect_uri,
                redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
                code_challenge=params.code_challenge,
                scopes=params.scopes or [],
                client_state=params.state,
                expires_at=time.time() + AUTH_CODE_TTL,
            )
            LOG.info("Redirecting client %s to Google for user authentication", client.client_id)
            return build_authorization_url(self.google_oauth, self.google_redirect_uri, nonce)

        # No federation configured: auto-approve (redirect_uri already validated).
        return self._issue_code_redirect(
            client_id=client.client_id,
            redirect_uri=params.redirect_uri,
            redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
            code_challenge=params.code_challenge,
            scopes=params.scopes or [],
            client_state=params.state,
            user_email=None,
        )

    def _issue_code_redirect(
        self,
        *,
        client_id: str,
        redirect_uri: AnyUrl,
        redirect_uri_provided_explicitly: bool,
        code_challenge: str,
        scopes: list[str],
        client_state: str | None,
        user_email: str | None,
    ) -> str:
        """Mint an authorization code and build the client redirect URL."""
        code = _random_token()
        self._auth_codes[code] = AuthorizationCode(
            code=code,
            scopes=scopes,
            expires_at=time.time() + AUTH_CODE_TTL,
            client_id=client_id,
            code_challenge=code_challenge,
            redirect_uri=redirect_uri,
            redirect_uri_provided_explicitly=redirect_uri_provided_explicitly,
        )
        if user_email is not None:
            self._code_identity[code] = user_email
        LOG.debug("Issued authorization code for client %s", client_id)

        redirect = str(redirect_uri)
        sep = "&" if "?" in redirect else "?"
        redirect += f"{sep}code={code}"
        if client_state:
            redirect += f"&state={client_state}"
        return redirect

    def complete_google_authorization(self, nonce: str, identity: GoogleIdentity) -> str:
        """Resume a parked authorization after a successful Google login.

        Returns the redirect URL back to the original MCP client, now carrying a
        code bound to the verified user's identity.  Raises ``KeyError`` if the
        nonce is unknown or expired.
        """
        pending = self._pending.pop(nonce, None)
        if pending is None or time.time() > pending.expires_at:
            raise KeyError("Unknown or expired authorization request")
        LOG.info("Google login succeeded for %s (client %s)", identity.email, pending.client_id)
        return self._issue_code_redirect(
            client_id=pending.client_id,
            redirect_uri=pending.redirect_uri,
            redirect_uri_provided_explicitly=pending.redirect_uri_provided_explicitly,
            code_challenge=pending.code_challenge,
            scopes=pending.scopes,
            client_state=pending.client_state,
            user_email=identity.email,
        )

    async def load_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: str,
    ) -> AuthorizationCode | None:
        ac = self._auth_codes.get(authorization_code)
        if ac is None or ac.client_id != client.client_id:
            return None
        if time.time() > ac.expires_at:
            self._auth_codes.pop(authorization_code, None)
            return None
        return ac

    async def exchange_authorization_code(
        self,
        client: OAuthClientInformationFull,
        authorization_code: AuthorizationCode,
    ) -> OAuthToken:
        # Consume the code (one-time use) and its identity binding.
        self._auth_codes.pop(authorization_code.code, None)
        user_email = self._code_identity.pop(authorization_code.code, None)

        access = _random_token()
        refresh = _random_token()
        now = time.time()

        self.token_store.put_access(_StoredAccessToken(
            token=access,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=now + ACCESS_TOKEN_TTL,
            user_email=user_email,
        ))
        self.token_store.put_refresh(_StoredRefreshToken(
            token=refresh,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=now + REFRESH_TOKEN_TTL,
            user_email=user_email,
        ))
        LOG.info("Issued access token for client %s (user=%s)", client.client_id, user_email or "-")
        return OAuthToken(
            access_token=access,
            token_type="Bearer",
            expires_in=ACCESS_TOKEN_TTL,
            refresh_token=refresh,
            scope=" ".join(authorization_code.scopes) if authorization_code.scopes else None,
        )

    # -- Refresh tokens -----------------------------------------------------

    async def load_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: str,
    ) -> _StoredRefreshToken | None:
        rt = self.token_store.get_refresh(refresh_token)
        if rt is None or rt.client_id != client.client_id:
            return None
        return rt

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: _StoredRefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        # Rotate tokens.
        self.token_store.delete_refresh(refresh_token.token)

        access = _random_token()
        new_refresh = _random_token()
        now = time.time()
        effective_scopes = scopes or refresh_token.scopes
        # Preserve the user identity across rotation.
        user_email = refresh_token.user_email

        self.token_store.put_access(_StoredAccessToken(
            token=access,
            client_id=client.client_id,
            scopes=effective_scopes,
            expires_at=now + ACCESS_TOKEN_TTL,
            user_email=user_email,
        ))
        self.token_store.put_refresh(_StoredRefreshToken(
            token=new_refresh,
            client_id=client.client_id,
            scopes=effective_scopes,
            expires_at=now + REFRESH_TOKEN_TTL,
            user_email=user_email,
        ))
        LOG.info("Rotated tokens for client %s (user=%s)", client.client_id, user_email or "-")
        return OAuthToken(
            access_token=access,
            token_type="Bearer",
            expires_in=ACCESS_TOKEN_TTL,
            refresh_token=new_refresh,
            scope=" ".join(effective_scopes) if effective_scopes else None,
        )

    # -- Access token verification ------------------------------------------

    async def load_access_token(self, token: str) -> AccessToken | None:
        # Static config-defined API tokens take precedence and never expire.
        stored = self._api_tokens.get(token)
        if stored is None:
            stored = self.token_store.get_access(token)
        if stored is None:
            return None
        return SupersetAccessToken(
            token=stored.token,
            client_id=stored.client_id,
            scopes=stored.scopes,
            expires_at=int(stored.expires_at) if stored.expires_at != float("inf") else int(time.time()) + 86400 * 365 * 100,
            user_email=stored.user_email,
        )

    # -- Revocation ---------------------------------------------------------

    async def revoke_token(
        self,
        token: _StoredAccessToken | _StoredRefreshToken,
    ) -> None:
        if isinstance(token, _StoredAccessToken):
            self.token_store.delete_access(token.token)
        elif isinstance(token, _StoredRefreshToken):
            self.token_store.delete_refresh(token.token)
        LOG.info("Revoked token for client %s", token.client_id)
