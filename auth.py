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

from mcp.server.auth.provider import (
    AuthorizationCode,
    AuthorizationParams,
    OAuthClientInformationFull,
    OAuthToken,
)

from auth_records import _StoredAccessToken, _StoredRefreshToken
from token_store import InMemoryTokenStore, TokenStore


LOG = logging.getLogger("superset_mcp.auth")

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

    # internal stores (keyed by token/code string)
    _clients: dict[str, OAuthClientInformationFull] = field(default_factory=dict, repr=False)
    _auth_codes: dict[str, AuthorizationCode] = field(default_factory=dict, repr=False)
    # Static config-defined API tokens are kept in memory (config is their source
    # of truth) and are never written to the persistent store.
    _api_tokens: dict[str, _StoredAccessToken] = field(default_factory=dict, repr=False)

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
        """Issue an authorization code and redirect back immediately.

        Since this server has no interactive login UI we auto-approve the
        request and redirect straight to the client's redirect_uri with the
        code attached.
        """
        code = _random_token()
        self._auth_codes[code] = AuthorizationCode(
            code=code,
            scopes=params.scopes or [],
            expires_at=time.time() + AUTH_CODE_TTL,
            client_id=client.client_id,
            code_challenge=params.code_challenge,
            redirect_uri=params.redirect_uri,
            redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
        )
        LOG.debug("Issued authorization code for client %s", client.client_id)

        # Build the redirect URL with the code and state.
        redirect = str(params.redirect_uri)
        sep = "&" if "?" in redirect else "?"
        redirect += f"{sep}code={code}"
        if params.state:
            redirect += f"&state={params.state}"
        return redirect

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
        # Consume the code (one-time use).
        self._auth_codes.pop(authorization_code.code, None)

        access = _random_token()
        refresh = _random_token()
        now = time.time()

        self.token_store.put_access(_StoredAccessToken(
            token=access,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=now + ACCESS_TOKEN_TTL,
        ))
        self.token_store.put_refresh(_StoredRefreshToken(
            token=refresh,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=now + REFRESH_TOKEN_TTL,
        ))
        LOG.info("Issued access token for client %s", client.client_id)
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

        self.token_store.put_access(_StoredAccessToken(
            token=access,
            client_id=client.client_id,
            scopes=effective_scopes,
            expires_at=now + ACCESS_TOKEN_TTL,
        ))
        self.token_store.put_refresh(_StoredRefreshToken(
            token=new_refresh,
            client_id=client.client_id,
            scopes=effective_scopes,
            expires_at=now + REFRESH_TOKEN_TTL,
        ))
        LOG.info("Rotated tokens for client %s", client.client_id)
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
        return AccessToken(
            token=stored.token,
            client_id=stored.client_id,
            scopes=stored.scopes,
            expires_at=int(stored.expires_at) if stored.expires_at != float("inf") else int(time.time()) + 86400 * 365 * 100,
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
