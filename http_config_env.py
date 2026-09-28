"""The HTTP and OAuth settings of this server, read from its environment.

esme_mcp takes an ``HTTPConfig``. The other MCP servers read one from the ``http`` section
of a YAML file; this one has always been configured through the EnvironmentFile its
systemd unit names, so the same model is built here from the same MCP_* variables that
file already sets. Nothing about how the server is deployed changes with the move to
esme_mcp.

The environment is passed in rather than read from ``os.environ``, so what a test or a
caller gets depends on what it hands over and on nothing else.
"""

from __future__ import annotations

from typing import List, Mapping, Optional

from esme_mcp.config import GoogleOAuthConfig, HTTPConfig, OAuthClientConfig

# The defaults this server had before the move, kept so an EnvironmentFile that relied on
# one keeps meaning the same thing.
DEFAULT_HTTP_HOST = "0.0.0.0"
DEFAULT_HTTP_PORT = 8044
DEFAULT_ISSUER_URL = "http://localhost:8044"


def _split_list(raw_value: str) -> List[str]:
    """Split a comma-separated variable into its non-empty, trimmed entries."""
    return [entry.strip() for entry in raw_value.split(",") if entry.strip()]


def _read_optional(environ: Mapping[str, str], name: str) -> Optional[str]:
    """The variable's value, or None when it is unset or empty."""
    value = environ.get(name, "").strip()
    return value or None


def _read_optional_int(environ: Mapping[str, str], name: str) -> Optional[int]:
    value = _read_optional(environ, name)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {value!r}") from None


def read_oauth_clients(raw_clients: str) -> List[OAuthClientConfig]:
    """Parse MCP_OAUTH_CLIENTS: comma-separated ``client_id:client_secret`` pairs.

    A pair without a colon is refused rather than skipped: skipping it is how a typo in
    the EnvironmentFile turns into a client that silently cannot log in.
    """
    clients = []
    for pair in _split_list(raw_clients):
        if ":" not in pair:
            raise ValueError("MCP_OAUTH_CLIENTS entries must be 'client_id:client_secret'")
        client_id, client_secret = pair.split(":", 1)
        clients.append(OAuthClientConfig(client_id=client_id.strip(), client_secret=client_secret.strip()))
    return clients


def read_google_oauth_config(environ: Mapping[str, str]) -> Optional[GoogleOAuthConfig]:
    """Google federation settings, or None when it is not configured.

    Federation is on when the client id, the secret and the domain are all set, as it was
    before the move. MCP_GOOGLE_REVALIDATE_SECONDS is what makes a removed Google account
    stop working; MCP_GOOGLE_GRANT_ENCRYPTION_KEY encrypts the grants that revalidation
    keeps in MCP_TOKEN_DB.
    """
    client_id = _read_optional(environ, "MCP_GOOGLE_CLIENT_ID")
    client_secret = _read_optional(environ, "MCP_GOOGLE_CLIENT_SECRET")
    allowed_domain = _read_optional(environ, "MCP_GOOGLE_ALLOWED_DOMAIN")
    if not (client_id and client_secret and allowed_domain):
        return None
    return GoogleOAuthConfig(
        client_id=client_id,
        client_secret=client_secret,
        allowed_domain=allowed_domain,
        allowed_emails=_split_list(environ.get("MCP_GOOGLE_ALLOWED_EMAILS", "")),
        revalidate_seconds=_read_optional_int(environ, "MCP_GOOGLE_REVALIDATE_SECONDS"),
        grant_encryption_key=_read_optional(environ, "MCP_GOOGLE_GRANT_ENCRYPTION_KEY"),
        max_grant_age_days=_read_optional_int(environ, "MCP_GOOGLE_MAX_GRANT_AGE_DAYS"),
    )


def read_http_config(environ: Mapping[str, str]) -> HTTPConfig:
    """Build the esme_mcp ``HTTPConfig`` from the MCP_* variables in *environ*."""
    allowed_ips = _split_list(environ.get("MCP_ALLOWED_IPS", ""))
    return HTTPConfig(
        host=_read_optional(environ, "MCP_HTTP_HOST") or DEFAULT_HTTP_HOST,
        port=_read_optional_int(environ, "MCP_HTTP_PORT") or DEFAULT_HTTP_PORT,
        issuer_url=_read_optional(environ, "MCP_ISSUER_URL") or DEFAULT_ISSUER_URL,
        clients=read_oauth_clients(environ.get("MCP_OAUTH_CLIENTS", "")),
        api_tokens=_split_list(environ.get("MCP_API_TOKENS", "")),
        token_db=_read_optional(environ, "MCP_TOKEN_DB"),
        tls_certfile=_read_optional(environ, "MCP_TLS_CERTFILE"),
        tls_keyfile=_read_optional(environ, "MCP_TLS_KEYFILE"),
        allowed_ips=allowed_ips or None,
        trusted_proxies=_split_list(environ.get("MCP_TRUSTED_PROXIES", "")),
        google_oauth=read_google_oauth_config(environ),
    )
