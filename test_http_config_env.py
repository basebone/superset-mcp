"""The MCP_* variables are read into the HTTPConfig esme_mcp serves with."""

from pathlib import Path

import pytest

from http_config_env import read_http_config

BASE_ENVIRONMENT = {
    "MCP_OAUTH_CLIENTS": "superset-mcp:client-secret",
}

GOOGLE_ENVIRONMENT = {
    **BASE_ENVIRONMENT,
    "MCP_GOOGLE_CLIENT_ID": "google-client-id",
    "MCP_GOOGLE_CLIENT_SECRET": "google-client-secret",
    "MCP_GOOGLE_ALLOWED_DOMAIN": "basebone.com",
}


def test_every_variable_reaches_the_config():
    http_config = read_http_config({
        **GOOGLE_ENVIRONMENT,
        "MCP_HTTP_HOST": "127.0.0.1",
        "MCP_HTTP_PORT": "8045",
        "MCP_ISSUER_URL": "https://smcp.example",
        "MCP_OAUTH_CLIENTS": "first:secret-1, second:secret:with:colons",
        "MCP_API_TOKENS": "token-a, token-b,",
        "MCP_TOKEN_DB": "/tmp/tokens.db",
        "MCP_ALLOWED_IPS": "10.0.0.0/8",
        "MCP_TRUSTED_PROXIES": "10.0.6.1,10.0.2.10",
        "MCP_GOOGLE_ALLOWED_EMAILS": "A@basebone.com, b@basebone.com",
        "MCP_GOOGLE_REVALIDATE_SECONDS": "300",
        "MCP_GOOGLE_GRANT_ENCRYPTION_KEY": "fernet-key",
        "MCP_GOOGLE_MAX_GRANT_AGE_DAYS": "30",
    })

    assert http_config.host == "127.0.0.1"
    assert http_config.port == 8045
    assert http_config.issuer_url == "https://smcp.example"
    assert [(c.client_id, c.client_secret) for c in http_config.clients] == [
        ("first", "secret-1"),
        ("second", "secret:with:colons"),
    ]
    assert http_config.api_tokens == ["token-a", "token-b"]
    assert http_config.token_db == Path("/tmp/tokens.db")
    assert http_config.allowed_ips == ["10.0.0.0/8"]
    assert http_config.trusted_proxies == ["10.0.6.1", "10.0.2.10"]

    google = http_config.google_oauth
    assert google.allowed_domain == "basebone.com"
    assert google.allowed_emails == ["a@basebone.com", "b@basebone.com"]
    assert google.revalidate_seconds == 300
    assert google.grant_encryption_key == "fernet-key"
    assert google.max_grant_age_days == 30


def test_defaults_are_the_ones_the_server_had_before():
    http_config = read_http_config(BASE_ENVIRONMENT)

    assert http_config.host == "0.0.0.0"
    assert http_config.port == 8044
    assert http_config.issuer_url == "http://localhost:8044"
    assert http_config.api_tokens == []
    assert http_config.token_db is None
    assert http_config.allowed_ips is None
    assert http_config.google_oauth is None


@pytest.mark.parametrize("missing", [
    "MCP_GOOGLE_CLIENT_ID", "MCP_GOOGLE_CLIENT_SECRET", "MCP_GOOGLE_ALLOWED_DOMAIN",
])
def test_google_federation_needs_id_secret_and_domain(missing):
    environment = {k: v for k, v in GOOGLE_ENVIRONMENT.items() if k != missing}

    assert read_http_config(environment).google_oauth is None


def test_revalidation_with_a_token_db_refuses_to_start_without_a_key():
    with pytest.raises(ValueError, match="grant_encryption_key"):
        read_http_config({
            **GOOGLE_ENVIRONMENT,
            "MCP_TOKEN_DB": "/tmp/tokens.db",
            "MCP_GOOGLE_REVALIDATE_SECONDS": "300",
        })


def test_a_client_without_a_secret_is_refused():
    with pytest.raises(ValueError, match="client_id:client_secret"):
        read_http_config({"MCP_OAUTH_CLIENTS": "superset-mcp"})


def test_no_client_at_all_is_refused():
    with pytest.raises(ValueError, match="at least one OAuth client"):
        read_http_config({})


def test_a_non_numeric_port_names_the_variable():
    with pytest.raises(ValueError, match="MCP_HTTP_PORT"):
        read_http_config({**BASE_ENVIRONMENT, "MCP_HTTP_PORT": "eighty"})
