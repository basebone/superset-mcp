"""Helpers for federating user authentication to Google (Workspace).

The MCP server remains the OAuth authorization server that its clients (Claude,
ChatGPT, ...) talk to, but it delegates the actual *user* login to Google: it
acts as an OAuth client of Google, then verifies the returned ID token and
enforces a domain / allow-list policy before issuing its own MCP token.

Network/library use is confined to :func:`build_authorization_url`,
:func:`exchange_code`, and :func:`verify_id_token`.  The access-policy decision
lives in :func:`check_identity_allowed`, a pure function that is unit-tested
without any Google dependency.
"""

from __future__ import annotations

import logging
import urllib.parse
from dataclasses import dataclass, field

LOG = logging.getLogger("superset_mcp.google_oauth")

GOOGLE_AUTH_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"

# Accepted issuer values for Google-signed ID tokens.
_VALID_ISSUERS = {"accounts.google.com", "https://accounts.google.com"}


@dataclass(frozen=True)
class GoogleOAuthConfig:
    """Federated authentication against Google (Workspace).

    When present, the server stops auto-approving authorization requests and
    instead redirects the user to Google to sign in.  Only users whose verified
    email is in ``allowed_domain`` (and, if set, ``allowed_emails``) are granted
    an MCP token.
    """

    client_id: str
    client_secret: str
    allowed_domain: str
    allowed_emails: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        # Normalise the domain and allow-list so comparisons are case-insensitive.
        object.__setattr__(self, "allowed_domain", self.allowed_domain.strip().lower().lstrip("@"))
        object.__setattr__(self, "allowed_emails", [e.strip().lower() for e in self.allowed_emails])


class GoogleAuthError(Exception):
    """Raised when Google authentication or the access policy fails."""


@dataclass(frozen=True)
class GoogleIdentity:
    """The verified identity of a user who signed in with Google."""

    email: str
    sub: str
    hd: str | None = None
    name: str | None = None


def build_authorization_url(
    config: GoogleOAuthConfig,
    redirect_uri: str,
    state: str,
) -> str:
    """Return the Google consent-screen URL to redirect the user's browser to.

    ``hd`` pre-filters the account chooser to the workspace domain; it is only a
    hint, so the domain is re-checked server-side in :func:`check_identity_allowed`.
    """
    params = {
        "client_id": config.client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "hd": config.allowed_domain,
        "access_type": "online",
        "prompt": "select_account",
    }
    return f"{GOOGLE_AUTH_ENDPOINT}?{urllib.parse.urlencode(params)}"


def exchange_code(config: GoogleOAuthConfig, code: str, redirect_uri: str) -> str:
    """Exchange a Google authorization code for an ID token (JWT).

    Returns the raw ``id_token`` string; the caller verifies it.
    """
    import requests  # local import so the dependency is only needed at runtime

    resp = requests.post(
        GOOGLE_TOKEN_ENDPOINT,
        data={
            "code": code,
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
        timeout=10,
    )
    if resp.status_code != 200:
        raise GoogleAuthError("Google token exchange failed")
    id_token = resp.json().get("id_token")
    if not id_token:
        raise GoogleAuthError("Google token response contained no id_token")
    return id_token


def verify_id_token(config: GoogleOAuthConfig, id_token: str) -> dict:
    """Verify a Google ID token's signature and standard claims.

    Returns the decoded claims on success; raises :class:`GoogleAuthError`
    otherwise.  Signature, ``aud``, and ``exp`` are checked by the library;
    ``iss`` is checked here.
    """
    from google.auth.transport import requests as google_requests
    from google.oauth2 import id_token as google_id_token

    try:
        claims = google_id_token.verify_oauth2_token(
            id_token,
            google_requests.Request(),
            audience=config.client_id,
        )
    except ValueError as exc:  # invalid signature, audience, or expiry
        raise GoogleAuthError("Google ID token verification failed") from exc

    if claims.get("iss") not in _VALID_ISSUERS:
        raise GoogleAuthError("Unexpected ID token issuer")
    return claims


def check_identity_allowed(claims: dict, config: GoogleOAuthConfig) -> GoogleIdentity:
    """Enforce the access policy on verified ID-token claims.

    A user is allowed when their email is verified, its domain matches
    ``allowed_domain`` (cross-checked against the ``hd`` claim when present),
    and — if an allow-list is configured — the email is on it.

    Raises :class:`GoogleAuthError` on any failure; returns the identity on success.
    """
    email = (claims.get("email") or "").strip().lower()
    if not email:
        raise GoogleAuthError("ID token has no email")
    if not claims.get("email_verified"):
        raise GoogleAuthError(f"Email {email} is not verified by Google")

    domain = email.rpartition("@")[2]
    if domain != config.allowed_domain:
        raise GoogleAuthError(
            f"Email domain '{domain}' is not permitted (expected '{config.allowed_domain}')"
        )

    # The hd claim, when present, must also match — guards against a personal
    # account whose email merely happens to share the domain string.
    hd = claims.get("hd")
    if hd is not None and hd.strip().lower() != config.allowed_domain:
        raise GoogleAuthError("Google account is not part of the expected workspace")

    if config.allowed_emails and email not in config.allowed_emails:
        raise GoogleAuthError(f"Email {email} is not on the allow-list")

    return GoogleIdentity(
        email=email,
        sub=str(claims.get("sub", "")),
        hd=hd,
        name=claims.get("name"),
    )
