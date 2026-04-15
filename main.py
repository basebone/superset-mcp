from typing import (
    Any,
    Dict,
    List,
    Optional,
    AsyncIterator,
    Callable,
    TypeVar,
    Awaitable,
    Union,
)
import argparse
import ipaddress
import os
import httpx
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import wraps
import inspect
from threading import Thread
import webbrowser
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from mcp.server.fastmcp import FastMCP, Context
from dotenv import load_dotenv
import json
import logging
import jwt
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

"""
Superset MCP Integration

This module provides a Model Control Protocol (MCP) server for Apache Superset,
enabling AI assistants to interact with and control a Superset instance programmatically.

It includes tools for:
- Authentication and token management
- Dashboard operations (list, get, create, update, delete)
- Chart management (list, get, create, update, delete)
- Database and dataset operations
- SQL execution and query management
- User information and recent activity tracking
- Advanced data type handling
- Tag management

Each tool follows a consistent naming convention: superset_<category>_<action>
"""

# Load environment variables from .env file
load_dotenv()

# Constants
SUPERSET_BASE_URL = os.getenv("SUPERSET_BASE_URL", "http://localhost:8088")
SUPERSET_USERNAME = os.getenv("SUPERSET_USERNAME")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD")
SUPERSET_JWT_TOKEN = os.getenv("SUPERSET_JWT_TOKEN")
SUPERSET_SESSION_COOKIE = os.getenv("SUPERSET_SESSION_COOKIE")
# Guest token configuration for embedded dashboards
GUEST_TOKEN_JWT_SECRET = os.getenv("GUEST_TOKEN_JWT_SECRET")
GUEST_TOKEN_JWT_AUDIENCE = os.getenv("GUEST_TOKEN_JWT_AUDIENCE")
GUEST_ROLE_NAME = os.getenv("GUEST_ROLE_NAME")
ACCESS_TOKEN_STORE_PATH = os.path.join(os.path.dirname(__file__), ".superset_token")

# Initialize FastAPI app for handling additional web endpoints if needed
app = FastAPI(title="Superset MCP Server")


@dataclass
class SupersetContext:
    """Typed context for the Superset MCP server"""

    client: httpx.AsyncClient
    base_url: str
    access_token: Optional[str] = None
    csrf_token: Optional[str] = None
    app: FastAPI = None
    current_user_id: Optional[int] = None
    current_username: Optional[str] = None


def load_stored_token() -> Optional[str]:
    """Load stored access token if it exists"""
    try:
        if os.path.exists(ACCESS_TOKEN_STORE_PATH):
            with open(ACCESS_TOKEN_STORE_PATH, "r") as f:
                return f.read().strip()
    except Exception:
        return None
    return None


def save_access_token(token: str):
    """Save access token to file"""
    try:
        with open(ACCESS_TOKEN_STORE_PATH, "w") as f:
            f.write(token)
    except Exception as e:
        logger.warning(f"Warning: Could not save access token: {e}")


def generate_guest_token(
    resource_type: str = "dashboard",
    resource_id: int = None,
    rls_rules: list = None,
    user: dict = None,
    role_override: str = None
) -> Optional[str]:
    """
    Generate a guest token for embedded Superset dashboards/charts

    This matches the PHP implementation from superfull\\Auth::getGuestTokenFor()

    Args:
        resource_type: Type of resource ('dashboard' or 'chart')
        resource_id: ID of the dashboard or chart
        rls_rules: Optional list of row-level security rules
        user: Optional user information dict (username, first_name, last_name)
        role_override: Optional role name to override GUEST_ROLE_NAME

    Returns:
        JWT guest token string or None if configuration is missing
    """
    if not GUEST_TOKEN_JWT_SECRET or not GUEST_TOKEN_JWT_AUDIENCE:
        logger.warning("Guest token configuration missing (GUEST_TOKEN_JWT_SECRET, GUEST_TOKEN_JWT_AUDIENCE)")
        return None

    # Default user info - matching PHP: remote_access bot
    if user is None:
        user = {
            "username": "remote_access",
            "first_name": "remote_access",
            "last_name": "bot"
        }

    # Create the token payload - matching PHP structure exactly
    payload = {
        "aud": GUEST_TOKEN_JWT_AUDIENCE,
        "resources": [{
            "id": resource_id,  # Keep as int, not string
            "type": resource_type
        }],
        "rls": [],  # Empty array for RLS
        "rls_rules": rls_rules or [],  # Separate rls_rules field
        "user": user,
        "type": "guest"
    }

    # Add role if specified (either override or default from config)
    role = role_override or GUEST_ROLE_NAME
    if role:
        # Superset expects roles in the user dict for guest tokens
        if "roles" not in user:
            user["roles"] = [role]

    # Generate the token using HS256 algorithm
    token = jwt.encode(payload, GUEST_TOKEN_JWT_SECRET, algorithm="HS256")
    return token


@asynccontextmanager
async def superset_lifespan(server: FastMCP) -> AsyncIterator[SupersetContext]:
    """Manage application lifecycle for Superset integration"""
    logger.info("Initializing Superset context...")

    # Create HTTP client
    client = httpx.AsyncClient(base_url=SUPERSET_BASE_URL, timeout=30.0)

    # Create context
    ctx = SupersetContext(client=client, base_url=SUPERSET_BASE_URL, app=app)

    # Priority 1: Check for session cookie (for OAuth-based authentication)
    if SUPERSET_SESSION_COOKIE:
        logger.info("Using session cookie from environment variable")
        # Extract domain from base URL for cookie
        from urllib.parse import urlparse
        parsed = urlparse(SUPERSET_BASE_URL)
        domain = parsed.netloc

        client.cookies.set("session", SUPERSET_SESSION_COOKIE, domain=domain)

        # Try to get a JWT token using the session
        try:
            response = await client.post("/api/v1/security/refresh")
            if response.status_code == 200:
                data = response.json()
                access_token = data.get("access_token")
                if access_token:
                    ctx.access_token = access_token
                    client.headers.update({"Authorization": f"Bearer {access_token}"})
                    save_access_token(access_token)
                    logger.info("Successfully obtained JWT token from session cookie")
                else:
                    logger.warning("No access token in refresh response, using session cookie only")
            else:
                logger.warning(f"Failed to get JWT token from session: {response.status_code}")
        except Exception as e:
            logger.warning(f"Error getting JWT token from session: {e}")

        # Verify the session/token works
        try:
            response = await client.get("/api/v1/me/")
            if response.status_code == 200:
                user_info = response.json()
                logger.info(f"Authenticated as user: {user_info.get('username', 'unknown')}")
            else:
                logger.warning(f"Session authentication failed: {response.status_code}")
        except Exception as e:
            logger.warning(f"Error verifying session: {e}")

    # Priority 2: Check for JWT token from environment variable
    elif SUPERSET_JWT_TOKEN:
        ctx.access_token = SUPERSET_JWT_TOKEN
        client.headers.update({"Authorization": f"Bearer {SUPERSET_JWT_TOKEN}"})
        logger.info("Using JWT token from environment variable")

        # Verify token validity
        try:
            response = await client.get("/api/v1/me/")
            if response.status_code != 200:
                logger.warning(
                    f"JWT token validation failed (status {response.status_code}): {response.text}"
                )
                ctx.access_token = None
                client.headers.pop("Authorization", None)
            else:
                logger.info("JWT token is valid and authenticated")
                user_info = response.json()
                logger.info(f"Authenticated as user: {user_info.get('username', 'unknown')}")
        except Exception as e:
            logger.warning(f"Error verifying JWT token: {e}")
            ctx.access_token = None
            client.headers.pop("Authorization", None)
    else:
        # Priority 3: Try to load existing token from file
        stored_token = load_stored_token()
        if stored_token:
            ctx.access_token = stored_token
            # Set the token in the client headers
            client.headers.update({"Authorization": f"Bearer {stored_token}"})
            logger.info("Using stored access token")

            # Verify token validity
            try:
                response = await client.get("/api/v1/me/")
                if response.status_code != 200:
                    logger.info(
                        f"Stored token is invalid (status {response.status_code}). Will need to re-authenticate."
                    )
                    ctx.access_token = None
                    client.headers.pop("Authorization", None)
            except Exception as e:
                logger.info(f"Error verifying stored token: {e}")
                ctx.access_token = None
                client.headers.pop("Authorization", None)

    try:
        yield ctx
    finally:
        # Cleanup on shutdown
        logger.info("Shutting down Superset context...")
        await client.aclose()


# ---------------------------------------------------------------------------
# HTTP transport configuration (from environment variables)
# ---------------------------------------------------------------------------
MCP_TRANSPORT = os.getenv("MCP_TRANSPORT", "stdio")  # stdio | streamable-http | sse | both
MCP_HTTP_HOST = os.getenv("MCP_HTTP_HOST", "0.0.0.0")
MCP_HTTP_PORT = int(os.getenv("MCP_HTTP_PORT", "8044"))
MCP_ISSUER_URL = os.getenv("MCP_ISSUER_URL", "http://localhost:8044")
MCP_RESOURCE_SERVER_URL = os.getenv("MCP_RESOURCE_SERVER_URL") or None
MCP_TLS_CERTFILE = os.getenv("MCP_TLS_CERTFILE")
MCP_TLS_KEYFILE = os.getenv("MCP_TLS_KEYFILE")
# OAuth clients: comma-separated "id:secret" pairs
# e.g. "claude-ai:secret1,chatgpt:secret2"
MCP_OAUTH_CLIENTS = os.getenv("MCP_OAUTH_CLIENTS", "")
# Static API tokens: comma-separated
MCP_API_TOKENS = os.getenv("MCP_API_TOKENS", "")
# IP allowlist: comma-separated CIDRs
MCP_ALLOWED_IPS = os.getenv("MCP_ALLOWED_IPS", "")
# Trusted proxies: comma-separated IPs
MCP_TRUSTED_PROXIES = os.getenv("MCP_TRUSTED_PROXIES", "")


def _build_mcp_kwargs() -> dict:
    """Build FastMCP constructor kwargs, adding OAuth when using HTTP transport."""
    kwargs: Dict[str, Any] = {
        "name": "superset",
        "lifespan": superset_lifespan,
        "dependencies": ["fastapi", "uvicorn", "python-dotenv", "httpx", "PyJWT"],
    }

    transport = os.getenv("MCP_TRANSPORT", "stdio")
    if transport in ("streamable-http", "sse", "both"):
        from auth import MCPOAuthProvider, OAuthClientEntry
        from mcp.server.auth.settings import AuthSettings

        # Parse OAuth clients from env
        client_entries = []
        if MCP_OAUTH_CLIENTS:
            for pair in MCP_OAUTH_CLIENTS.split(","):
                pair = pair.strip()
                if ":" in pair:
                    cid, csec = pair.split(":", 1)
                    client_entries.append(OAuthClientEntry(client_id=cid.strip(), client_secret=csec.strip()))

        if not client_entries:
            raise SystemExit(
                "HTTP transport requires at least one OAuth client. "
                "Set MCP_OAUTH_CLIENTS='client_id:client_secret' in environment."
            )

        api_tokens = [t.strip() for t in MCP_API_TOKENS.split(",") if t.strip()] if MCP_API_TOKENS else []

        provider = MCPOAuthProvider(clients=client_entries, api_tokens=api_tokens)
        kwargs.update(
            host=MCP_HTTP_HOST,
            port=MCP_HTTP_PORT,
            auth_server_provider=provider,
            auth=AuthSettings(
                issuer_url=MCP_ISSUER_URL,
                resource_server_url=MCP_RESOURCE_SERVER_URL,
            ),
        )
        logger.info("HTTP transport configured on %s:%s (OAuth clients: %d)",
                     MCP_HTTP_HOST, MCP_HTTP_PORT, len(client_entries))

    return kwargs


# Initialize FastMCP server with lifespan and dependencies
mcp = FastMCP(**_build_mcp_kwargs())

# Type variables for generic function annotations
T = TypeVar("T")
R = TypeVar("R")

# ===== Helper Functions and Decorators =====


def requires_auth(
    func: Callable[..., Awaitable[Dict[str, Any]]],
) -> Callable[..., Awaitable[Dict[str, Any]]]:
    """Decorator to check authentication before executing a function"""

    @wraps(func)
    async def wrapper(ctx: Context, *args, **kwargs) -> Dict[str, Any]:
        superset_ctx: SupersetContext = ctx.request_context.lifespan_context

        if not superset_ctx.access_token:
            return {"error": "Not authenticated. Please authenticate first."}

        return await func(ctx, *args, **kwargs)

    return wrapper


def handle_api_errors(
    func: Callable[..., Awaitable[Dict[str, Any]]],
) -> Callable[..., Awaitable[Dict[str, Any]]]:
    """Decorator to handle API errors in a consistent way"""

    @wraps(func)
    async def wrapper(ctx: Context, *args, **kwargs) -> Dict[str, Any]:
        try:
            return await func(ctx, *args, **kwargs)
        except Exception as e:
            # Extract function name for better error context
            function_name = func.__name__
            return {"error": f"Error in {function_name}: {str(e)}"}

    return wrapper


async def with_auto_refresh(
    ctx: Context, api_call: Callable[[], Awaitable[httpx.Response]]
) -> httpx.Response:
    """
    Helper function to handle automatic token refreshing for API calls

    This function will attempt to execute the provided API call. If the call
    fails with a 401 Unauthorized error, it will try to refresh the token
    and retry the API call once.

    Args:
        ctx: The MCP context
        api_call: The API call function to execute (should be a callable that returns a response)
    """
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context

    if not superset_ctx.access_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    # First attempt
    try:
        response = await api_call()

        # If not an auth error, return the response
        if response.status_code != 401:
            return response

    except httpx.HTTPStatusError as e:
        if e.response.status_code != 401:
            raise e
        response = e.response
    except Exception as e:
        # For other errors, just raise
        raise e

    # If we got a 401, try to refresh the token
    logger.info("Received 401 Unauthorized. Attempting to refresh token...")
    refresh_result = await superset_auth_refresh_token(ctx)

    if refresh_result.get("error"):
        # If refresh failed, try to re-authenticate
        logger.info(
            f"Token refresh failed: {refresh_result.get('error')}. Attempting re-authentication..."
        )
        auth_result = await superset_auth_authenticate_user(ctx)

        if auth_result.get("error"):
            # If re-authentication failed, raise an exception
            raise HTTPException(status_code=401, detail="Authentication failed")

    # Retry the API call with the new token
    return await api_call()


async def get_csrf_token(ctx: Context) -> Optional[str]:
    """
    Get a CSRF token from Superset

    Makes a request to the /api/v1/security/csrf_token endpoint to get a token

    Args:
        ctx: MCP context
    """
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context
    client = superset_ctx.client

    try:
        response = await client.get("/api/v1/security/csrf_token/")
        if response.status_code == 200:
            data = response.json()
            csrf_token = data.get("result")
            superset_ctx.csrf_token = csrf_token
            return csrf_token
        else:
            logger.info(
                f"Failed to get CSRF token: {response.status_code} - {response.text}"
            )
            return None
    except Exception as e:
        logger.info(f"Error getting CSRF token: {str(e)}")
        return None


async def make_api_request(
    ctx: Context,
    method: str,
    endpoint: str,
    data: Dict[str, Any] = None,
    params: Dict[str, Any] = None,
    auto_refresh: bool = True,
) -> Dict[str, Any]:
    """
    Helper function to make API requests to Superset

    Args:
        ctx: MCP context
        method: HTTP method (get, post, put, delete)
        endpoint: API endpoint (without base URL)
        data: Optional JSON payload for POST/PUT requests
        params: Optional query parameters
        auto_refresh: Whether to auto-refresh token on 401
    """
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context
    client = superset_ctx.client

    # For non-GET requests, make sure we have a CSRF token
    if method.lower() != "get" and not superset_ctx.csrf_token:
        await get_csrf_token(ctx)

    async def make_request() -> httpx.Response:
        headers = {}

        # Add CSRF token for non-GET requests
        if method.lower() != "get" and superset_ctx.csrf_token:
            headers["X-CSRFToken"] = superset_ctx.csrf_token

        if method.lower() == "get":
            return await client.get(endpoint, params=params)
        elif method.lower() == "post":
            return await client.post(
                endpoint, json=data, params=params, headers=headers
            )
        elif method.lower() == "put":
            return await client.put(endpoint, json=data, headers=headers)
        elif method.lower() == "delete":
            return await client.delete(endpoint, headers=headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

    # Use auto_refresh if requested
    response = (
        await with_auto_refresh(ctx, make_request)
        if auto_refresh
        else await make_request()
    )

    if response.status_code not in [200, 201]:
        return {
            "error": f"API request failed: {response.status_code} - {response.text}"
        }

    return response.json()


def _safe_int(value: Any) -> Optional[int]:
    """Convert a value to int when possible."""
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_resource(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize Superset API payloads that may wrap data in `result`."""
    result = payload.get("result")
    return result if isinstance(result, dict) else payload


def _extract_owner_refs(resource: Dict[str, Any]) -> tuple[set[int], set[str]]:
    """Extract owner IDs and usernames from common Superset fields."""
    owner_ids: set[int] = set()
    owner_names: set[str] = set()

    def parse_ref(ref: Any):
        if ref is None:
            return

        if isinstance(ref, list):
            for item in ref:
                parse_ref(item)
            return

        if isinstance(ref, dict):
            for key in ("id", "user_id", "owner_id", "created_by_fk"):
                maybe_id = _safe_int(ref.get(key))
                if maybe_id is not None:
                    owner_ids.add(maybe_id)

            for key in ("username", "user_name", "name"):
                value = ref.get(key)
                if isinstance(value, str) and value.strip():
                    owner_names.add(value.strip())
            return

        maybe_id = _safe_int(ref)
        if maybe_id is not None:
            owner_ids.add(maybe_id)

    for field in ("owners", "owner", "created_by"):
        parse_ref(resource.get(field))

    created_by_fk = _safe_int(resource.get("created_by_fk"))
    if created_by_fk is not None:
        owner_ids.add(created_by_fk)

    return owner_ids, owner_names


async def get_current_user_identity(ctx: Context) -> tuple[Optional[int], Optional[str]]:
    """Get and cache the authenticated Superset user's ID and username."""
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context
    if superset_ctx.current_user_id is not None or superset_ctx.current_username:
        return superset_ctx.current_user_id, superset_ctx.current_username

    me = await make_api_request(ctx, "get", "/api/v1/me/")
    if me.get("error"):
        return None, None

    me_resource = _as_resource(me)
    superset_ctx.current_user_id = _safe_int(me_resource.get("id"))

    username = me_resource.get("username")
    if isinstance(username, str) and username.strip():
        superset_ctx.current_username = username.strip()

    return superset_ctx.current_user_id, superset_ctx.current_username


async def require_resource_ownership(
    ctx: Context, resource_type: str, resource_id: int, endpoint: str
) -> Optional[Dict[str, Any]]:
    """Allow mutation only when the current user owns the target resource."""
    user_id, username = await get_current_user_identity(ctx)
    if user_id is None and not username:
        return {
            "error": "Unable to determine current user identity; ownership check failed."
        }

    resource_response = await make_api_request(ctx, "get", endpoint)
    if resource_response.get("error"):
        return {
            "error": (
                f"Unable to verify ownership for {resource_type} {resource_id}: "
                f"{resource_response['error']}"
            )
        }

    resource = _as_resource(resource_response)
    owner_ids, owner_names = _extract_owner_refs(resource)
    if (user_id is not None and user_id in owner_ids) or (
        username and username in owner_names
    ):
        return None

    owner_display = username or f"user_id={user_id}"
    return {
        "error": (
            f"Permission denied: {resource_type} {resource_id} is not owned by the "
            f"authenticated MCP user ({owner_display})."
        )
    }


async def add_current_user_as_owner(
    ctx: Context, payload: Dict[str, Any], owners_key: str = "owners"
) -> Optional[Dict[str, Any]]:
    """Ensure the authenticated MCP user is included in payload owners."""
    user_id, _ = await get_current_user_identity(ctx)
    if user_id is None:
        return {
            "error": (
                "Unable to determine current user ID; refusing create operation "
                "without explicit ownership."
            )
        }

    owners_value = payload.get(owners_key)
    owner_ids: set[int] = set()

    if isinstance(owners_value, list):
        for owner in owners_value:
            owner_id = _safe_int(owner.get("id") if isinstance(owner, dict) else owner)
            if owner_id is not None:
                owner_ids.add(owner_id)

    owner_ids.add(user_id)
    payload[owners_key] = sorted(owner_ids)
    return None


# ===== Authentication Tools =====


@mcp.tool()
@handle_api_errors
async def superset_auth_check_token_validity(ctx: Context) -> Dict[str, Any]:
    """
    Check if the current access token is still valid

    Makes a request to the /api/v1/me/ endpoint to test if the current token is valid.
    Use this to verify authentication status before making other API calls.

    Returns:
        A dictionary with token validity status and any error information
    """
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context

    if not superset_ctx.access_token:
        return {"valid": False, "error": "No access token available"}

    try:
        # Make a simple API call to test if token is valid (get user info)
        response = await superset_ctx.client.get("/api/v1/me/")

        if response.status_code == 200:
            return {"valid": True}
        else:
            return {
                "valid": False,
                "status_code": response.status_code,
                "error": response.text,
            }
    except Exception as e:
        return {"valid": False, "error": str(e)}


@mcp.tool()
@handle_api_errors
async def superset_auth_refresh_token(ctx: Context) -> Dict[str, Any]:
    """
    Refresh the access token using the refresh endpoint

    Makes a request to the /api/v1/security/refresh endpoint to get a new access token
    without requiring re-authentication with username/password.

    Returns:
        A dictionary with the new access token or error information
    """
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context

    if not superset_ctx.access_token:
        return {"error": "No access token to refresh. Please authenticate first."}

    try:
        # Use the refresh endpoint to get a new token
        response = await superset_ctx.client.post("/api/v1/security/refresh")

        if response.status_code != 200:
            return {
                "error": f"Failed to refresh token: {response.status_code} - {response.text}"
            }

        data = response.json()
        access_token = data.get("access_token")

        if not access_token:
            return {"error": "No access token returned from refresh"}

        # Save and set the new access token
        save_access_token(access_token)
        superset_ctx.access_token = access_token
        superset_ctx.client.headers.update({"Authorization": f"Bearer {access_token}"})

        return {
            "message": "Successfully refreshed access token",
        }
    except Exception as e:
        return {"error": f"Error refreshing token: {str(e)}"}


@mcp.tool()
@handle_api_errors
async def superset_auth_authenticate_user(
    ctx: Context,
    username: Optional[str] = None,
    password: Optional[str] = None,
    refresh: bool = True,
) -> Dict[str, Any]:
    """
    Authenticate with Superset and get access token

    Makes a request to the /api/v1/security/login endpoint to authenticate and obtain an access token.
    If there's an existing token, will first try to check its validity.
    If invalid, will attempt to refresh token before falling back to re-authentication.

    Args:
        username: Superset username (falls back to environment variable if not provided)
        password: Superset password (falls back to environment variable if not provided)
        refresh: Whether to refresh the token if invalid (defaults to True)

    Returns:
        A dictionary with authentication status and access token or error information
    """
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context

    # If we already have a token, check if it's valid
    if superset_ctx.access_token:
        validity = await superset_auth_check_token_validity(ctx)

        if validity.get("valid"):
            return {
                "message": "Already authenticated with valid token",
            }

        # Token invalid, try to refresh if requested
        if refresh:
            refresh_result = await superset_auth_refresh_token(ctx)
            if not refresh_result.get("error"):
                return refresh_result
            # If refresh fails, fall back to re-authentication

    # Use provided credentials or fall back to env vars
    username = username or SUPERSET_USERNAME
    password = password or SUPERSET_PASSWORD

    if not username or not password:
        return {
            "error": "Username and password must be provided either as arguments or set in environment variables"
        }

    try:
        # Get access token directly using the security login API endpoint
        response = await superset_ctx.client.post(
            "/api/v1/security/login",
            json={
                "username": username,
                "password": password,
                "provider": "db",
                "refresh": refresh,
            },
        )

        if response.status_code != 200:
            return {
                "error": f"Failed to get access token: {response.status_code} - {response.text}"
            }

        data = response.json()
        access_token = data.get("access_token")

        if not access_token:
            return {"error": "No access token returned"}

        # Save and set the access token
        save_access_token(access_token)
        superset_ctx.access_token = access_token
        superset_ctx.client.headers.update({"Authorization": f"Bearer {access_token}"})

        # Get CSRF token after successful authentication
        await get_csrf_token(ctx)

        return {
            "message": "Successfully authenticated with Superset",
        }

    except Exception as e:
        return {"error": f"Authentication error: {str(e)}"}


# ===== Dashboard Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dashboard_list(ctx: Context) -> Dict[str, Any]:
    """
    Get a list of dashboards from Superset

    Makes a request to the /api/v1/dashboard/ endpoint to retrieve all dashboards
    the current user has access to view. Results are paginated.

    Returns:
        A dictionary containing dashboard data including id, title, url, and metadata
    """
    return await make_api_request(ctx, "get", "/api/v1/dashboard/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dashboard_get_by_id(
    ctx: Context, dashboard_id: int
) -> Dict[str, Any]:
    """
    Get details for a specific dashboard

    Makes a request to the /api/v1/dashboard/{id} endpoint to retrieve detailed
    information about a specific dashboard.

    Args:
        dashboard_id: ID of the dashboard to retrieve

    Returns:
        A dictionary with complete dashboard information including components and layout
    """
    return await make_api_request(ctx, "get", f"/api/v1/dashboard/{dashboard_id}")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dashboard_create(
    ctx: Context, dashboard_title: str, json_metadata: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Create a new dashboard in Superset

    Makes a request to the /api/v1/dashboard/ POST endpoint to create a new dashboard.

    Args:
        dashboard_title: Title of the dashboard
        json_metadata: Optional JSON metadata for dashboard configuration,
                       can include layout, color scheme, and filter configuration

    Returns:
        A dictionary with the created dashboard information including its ID
    """
    payload = {"dashboard_title": dashboard_title}
    if json_metadata:
        payload["json_metadata"] = json_metadata

    owner_error = await add_current_user_as_owner(ctx, payload)
    if owner_error:
        return owner_error

    return await make_api_request(ctx, "post", "/api/v1/dashboard/", data=payload)


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dashboard_update(
    ctx: Context, dashboard_id: int, data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Update an existing dashboard

    Makes a request to the /api/v1/dashboard/{id} PUT endpoint to update
    dashboard properties.

    Args:
        dashboard_id: ID of the dashboard to update
        data: Data to update, can include dashboard_title, slug, owners, position, and metadata

    Returns:
        A dictionary with the updated dashboard information
    """
    ownership_error = await require_resource_ownership(
        ctx, "dashboard", dashboard_id, f"/api/v1/dashboard/{dashboard_id}"
    )
    if ownership_error:
        return ownership_error

    return await make_api_request(
        ctx, "put", f"/api/v1/dashboard/{dashboard_id}", data=data
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dashboard_delete(ctx: Context, dashboard_id: int) -> Dict[str, Any]:
    """
    Delete a dashboard

    Makes a request to the /api/v1/dashboard/{id} DELETE endpoint to remove a dashboard.
    This operation is permanent and cannot be undone.

    Args:
        dashboard_id: ID of the dashboard to delete

    Returns:
        A dictionary with deletion confirmation message
    """
    ownership_error = await require_resource_ownership(
        ctx, "dashboard", dashboard_id, f"/api/v1/dashboard/{dashboard_id}"
    )
    if ownership_error:
        return ownership_error

    response = await make_api_request(
        ctx, "delete", f"/api/v1/dashboard/{dashboard_id}"
    )

    # For delete endpoints, we may want a custom success message
    if not response.get("error"):
        return {"message": f"Dashboard {dashboard_id} deleted successfully"}

    return response


# ===== Chart Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_chart_list(ctx: Context) -> Dict[str, Any]:
    """
    Get a list of charts from Superset

    Makes a request to the /api/v1/chart/ endpoint to retrieve all charts
    the current user has access to view. Results are paginated.

    Returns:
        A dictionary containing chart data including id, slice_name, viz_type, and datasource info
    """
    return await make_api_request(ctx, "get", "/api/v1/chart/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_chart_get_by_id(ctx: Context, chart_id: int) -> Dict[str, Any]:
    """
    Get details for a specific chart

    Makes a request to the /api/v1/chart/{id} endpoint to retrieve detailed
    information about a specific chart/slice.

    Args:
        chart_id: ID of the chart to retrieve

    Returns:
        A dictionary with complete chart information including visualization configuration
    """
    return await make_api_request(ctx, "get", f"/api/v1/chart/{chart_id}")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_chart_create(
    ctx: Context,
    slice_name: str,
    datasource_id: int,
    datasource_type: str,
    viz_type: str,
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Create a new chart in Superset

    Makes a request to the /api/v1/chart/ POST endpoint to create a new visualization.

    Args:
        slice_name: Name/title of the chart
        datasource_id: ID of the dataset or SQL table
        datasource_type: Type of datasource ('table' for datasets, 'query' for SQL)
        viz_type: Visualization type (e.g., 'bar', 'line', 'pie', 'big_number', etc.)
        params: Visualization parameters including metrics, groupby, time_range, etc.

    Returns:
        A dictionary with the created chart information including its ID
    """
    payload = {
        "slice_name": slice_name,
        "datasource_id": datasource_id,
        "datasource_type": datasource_type,
        "viz_type": viz_type,
        "params": json.dumps(params),
    }

    owner_error = await add_current_user_as_owner(ctx, payload)
    if owner_error:
        return owner_error

    return await make_api_request(ctx, "post", "/api/v1/chart/", data=payload)


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_chart_update(
    ctx: Context, chart_id: int, data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Update an existing chart

    Makes a request to the /api/v1/chart/{id} PUT endpoint to update
    chart properties and visualization settings.

    Args:
        chart_id: ID of the chart to update
        data: Data to update, can include slice_name, description, viz_type, params, etc.

    Returns:
        A dictionary with the updated chart information
    """
    ownership_error = await require_resource_ownership(
        ctx, "chart", chart_id, f"/api/v1/chart/{chart_id}"
    )
    if ownership_error:
        return ownership_error

    return await make_api_request(ctx, "put", f"/api/v1/chart/{chart_id}", data=data)


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_chart_delete(ctx: Context, chart_id: int) -> Dict[str, Any]:
    """
    Delete a chart

    Makes a request to the /api/v1/chart/{id} DELETE endpoint to remove a chart.
    This operation is permanent and cannot be undone.

    Args:
        chart_id: ID of the chart to delete

    Returns:
        A dictionary with deletion confirmation message
    """
    ownership_error = await require_resource_ownership(
        ctx, "chart", chart_id, f"/api/v1/chart/{chart_id}"
    )
    if ownership_error:
        return ownership_error

    response = await make_api_request(ctx, "delete", f"/api/v1/chart/{chart_id}")

    if not response.get("error"):
        return {"message": f"Chart {chart_id} deleted successfully"}

    return response


# ===== Database Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_list(ctx: Context) -> Dict[str, Any]:
    """
    Get a list of databases from Superset

    Makes a request to the /api/v1/database/ endpoint to retrieve all database
    connections the current user has access to. Results are paginated.

    Returns:
        A dictionary containing database connection information including id, name, and configuration
    """
    return await make_api_request(ctx, "get", "/api/v1/database/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_get_by_id(ctx: Context, database_id: int) -> Dict[str, Any]:
    """
    Get details for a specific database

    Makes a request to the /api/v1/database/{id} endpoint to retrieve detailed
    information about a specific database connection.

    Args:
        database_id: ID of the database to retrieve

    Returns:
        A dictionary with complete database configuration information
    """
    return await make_api_request(ctx, "get", f"/api/v1/database/{database_id}")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_create(
    ctx: Context,
    engine: str,
    configuration_method: str,
    database_name: str,
    sqlalchemy_uri: str,
) -> Dict[str, Any]:
    """
    Create a new database connection in Superset

    IMPORTANT: Don't call this tool, unless user have given connection details. This function will only create database connections with explicit user consent and input.
    No default values or assumptions will be made without user confirmation. All connection parameters,
    including sensitive credentials, must be explicitly provided by the user.

    Makes a POST request to /api/v1/database/ to create a new database connection in Superset.
    The endpoint requires a valid SQLAlchemy URI and database configuration parameters.
    The engine parameter will be automatically determined from the SQLAlchemy URI prefix if not specified:
    - 'postgresql://' -> engine='postgresql'
    - 'mysql://' -> engine='mysql'
    - 'mssql://' -> engine='mssql'
    - 'oracle://' -> engine='oracle'
    - 'sqlite://' -> engine='sqlite'

    The SQLAlchemy URI must follow the format: dialect+driver://username:password@host:port/database
    If the URI is not provided, the function will prompt for individual connection parameters to construct it.

    All required parameters must be provided and validated before creating the connection.
    The configuration_method parameter should typically be set to 'sqlalchemy_form'.

    Args:
        engine: Database engine (e.g., 'postgresql', 'mysql', etc.)
        configuration_method: Method used for configuration (typically 'sqlalchemy_form')
        database_name: Name for the database connection
        sqlalchemy_uri: SQLAlchemy URI for the connection (e.g., 'postgresql://user:pass@host/db')

    Returns:
        A dictionary with the created database connection information including its ID
    """
    payload = {
        "engine": engine,
        "configuration_method": configuration_method,
        "database_name": database_name,
        "sqlalchemy_uri": sqlalchemy_uri,
        "allow_dml": True,
        "allow_cvas": True,
        "allow_ctas": True,
        "expose_in_sqllab": True,
    }

    return await make_api_request(ctx, "post", "/api/v1/database/", data=payload)


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_get_tables(
    ctx: Context, database_id: int
) -> Dict[str, Any]:
    """
    Get a list of tables for a given database

    Makes a request to the /api/v1/database/{id}/tables/ endpoint to retrieve
    all tables available in the database.

    Args:
        database_id: ID of the database

    Returns:
        A dictionary with list of tables including schema and table name information
    """
    return await make_api_request(ctx, "get", f"/api/v1/database/{database_id}/tables/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_schemas(ctx: Context, database_id: int) -> Dict[str, Any]:
    """
    Get schemas for a specific database

    Makes a request to the /api/v1/database/{id}/schemas/ endpoint to retrieve
    all schemas available in the database.

    Args:
        database_id: ID of the database

    Returns:
        A dictionary with list of schema names
    """
    return await make_api_request(
        ctx, "get", f"/api/v1/database/{database_id}/schemas/"
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_test_connection(
    ctx: Context, database_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Test a database connection

    Makes a request to the /api/v1/database/test_connection endpoint to verify if
    the provided connection details can successfully connect to the database.

    Args:
        database_data: Database connection details including sqlalchemy_uri and other parameters

    Returns:
        A dictionary with connection test results
    """
    return await make_api_request(
        ctx, "post", "/api/v1/database/test_connection", data=database_data
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_update(
    ctx: Context, database_id: int, data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Update an existing database connection

    Makes a request to the /api/v1/database/{id} PUT endpoint to update
    database connection properties.

    Args:
        database_id: ID of the database to update
        data: Data to update, can include database_name, sqlalchemy_uri, password, and extra configs

    Returns:
        A dictionary with the updated database information
    """
    ownership_error = await require_resource_ownership(
        ctx, "database", database_id, f"/api/v1/database/{database_id}"
    )
    if ownership_error:
        return ownership_error

    return await make_api_request(
        ctx, "put", f"/api/v1/database/{database_id}", data=data
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_delete(ctx: Context, database_id: int) -> Dict[str, Any]:
    """
    Delete a database connection

    Makes a request to the /api/v1/database/{id} DELETE endpoint to remove a database connection.
    This operation is permanent and cannot be undone. This will also remove associated datasets.

    Args:
        database_id: ID of the database to delete

    Returns:
        A dictionary with deletion confirmation message
    """
    ownership_error = await require_resource_ownership(
        ctx, "database", database_id, f"/api/v1/database/{database_id}"
    )
    if ownership_error:
        return ownership_error

    response = await make_api_request(ctx, "delete", f"/api/v1/database/{database_id}")

    if not response.get("error"):
        return {"message": f"Database {database_id} deleted successfully"}

    return response


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_get_catalogs(
    ctx: Context, database_id: int
) -> Dict[str, Any]:
    """
    Get all catalogs from a database

    Makes a request to the /api/v1/database/{id}/catalogs/ endpoint to retrieve
    all catalogs available in the database.

    Args:
        database_id: ID of the database

    Returns:
        A dictionary with list of catalog names for databases that support catalogs
    """
    return await make_api_request(
        ctx, "get", f"/api/v1/database/{database_id}/catalogs/"
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_get_connection(
    ctx: Context, database_id: int
) -> Dict[str, Any]:
    """
    Get database connection information

    Makes a request to the /api/v1/database/{id}/connection endpoint to retrieve
    connection details for a specific database.

    Args:
        database_id: ID of the database

    Returns:
        A dictionary with detailed connection information
    """
    return await make_api_request(
        ctx, "get", f"/api/v1/database/{database_id}/connection"
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_get_function_names(
    ctx: Context, database_id: int
) -> Dict[str, Any]:
    """
    Get function names supported by a database

    Makes a request to the /api/v1/database/{id}/function_names/ endpoint to retrieve
    all SQL functions supported by the database.

    Args:
        database_id: ID of the database

    Returns:
        A dictionary with list of supported function names
    """
    return await make_api_request(
        ctx, "get", f"/api/v1/database/{database_id}/function_names/"
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_get_related_objects(
    ctx: Context, database_id: int
) -> Dict[str, Any]:
    """
    Get charts and dashboards associated with a database

    Makes a request to the /api/v1/database/{id}/related_objects/ endpoint to retrieve
    counts and references of charts and dashboards that depend on this database.

    Args:
        database_id: ID of the database

    Returns:
        A dictionary with counts and lists of related charts and dashboards
    """
    return await make_api_request(
        ctx, "get", f"/api/v1/database/{database_id}/related_objects/"
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_validate_sql(
    ctx: Context, database_id: int, sql: str
) -> Dict[str, Any]:
    """
    Validate arbitrary SQL against a database

    Makes a request to the /api/v1/database/{id}/validate_sql/ endpoint to check
    if the provided SQL is valid for the specified database.

    Args:
        database_id: ID of the database
        sql: SQL query to validate

    Returns:
        A dictionary with validation results
    """
    payload = {"sql": sql}
    return await make_api_request(
        ctx, "post", f"/api/v1/database/{database_id}/validate_sql/", data=payload
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_database_validate_parameters(
    ctx: Context, parameters: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Validate database connection parameters

    Makes a request to the /api/v1/database/validate_parameters/ endpoint to verify
    if the provided connection parameters are valid without creating a connection.

    Args:
        parameters: Connection parameters to validate

    Returns:
        A dictionary with validation results
    """
    return await make_api_request(
        ctx, "post", "/api/v1/database/validate_parameters/", data=parameters
    )


# ===== Dataset Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dataset_list(ctx: Context) -> Dict[str, Any]:
    """
    Get a list of datasets from Superset

    Makes a request to the /api/v1/dataset/ endpoint to retrieve all datasets
    the current user has access to view. Results are paginated.

    Returns:
        A dictionary containing dataset information including id, table_name, and database
    """
    return await make_api_request(ctx, "get", "/api/v1/dataset/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dataset_get_by_id(ctx: Context, dataset_id: int) -> Dict[str, Any]:
    """
    Get details for a specific dataset

    Makes a request to the /api/v1/dataset/{id} endpoint to retrieve detailed
    information about a specific dataset including columns and metrics.

    Args:
        dataset_id: ID of the dataset to retrieve

    Returns:
        A dictionary with complete dataset information
    """
    return await make_api_request(ctx, "get", f"/api/v1/dataset/{dataset_id}")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dataset_create(
    ctx: Context,
    table_name: str,
    database_id: int,
    schema: str = None,
    owners: List[int] = None,
    sql: str = None,
) -> Dict[str, Any]:
    """
    Create a new dataset in Superset

    Makes a request to the /api/v1/dataset/ POST endpoint to create a new dataset.
    Can create either a physical dataset (from an existing table) or a virtual dataset
    (from a SQL query).

    Args:
        table_name: Name for the dataset. For physical datasets this is the table name.
                    For virtual datasets this is used as the display name.
        database_id: ID of the database where the table exists or the query runs against
        schema: Optional database schema name
        owners: Optional list of user IDs who should own this dataset
        sql: Optional SQL query to create a virtual dataset. When provided, the dataset
             will be based on this query instead of a physical table.

    Returns:
        A dictionary with the created dataset information including its ID
    """
    payload = {
        "table_name": table_name,
        "database": database_id,
    }

    if schema:
        payload["schema"] = schema

    if sql:
        payload["sql"] = sql

    if owners:
        payload["owners"] = owners

    owner_error = await add_current_user_as_owner(ctx, payload)
    if owner_error:
        return owner_error

    return await make_api_request(ctx, "post", "/api/v1/dataset/", data=payload)


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dataset_delete(ctx: Context, dataset_id: int) -> Dict[str, Any]:
    """
    Delete a dataset

    Makes a request to the /api/v1/dataset/{id} DELETE endpoint to remove a dataset.
    This operation is permanent and cannot be undone. Only datasets owned by the
    current user can be deleted.

    Args:
        dataset_id: ID of the dataset to delete

    Returns:
        A dictionary with deletion confirmation message
    """
    ownership_error = await require_resource_ownership(
        ctx, "dataset", dataset_id, f"/api/v1/dataset/{dataset_id}"
    )
    if ownership_error:
        return ownership_error

    response = await make_api_request(ctx, "delete", f"/api/v1/dataset/{dataset_id}")

    if not response.get("error"):
        return {"message": f"Dataset {dataset_id} deleted successfully"}

    return response


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dataset_update(
    ctx: Context,
    dataset_id: int,
    table_name: str = None,
    description: str = None,
    sql: str = None,
    schema: str = None,
    owners: List[int] = None,
    cache_timeout: int = None,
    columns: List[Dict[str, Any]] = None,
    metrics: List[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Update an existing dataset in Superset

    Makes a request to the /api/v1/dataset/{id} PUT endpoint to update an existing
    dataset. Only datasets owned by the current user can be updated.

    Args:
        dataset_id: ID of the dataset to update
        table_name: New name for the dataset
        description: New description for the dataset
        sql: New SQL query (for virtual datasets)
        schema: New database schema name
        owners: New list of user IDs who should own this dataset
        cache_timeout: Cache timeout in seconds
        columns: List of column definitions (each a dict with keys like
                 column_name, type, filterable, groupby, etc.)
        metrics: List of metric definitions (each a dict with keys like
                 metric_name, expression, etc.)

    Returns:
        A dictionary with the updated dataset information
    """
    ownership_error = await require_resource_ownership(
        ctx, "dataset", dataset_id, f"/api/v1/dataset/{dataset_id}"
    )
    if ownership_error:
        return ownership_error

    payload = {}

    if table_name is not None:
        payload["table_name"] = table_name
    if description is not None:
        payload["description"] = description
    if sql is not None:
        payload["sql"] = sql
    if schema is not None:
        payload["schema"] = schema
    if owners is not None:
        payload["owners"] = owners
    if cache_timeout is not None:
        payload["cache_timeout"] = cache_timeout
    if columns is not None:
        payload["columns"] = columns
    if metrics is not None:
        payload["metrics"] = metrics

    if not payload:
        return {"error": "No fields provided to update"}

    owner_error = await add_current_user_as_owner(ctx, payload)
    if owner_error:
        return owner_error

    return await make_api_request(
        ctx, "put", f"/api/v1/dataset/{dataset_id}", data=payload
    )


# ===== SQL Lab Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_sqllab_execute_query(
    ctx: Context, database_id: int, sql: str
) -> Dict[str, Any]:
    """
    Execute a SQL query in SQL Lab

    Makes a request to the /api/v1/sqllab/execute/ endpoint to run a SQL query
    against the specified database.

    Args:
        database_id: ID of the database to query
        sql: SQL query to execute

    Returns:
        A dictionary with query results or execution status for async queries
    """
    # Ensure we have a CSRF token before executing the query
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context
    if not superset_ctx.csrf_token:
        await get_csrf_token(ctx)

    payload = {
        "database_id": database_id,
        "sql": sql,
        "schema": "",
        "tab": "MCP Query",
        "runAsync": False,
        "select_as_cta": False,
    }

    return await make_api_request(ctx, "post", "/api/v1/sqllab/execute/", data=payload)


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_sqllab_get_saved_queries(ctx: Context) -> Dict[str, Any]:
    """
    Get a list of saved queries from SQL Lab

    Makes a request to the /api/v1/saved_query/ endpoint to retrieve all saved queries
    the current user has access to. Results are paginated.

    Returns:
        A dictionary containing saved query information including id, label, and database
    """
    return await make_api_request(ctx, "get", "/api/v1/saved_query/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_sqllab_format_sql(ctx: Context, sql: str) -> Dict[str, Any]:
    """
    Format a SQL query for better readability

    Makes a request to the /api/v1/sqllab/format_sql endpoint to apply standard
    formatting rules to the provided SQL query.

    Args:
        sql: SQL query to format

    Returns:
        A dictionary with the formatted SQL
    """
    payload = {"sql": sql}
    return await make_api_request(
        ctx, "post", "/api/v1/sqllab/format_sql", data=payload
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_sqllab_get_results(ctx: Context, key: str) -> Dict[str, Any]:
    """
    Get results of a previously executed SQL query

    Makes a request to the /api/v1/sqllab/results/ endpoint to retrieve results
    for an asynchronous query using its result key.

    Args:
        key: Result key to retrieve

    Returns:
        A dictionary with query results including column information and data rows
    """
    return await make_api_request(
        ctx, "get", f"/api/v1/sqllab/results/", params={"key": key}
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_sqllab_estimate_query_cost(
    ctx: Context, database_id: int, sql: str, schema: str = None
) -> Dict[str, Any]:
    """
    Estimate the cost of executing a SQL query

    Makes a request to the /api/v1/sqllab/estimate endpoint to get approximate cost
    information for a query before executing it.

    Args:
        database_id: ID of the database
        sql: SQL query to estimate
        schema: Optional schema name

    Returns:
        A dictionary with estimated query cost metrics
    """
    payload = {
        "database_id": database_id,
        "sql": sql,
    }

    if schema:
        payload["schema"] = schema

    return await make_api_request(ctx, "post", "/api/v1/sqllab/estimate", data=payload)


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_sqllab_export_query_results(
    ctx: Context, client_id: str
) -> Dict[str, Any]:
    """
    Export the results of a SQL query to CSV

    Makes a request to the /api/v1/sqllab/export/{client_id} endpoint to download
    query results in CSV format.

    Args:
        client_id: Client ID of the query

    Returns:
        A dictionary with the exported data or error information
    """
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context

    try:
        response = await superset_ctx.client.get(f"/api/v1/sqllab/export/{client_id}")

        if response.status_code != 200:
            return {
                "error": f"Failed to export query results: {response.status_code} - {response.text}"
            }

        return {"message": "Query results exported successfully", "data": response.text}

    except Exception as e:
        return {"error": f"Error exporting query results: {str(e)}"}


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_sqllab_get_bootstrap_data(ctx: Context) -> Dict[str, Any]:
    """
    Get the bootstrap data for SQL Lab

    Makes a request to the /api/v1/sqllab/ endpoint to retrieve configuration data
    needed for the SQL Lab interface.

    Returns:
        A dictionary with SQL Lab configuration including allowed databases and settings
    """
    return await make_api_request(ctx, "get", "/api/v1/sqllab/")


# ===== Saved Query Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_saved_query_get_by_id(ctx: Context, query_id: int) -> Dict[str, Any]:
    """
    Get details for a specific saved query

    Makes a request to the /api/v1/saved_query/{id} endpoint to retrieve information
    about a saved SQL query.

    Args:
        query_id: ID of the saved query to retrieve

    Returns:
        A dictionary with the saved query details including SQL text and database
    """
    return await make_api_request(ctx, "get", f"/api/v1/saved_query/{query_id}")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_saved_query_create(
    ctx: Context, query_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create a new saved query

    Makes a request to the /api/v1/saved_query/ POST endpoint to save a SQL query
    for later reuse.

    Args:
        query_data: Dictionary containing the query information including:
                   - db_id: Database ID
                   - schema: Schema name (optional)
                   - sql: SQL query text
                   - label: Display name for the saved query
                   - description: Optional description of the query

    Returns:
        A dictionary with the created saved query information including its ID
    """
    return await make_api_request(ctx, "post", "/api/v1/saved_query/", data=query_data)


# ===== Query Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_query_stop(ctx: Context, client_id: str) -> Dict[str, Any]:
    """
    Stop a running query

    Makes a request to the /api/v1/query/stop endpoint to terminate a query that
    is currently running.

    Args:
        client_id: Client ID of the query to stop

    Returns:
        A dictionary with confirmation of query termination
    """
    payload = {"client_id": client_id}
    return await make_api_request(ctx, "post", "/api/v1/query/stop", data=payload)


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_query_list(ctx: Context) -> Dict[str, Any]:
    """
    Get a list of queries from Superset

    Makes a request to the /api/v1/query/ endpoint to retrieve query history.
    Results are paginated and include both finished and running queries.

    Returns:
        A dictionary containing query information including status, duration, and SQL
    """
    return await make_api_request(ctx, "get", "/api/v1/query/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_query_get_by_id(ctx: Context, query_id: int) -> Dict[str, Any]:
    """
    Get details for a specific query

    Makes a request to the /api/v1/query/{id} endpoint to retrieve detailed
    information about a specific query execution.

    Args:
        query_id: ID of the query to retrieve

    Returns:
        A dictionary with complete query execution information
    """
    return await make_api_request(ctx, "get", f"/api/v1/query/{query_id}")


# ===== Activity and User Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_activity_get_recent(ctx: Context) -> Dict[str, Any]:
    """
    Get recent activity data for the current user

    Makes a request to the /api/v1/log/recent_activity/ endpoint to retrieve
    a history of actions performed by the current user.

    Returns:
        A dictionary with recent user activities including viewed charts and dashboards
    """
    return await make_api_request(ctx, "get", "/api/v1/log/recent_activity/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_user_get_current(ctx: Context) -> Dict[str, Any]:
    """
    Get information about the currently authenticated user

    Makes a request to the /api/v1/me/ endpoint to retrieve the user's profile
    information including permissions and preferences.

    Returns:
        A dictionary with user profile data
    """
    return await make_api_request(ctx, "get", "/api/v1/me/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_user_get_roles(ctx: Context) -> Dict[str, Any]:
    """
    Get roles for the current user

    Makes a request to the /api/v1/me/roles/ endpoint to retrieve all roles
    assigned to the current user.

    Returns:
        A dictionary with user role information
    """
    return await make_api_request(ctx, "get", "/api/v1/me/roles/")


# ===== Tag Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_tag_list(ctx: Context) -> Dict[str, Any]:
    """
    Get a list of tags from Superset

    Makes a request to the /api/v1/tag/ endpoint to retrieve all tags
    defined in the Superset instance.

    Returns:
        A dictionary containing tag information including id and name
    """
    return await make_api_request(ctx, "get", "/api/v1/tag/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_tag_create(ctx: Context, name: str) -> Dict[str, Any]:
    """
    Create a new tag in Superset

    Makes a request to the /api/v1/tag/ POST endpoint to create a new tag
    that can be applied to objects like charts and dashboards.

    Args:
        name: Name for the tag

    Returns:
        A dictionary with the created tag information
    """
    payload = {"name": name}
    return await make_api_request(ctx, "post", "/api/v1/tag/", data=payload)


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_tag_get_by_id(ctx: Context, tag_id: int) -> Dict[str, Any]:
    """
    Get details for a specific tag

    Makes a request to the /api/v1/tag/{id} endpoint to retrieve information
    about a specific tag.

    Args:
        tag_id: ID of the tag to retrieve

    Returns:
        A dictionary with tag details
    """
    return await make_api_request(ctx, "get", f"/api/v1/tag/{tag_id}")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_tag_objects(ctx: Context) -> Dict[str, Any]:
    """
    Get objects associated with tags

    Makes a request to the /api/v1/tag/get_objects/ endpoint to retrieve
    all objects that have tags assigned to them.

    Returns:
        A dictionary with tagged objects grouped by tag
    """
    return await make_api_request(ctx, "get", "/api/v1/tag/get_objects/")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_tag_delete(ctx: Context, tag_id: int) -> Dict[str, Any]:
    """
    Delete a tag

    Makes a request to the /api/v1/tag/{id} DELETE endpoint to remove a tag.
    This operation is permanent and cannot be undone.

    Args:
        tag_id: ID of the tag to delete

    Returns:
        A dictionary with deletion confirmation message
    """
    ownership_error = await require_resource_ownership(
        ctx, "tag", tag_id, f"/api/v1/tag/{tag_id}"
    )
    if ownership_error:
        return ownership_error

    response = await make_api_request(ctx, "delete", f"/api/v1/tag/{tag_id}")

    if not response.get("error"):
        return {"message": f"Tag {tag_id} deleted successfully"}

    return response


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_tag_object_add(
    ctx: Context, object_type: str, object_id: int, tag_name: str
) -> Dict[str, Any]:
    """
    Add a tag to an object

    Makes a request to tag an object with a specific tag. This creates an association
    between the tag and the specified object (chart, dashboard, etc.)

    Args:
        object_type: Type of the object ('chart', 'dashboard', etc.)
        object_id: ID of the object to tag
        tag_name: Name of the tag to apply

    Returns:
        A dictionary with the tagging confirmation
    """
    endpoint_map = {
        "chart": f"/api/v1/chart/{object_id}",
        "dashboard": f"/api/v1/dashboard/{object_id}",
        "dataset": f"/api/v1/dataset/{object_id}",
        "database": f"/api/v1/database/{object_id}",
    }
    endpoint = endpoint_map.get(object_type.lower())
    if not endpoint:
        return {
            "error": (
                "Unsupported object_type for ownership check. "
                "Allowed values: chart, dashboard, dataset, database."
            )
        }

    ownership_error = await require_resource_ownership(
        ctx, object_type, object_id, endpoint
    )
    if ownership_error:
        return ownership_error

    payload = {
        "object_type": object_type,
        "object_id": object_id,
        "tag_name": tag_name,
    }

    return await make_api_request(
        ctx, "post", "/api/v1/tag/tagged_objects", data=payload
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_tag_object_remove(
    ctx: Context, object_type: str, object_id: int, tag_name: str
) -> Dict[str, Any]:
    """
    Remove a tag from an object

    Makes a request to remove a tag association from a specific object.

    Args:
        object_type: Type of the object ('chart', 'dashboard', etc.)
        object_id: ID of the object to untag
        tag_name: Name of the tag to remove

    Returns:
        A dictionary with the untagging confirmation message
    """
    endpoint_map = {
        "chart": f"/api/v1/chart/{object_id}",
        "dashboard": f"/api/v1/dashboard/{object_id}",
        "dataset": f"/api/v1/dataset/{object_id}",
        "database": f"/api/v1/database/{object_id}",
    }
    endpoint = endpoint_map.get(object_type.lower())
    if not endpoint:
        return {
            "error": (
                "Unsupported object_type for ownership check. "
                "Allowed values: chart, dashboard, dataset, database."
            )
        }

    ownership_error = await require_resource_ownership(
        ctx, object_type, object_id, endpoint
    )
    if ownership_error:
        return ownership_error

    response = await make_api_request(
        ctx,
        "delete",
        f"/api/v1/tag/{object_type}/{object_id}",
        params={"tag_name": tag_name},
    )

    if not response.get("error"):
        return {
            "message": f"Tag '{tag_name}' removed from {object_type} {object_id} successfully"
        }

    return response


# ===== Explore Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_explore_form_data_create(
    ctx: Context, form_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create form data for chart exploration

    Makes a request to the /api/v1/explore/form_data POST endpoint to store
    chart configuration data temporarily.

    Args:
        form_data: Chart configuration including datasource, metrics, and visualization settings

    Returns:
        A dictionary with a key that can be used to retrieve the form data
    """
    return await make_api_request(
        ctx, "post", "/api/v1/explore/form_data", data=form_data
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_explore_form_data_get(ctx: Context, key: str) -> Dict[str, Any]:
    """
    Get form data for chart exploration

    Makes a request to the /api/v1/explore/form_data/{key} endpoint to retrieve
    previously stored chart configuration.

    Args:
        key: Key of the form data to retrieve

    Returns:
        A dictionary with the stored chart configuration
    """
    return await make_api_request(ctx, "get", f"/api/v1/explore/form_data/{key}")


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_explore_permalink_create(
    ctx: Context, state: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create a permalink for chart exploration

    Makes a request to the /api/v1/explore/permalink POST endpoint to generate
    a shareable link to a specific chart exploration state.

    Args:
        state: State data for the permalink including form_data

    Returns:
        A dictionary with a key that can be used to access the permalink
    """
    return await make_api_request(ctx, "post", "/api/v1/explore/permalink", data=state)


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_explore_permalink_get(ctx: Context, key: str) -> Dict[str, Any]:
    """
    Get a permalink for chart exploration

    Makes a request to the /api/v1/explore/permalink/{key} endpoint to retrieve
    a previously saved exploration state.

    Args:
        key: Key of the permalink to retrieve

    Returns:
        A dictionary with the stored exploration state
    """
    return await make_api_request(ctx, "get", f"/api/v1/explore/permalink/{key}")


# ===== Menu Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_menu_get(ctx: Context) -> Dict[str, Any]:
    """
    Get the Superset menu data

    Makes a request to the /api/v1/menu/ endpoint to retrieve the navigation
    menu structure based on user permissions.

    Returns:
        A dictionary with menu items and their configurations
    """
    return await make_api_request(ctx, "get", "/api/v1/menu/")


# ===== Guest Token Tools =====


@mcp.tool()
@handle_api_errors
async def superset_guest_token_generate(
    ctx: Context,
    resource_type: str,
    resource_id: int,
    rls_rules: List[Dict[str, Any]] = None,
    user_username: str = "remote_access",
    user_first_name: str = "remote_access",
    user_last_name: str = "bot",
    role_name: str = None
) -> Dict[str, Any]:
    """
    Generate a guest token for embedded Superset dashboards or charts

    This generates a JWT guest token that can be used to access Superset resources
    in embedded mode without requiring user authentication. This is useful for
    embedding dashboards in external applications.

    Matches the PHP implementation from superfull\\Auth::getGuestTokenFor()

    Note: Requires GUEST_TOKEN_JWT_SECRET and GUEST_TOKEN_JWT_AUDIENCE to be
    configured in your environment variables.

    Args:
        resource_type: Type of resource ('dashboard' or 'chart')
        resource_id: ID of the dashboard or chart
        rls_rules: Optional list of row-level security rules as dictionaries
        user_username: Username for the guest user (default: 'remote_access')
        user_first_name: First name for the guest user (default: 'remote_access')
        user_last_name: Last name for the guest user (default: 'bot')
        role_name: Optional role name to use (overrides GUEST_ROLE_NAME from config).
                   Use this to request a different role with more permissions than the default.
                   Example: 'Admin', 'Alpha', 'Gamma', or any custom role configured in Superset

    Returns:
        A dictionary with the generated guest token
    """
    user = {
        "username": user_username,
        "first_name": user_first_name,
        "last_name": user_last_name
    }

    token = generate_guest_token(
        resource_type=resource_type,
        resource_id=resource_id,
        rls_rules=rls_rules,
        user=user,
        role_override=role_name
    )

    if not token:
        return {
            "error": "Failed to generate guest token. Make sure GUEST_TOKEN_JWT_SECRET and GUEST_TOKEN_JWT_AUDIENCE are configured."
        }

    return {
        "message": "Guest token generated successfully",
        "resource_type": resource_type,
        "resource_id": resource_id,
        "role": role_name or GUEST_ROLE_NAME or "default",
        "note": "Token has no expiration (matches PHP implementation). Token is stored internally and will be used automatically for subsequent API calls."
    }


# ===== Screenshot Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dashboard_cache_screenshot(
    ctx: Context, dashboard_id: int, use_guest_token: bool = False
) -> Dict[str, Any]:
    """
    Cache a screenshot of a dashboard

    Makes a request to the /api/v1/dashboard/{id}/cache_dashboard_screenshot/ endpoint
    to generate and cache a screenshot of the dashboard. This is an asynchronous operation
    that returns an image_url which can be used to retrieve the cached screenshot.

    Note: This requires the THUMBNAILS and ENABLE_DASHBOARD_SCREENSHOT_ENDPOINTS feature
    flags to be enabled in your Superset configuration. Additionally, you need a proper
    cache backend (like Redis) configured instead of NullCache.

    Args:
        dashboard_id: ID of the dashboard to screenshot

    Returns:
        A dictionary with the image_url for retrieving the cached screenshot or status information
    """
    return await make_api_request(
        ctx, "post", f"/api/v1/dashboard/{dashboard_id}/cache_dashboard_screenshot/"
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dashboard_get_screenshot(
    ctx: Context, dashboard_id: int, cache_key: str
) -> Dict[str, Any]:
    """
    Get a cached screenshot of a dashboard

    Makes a request to the /api/v1/dashboard/{id}/screenshot/{cache_key}/ endpoint
    to retrieve a previously cached screenshot of the dashboard.

    Note: You must first call superset_dashboard_cache_screenshot to generate the
    screenshot and obtain the cache_key.

    Args:
        dashboard_id: ID of the dashboard
        cache_key: Cache key returned from cache_dashboard_screenshot endpoint

    Returns:
        A dictionary with the screenshot image data or error information
    """
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context

    try:
        response = await superset_ctx.client.get(
            f"/api/v1/dashboard/{dashboard_id}/screenshot/{cache_key}/"
        )

        if response.status_code != 200:
            return {
                "error": f"Failed to get screenshot: {response.status_code} - {response.text}"
            }

        # Return the binary image data
        return {
            "message": "Screenshot retrieved successfully",
            "content_type": response.headers.get("content-type", "image/png"),
            "data": response.content.decode("latin-1") if isinstance(response.content, bytes) else response.content,
            "size_bytes": len(response.content),
        }

    except Exception as e:
        return {"error": f"Error retrieving screenshot: {str(e)}"}


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_dashboard_get_thumbnail(
    ctx: Context, dashboard_id: int, digest: str
) -> Dict[str, Any]:
    """
    Get a thumbnail image of a dashboard

    Makes a request to the /api/v1/dashboard/{id}/thumbnail/{digest}/ endpoint
    to retrieve a thumbnail image of the dashboard.

    Note: This requires the THUMBNAILS feature flag to be enabled in your Superset configuration.

    Args:
        dashboard_id: ID of the dashboard
        digest: Digest/hash for the thumbnail version

    Returns:
        A dictionary with the thumbnail image data or error information
    """
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context

    try:
        response = await superset_ctx.client.get(
            f"/api/v1/dashboard/{dashboard_id}/thumbnail/{digest}/"
        )

        if response.status_code != 200:
            return {
                "error": f"Failed to get thumbnail: {response.status_code} - {response.text}"
            }

        # Return the binary image data
        return {
            "message": "Thumbnail retrieved successfully",
            "content_type": response.headers.get("content-type", "image/png"),
            "data": response.content.decode("latin-1") if isinstance(response.content, bytes) else response.content,
            "size_bytes": len(response.content),
        }

    except Exception as e:
        return {"error": f"Error retrieving thumbnail: {str(e)}"}


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_chart_export_image(
    ctx: Context, chart_id: int
) -> Dict[str, Any]:
    """
    Export a chart as an image

    Makes a request to the /api/v1/chart/export/ endpoint to export a chart
    as an image file (PNG or JPEG format).

    Args:
        chart_id: ID of the chart to export

    Returns:
        A dictionary with the image data or error information
    """
    payload = {"chart_id": chart_id}

    superset_ctx: SupersetContext = ctx.request_context.lifespan_context

    try:
        response = await superset_ctx.client.post(
            "/api/v1/chart/export/",
            json=payload
        )

        if response.status_code != 200:
            return {
                "error": f"Failed to export chart: {response.status_code} - {response.text}"
            }

        # Return the binary image data
        return {
            "message": "Chart exported successfully",
            "content_type": response.headers.get("content-type", "image/png"),
            "data": response.content.decode("latin-1") if isinstance(response.content, bytes) else response.content,
            "size_bytes": len(response.content),
        }

    except Exception as e:
        return {"error": f"Error exporting chart: {str(e)}"}


# ===== Configuration Tools =====


@mcp.tool()
@handle_api_errors
async def superset_config_get_base_url(ctx: Context) -> Dict[str, Any]:
    """
    Get the base URL of the Superset instance

    Returns the configured Superset base URL that this MCP server is connecting to.
    This can be useful for constructing full URLs to Superset resources or for
    displaying information about the connected instance.

    This tool does not require authentication as it only returns configuration information.

    Returns:
        A dictionary with the Superset base URL
    """
    superset_ctx: SupersetContext = ctx.request_context.lifespan_context

    return {
        "base_url": superset_ctx.base_url,
        "message": f"Connected to Superset instance at: {superset_ctx.base_url}",
    }


# ===== Advanced Data Type Tools =====


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_advanced_data_type_convert(
    ctx: Context, type_name: str, value: Any
) -> Dict[str, Any]:
    """
    Convert a value to an advanced data type

    Makes a request to the /api/v1/advanced_data_type/convert endpoint to transform
    a value into the specified advanced data type format.

    Args:
        type_name: Name of the advanced data type
        value: Value to convert

    Returns:
        A dictionary with the converted value
    """
    params = {
        "type_name": type_name,
        "value": value,
    }

    return await make_api_request(
        ctx, "get", "/api/v1/advanced_data_type/convert", params=params
    )


@mcp.tool()
@requires_auth
@handle_api_errors
async def superset_advanced_data_type_list(ctx: Context) -> Dict[str, Any]:
    """
    Get list of available advanced data types

    Makes a request to the /api/v1/advanced_data_type/types endpoint to retrieve
    all advanced data types supported by this Superset instance.

    Returns:
        A dictionary with available advanced data types and their configurations
    """
    return await make_api_request(ctx, "get", "/api/v1/advanced_data_type/types")


def _get_client_ip(request, trusted_proxies: set) -> str:
    """Resolve the real client IP, respecting X-Forwarded-For from trusted proxies."""
    peer_ip = request.client.host if request.client else "unknown"
    if peer_ip in trusted_proxies:
        xff = request.headers.get("x-forwarded-for", "")
        if xff:
            return xff.split(",")[0].strip()
    return peer_ip


async def _run_http(server: FastMCP, transport: str) -> None:
    """Run HTTP-based transport with optional TLS, IP allowlist, and health check."""
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request as StarletteRequest
    from starlette.responses import JSONResponse, PlainTextResponse

    trusted_proxies = set(t.strip() for t in MCP_TRUSTED_PROXIES.split(",") if t.strip())

    # Build IP allowlist
    allow_nets = None
    if MCP_ALLOWED_IPS:
        allow_nets = [ipaddress.ip_network(e.strip(), strict=False)
                      for e in MCP_ALLOWED_IPS.split(",") if e.strip()]
        logger.info("IP allowlist active: %s", [str(n) for n in allow_nets])

    async def _health_check(request: StarletteRequest) -> JSONResponse:
        """Unauthenticated health check — tests Superset API connectivity."""
        try:
            async with httpx.AsyncClient(base_url=SUPERSET_BASE_URL, timeout=10.0) as client:
                resp = await client.get("/health")
                if resp.status_code == 200:
                    return JSONResponse({"status": "ok"}, status_code=200)
                return JSONResponse({"status": "error", "detail": f"Superset returned {resp.status_code}"}, status_code=503)
        except Exception as exc:
            logger.error("Health check failed: %s", exc)
            return JSONResponse({"status": "error", "detail": str(exc)}, status_code=503)

    class _IPAllowlistMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: StarletteRequest, call_next):
            # Health check — bypass IP allowlist and auth.
            if request.url.path == "/health":
                return await _health_check(request)
            if allow_nets is not None:
                client_ip = _get_client_ip(request, trusted_proxies)
                try:
                    addr = ipaddress.ip_address(client_ip)
                except ValueError:
                    logger.warning("Rejected request with unparseable IP: %s", client_ip)
                    return PlainTextResponse("Forbidden", status_code=403)
                if not any(addr in net for net in allow_nets):
                    logger.warning("Rejected request from %s (not in allowlist)", client_ip)
                    return PlainTextResponse("Forbidden", status_code=403)
            return await call_next(request)

    # Build ASGI app(s)
    if transport == "both":
        from starlette.applications import Starlette

        http_app = server.streamable_http_app()
        sse_app = server.sse_app()

        seen_paths: set = set()
        merged_routes = []
        for route in http_app.routes:
            path = getattr(route, "path", None)
            if path not in seen_paths:
                seen_paths.add(path)
                merged_routes.append(route)
        for route in sse_app.routes:
            path = getattr(route, "path", None)
            if path not in seen_paths:
                seen_paths.add(path)
                merged_routes.append(route)

        starlette_app = Starlette(
            debug=http_app.debug,
            routes=merged_routes,
            middleware=list(http_app.user_middleware),
            lifespan=lambda app: server.session_manager.run(),
        )
    elif transport == "sse":
        starlette_app = server.sse_app()
    else:
        starlette_app = server.streamable_http_app()

    starlette_app.add_middleware(_IPAllowlistMiddleware)

    uv_kwargs: Dict[str, Any] = {
        "host": server.settings.host,
        "port": server.settings.port,
        "log_level": "info",
    }
    if MCP_TLS_CERTFILE and MCP_TLS_KEYFILE:
        uv_kwargs["ssl_certfile"] = MCP_TLS_CERTFILE
        uv_kwargs["ssl_keyfile"] = MCP_TLS_KEYFILE
        logger.info("TLS enabled with cert %s", MCP_TLS_CERTFILE)
    else:
        logger.info("TLS disabled (expecting upstream TLS termination)")

    config = uvicorn.Config(starlette_app, **uv_kwargs)
    uv_server = uvicorn.Server(config)
    await uv_server.serve()


def run():
    """Parse args and start the MCP server."""
    parser = argparse.ArgumentParser(description="Superset MCP Server")
    parser.add_argument(
        "--transport",
        default=None,
        choices=["stdio", "streamable-http", "sse", "both"],
        help="MCP transport (default: from MCP_TRANSPORT env or stdio)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper()), force=True)

    transport = args.transport or MCP_TRANSPORT

    logger.info("Starting Superset MCP server (transport=%s)...", transport)

    if transport in ("streamable-http", "sse", "both"):
        import anyio
        anyio.run(_run_http, mcp, transport)
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    run()
