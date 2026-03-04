# Guest Token Authentication Setup Guide

This guide explains how to use guest tokens with the Superset MCP for embedded dashboards and custom role access.

**✅ Updated to match PHP `superfull\Auth::getGuestTokenFor()` implementation**

## Overview

Guest tokens allow you to access Superset dashboards and charts without requiring user authentication. This is particularly useful for:

- Embedding dashboards in external applications
- Providing limited access to specific resources
- Working with Superset instances that use OAuth (like Google OAuth)

## Configuration

### 1. Get Your Superset Guest Token Configuration

These values match your PHP Auth class:

```php
// From superfull\Auth
const SECRET = 'your_jwt_secret_here';
// Audience: 'your_audience_here'
```

### 2. Add to Your `.env` File

```env
SUPERSET_BASE_URL=https://your-superset-instance.com
GUEST_TOKEN_JWT_SECRET=your_jwt_secret_here
GUEST_TOKEN_JWT_AUDIENCE=your_audience_here
GUEST_ROLE_NAME=BxpEmbed  # Optional: default role if needed
```

## What Changed to Match PHP

The MCP implementation now **exactly matches** your PHP code:

### ✅ Token Payload Structure
- **No expiration** (`exp`) field - tokens don't expire
- **Integer resource ID** - not converted to string
- **Separate `rls` and `rls_rules` fields**
- **Field order matches PHP** exactly

### ✅ Default User Changed
From generic "guest" to your PHP defaults:
```python
{
    "username": "remote_access",
    "first_name": "remote_access",
    "last_name": "bot"
}
```

## Usage

### Generate a Guest Token

Using the MCP tool `superset_guest_token_generate`:

```python
# Basic usage - uses default role from GUEST_ROLE_NAME
token = superset_guest_token_generate(
    resource_type="dashboard",
    resource_id=432
)

# With custom role (e.g., for more permissions)
token = superset_guest_token_generate(
    resource_type="dashboard",
    resource_id=432,
    role_name="Admin"  # Override the default BxpEmbed role
)

# With custom user info
token = superset_guest_token_generate(
    resource_type="dashboard",
    resource_id=432,
    user_username="john_doe",
    user_first_name="John",
    user_last_name="Doe",
    role_name="Gamma"
)

# With row-level security rules
token = superset_guest_token_generate(
    resource_type="dashboard",
    resource_id=432,
    rls_rules=[
        {"clause": "company_id = 123"}
    ],
    role_name="Alpha"
)
```

### Using Different Roles

The `role_name` parameter allows you to override the default `GUEST_ROLE_NAME` from your configuration. Common Superset roles:

- **Admin**: Full access to everything
- **Alpha**: Can create and edit content
- **Gamma**: Read-only access to shared content
- **sql_lab**: Access to SQL Lab only
- **Public**: Very limited public access
- **Your Custom Roles**: Any roles configured in your Superset instance

Example use cases:

1. **Limited Embed (BxpEmbed)**: Use the default role for basic embedded dashboard viewing
2. **Data Analyst (Alpha)**: Generate a token with 'Alpha' role for users who need to explore and create charts
3. **Executive Dashboard (Gamma)**: Use 'Gamma' role for read-only access to curated dashboards
4. **Admin Access**: Use 'Admin' role when you need full control (use carefully!)

## Testing

Use the provided test script to verify your guest token configuration:

```bash
# Test with default role
python test_guest_token.py 432

# Test with custom role
python test_guest_token.py 432 Admin
```

## Token Payload Structure

The generated JWT token now matches your PHP implementation exactly:

### PHP Implementation:
```php
$payload = [
    "aud" => "basebone",
    "resources" => [["id" => $dashboardId, "type" => "dashboard"]],
    "rls" => [],
    "rls_rules" => [],
    "user" => [
        "first_name" => "remote_access",
        "last_name" => "bot",
        "username" => "remote_access"
    ],
    "type" => "guest"
];
```

### Python MCP Implementation (matches exactly):
```json
{
  "aud": "your_audience_here",
  "resources": [{
    "id": 432,
    "type": "dashboard"
  }],
  "rls": [],
  "rls_rules": [],
  "user": {
    "username": "remote_access",
    "first_name": "remote_access",
    "last_name": "bot"
  },
  "type": "guest"
}
```

**Note:** No `exp` (expiration) field - tokens don't expire, matching your PHP implementation.

## Troubleshooting

### Token Generation Fails

- Ensure `GUEST_TOKEN_JWT_SECRET` and `GUEST_TOKEN_JWT_AUDIENCE` are set in `.env`
- Verify the values match your Superset configuration exactly
- Check that PyJWT is installed: `pip install PyJWT>=2.8.0`

### Token Works But Access Denied

- The role you specified might not exist in Superset
- The role might not have permissions for the requested resource
- Check Superset's role configuration: Settings > Roles
- Verify the resource ID is correct

### Role Override Not Working

- Make sure the role name exactly matches a role in Superset (case-sensitive)
- Check that `ENABLE_EMBEDDED_SUPERSET` is enabled in Superset config
- Verify the role has the necessary permissions

## Security Considerations

1. **Keep your JWT secret safe**: Never commit `GUEST_TOKEN_JWT_SECRET` to version control
2. **Use appropriate roles**: Don't give 'Admin' role to guest tokens unless absolutely necessary
3. **Set up RLS rules**: Use row-level security to limit data access
4. **⚠️ No expiration**: Tokens generated by this implementation **do not expire** (matching PHP). Consider implementing token rotation or additional security measures if needed
5. **Rotate secrets periodically**: Update your JWT secret in both Superset and the MCP configuration

## API Endpoints That Work With Guest Tokens

Guest tokens can be used with these Superset API endpoints:

- `GET /api/v1/dashboard/{id}` - Get dashboard details
- `GET /api/v1/chart/{id}` - Get chart details
- `POST /api/v1/chart/data` - Get chart data
- `GET /api/v1/dashboard/{id}/thumbnail/{digest}/` - Get dashboard thumbnail
- `POST /api/v1/dashboard/{id}/cache_dashboard_screenshot/` - Cache dashboard screenshot

The permissions depend on the role assigned to the guest token.

## Example: Embedding a Dashboard

```python
# 1. Generate a guest token with appropriate role
response = superset_guest_token_generate(
    resource_type="dashboard",
    resource_id=432,
    role_name="Gamma",  # Read-only access
    user_username="embedded_viewer"
)

token = response["token"]

# 2. Use the token to embed the dashboard
embed_url = f"https://your-superset-instance.com/superset/dashboard/432/?standalone=2&guest_token={token}"

# 3. Display in iframe or use with API calls
# Note: Token has no expiration (matches PHP implementation)
```

## Verification

To verify tokens match between PHP and Python:

```bash
# PHP
$phpToken = Auth::getGuestTokenFor(432);

# Python via Claude/MCP
# Ask: "Generate a guest token for dashboard 432"

# Decode both tokens at jwt.io - they should be identical
```
