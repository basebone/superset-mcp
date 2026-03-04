# Setting Up Admin Guest Tokens for Your Superset Instance

## Current State
Your Superset config already has:
- ✅ Guest token authentication enabled
- ✅ `BxpEmbed` role configured as default guest role
- ✅ JWT secret and audience configured
- ✅ Redis configured for Celery
- ⚠️ Missing: Thumbnail/screenshot cache configuration
- ⚠️ Missing: Screenshot feature flags

## What You Need to Do

### Step 1: Update Your Superset Config

Add these sections to your `superset_config.py`:

```python
# 1. Add screenshot feature flags to your existing FEATURE_FLAGS
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_VIRTUALIZATION": True,
    "SQLLAB_BACKEND_PERSISTENCE": True,
    "SQL_VALIDATORS_BY_ENGINE": True,
    "ENABLE_JAVASCRIPT_CONTROLS": True,
    "DRILL_TO_DETAIL": True,
    "EMBEDDED_SUPERSET": True,
    "EMBEDDABLE_CHARTS": True,
    "ENABLE_SUPERSET_META_DB": True,
    "ALERT_REPORTS": True,
    "ENABLE_REST_API": True,

    # ADD THESE TWO:
    "THUMBNAILS": True,
    "ENABLE_DASHBOARD_SCREENSHOT_ENDPOINTS": True,
}

# 2. Configure Redis cache for thumbnails/screenshots
# (Replace your existing cache config if you have one)
THUMBNAIL_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 86400,  # 24 hours
    'CACHE_KEY_PREFIX': 'superset_thumbnails_',
    'CACHE_REDIS_HOST': 'localhost',  # Your Redis host
    'CACHE_REDIS_PORT': 6379,         # Your Redis port
    'CACHE_REDIS_DB': 1,              # Different DB from Celery (which uses 0)
}

DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 86400,
    'CACHE_KEY_PREFIX': 'superset_data_',
    'CACHE_REDIS_HOST': 'localhost',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 2,
}

# 3. Screenshot configuration
SCREENSHOT_LOCATE_WAIT = 10
SCREENSHOT_LOAD_WAIT = 60
```

### Step 2: Create a Custom Power User Role (Recommended)

**Option A: Via Superset UI (Easiest)**

1. Log in to Superset as an Admin
2. Go to **Settings** → **List Roles**
3. Click the **+** button to add a new role
4. Name it: `GuestPowerUser` (or any name you prefer)
5. Add these permissions:

```
View/Menu               Permission
--------------------    ----------------------
Dashboard               can read
Dashboard               can screenshot
Dashboard               can cache dashboard screenshot
Chart                   can read
Chart                   can screenshot
Dataset                 can read
Database                can read
SavedQuery              can read
Query                   can read
Superset                can explore
Superset                can dashboard
Superset                can slice
```

6. Click **Save**

**Option B: Use the Existing Admin Role (Not Recommended)**

You can use the built-in `Admin` role, but this is **NOT recommended** for production because:
- Admin has full destructive permissions (delete, modify, etc.)
- Guest tokens can be leaked or intercepted
- Embedded dashboards don't need admin-level access

### Step 3: Restart Superset

```bash
# Stop Superset
docker-compose down

# Or if using systemd
sudo systemctl stop superset

# Start Superset with new configuration
docker-compose up -d

# Or
sudo systemctl start superset
```

### Step 4: Test Guest Token Generation

```bash
# Install PyJWT if not already installed
pip install PyJWT>=2.8.0

# Test with the new role
python test_guest_token.py 432 GuestPowerUser

# Or test with Admin role (if you must)
python test_guest_token.py 432 Admin
```

### Step 5: Use in MCP

Now you can generate guest tokens with your custom role:

```python
# Using the MCP tool via Claude:
# "Generate a guest token for dashboard 432 with role GuestPowerUser"

# This will call:
superset_guest_token_generate(
    resource_type="dashboard",
    resource_id=432,
    role_name="GuestPowerUser"
)

# For chart screenshots:
superset_guest_token_generate(
    resource_type="chart",
    resource_id=123,
    role_name="GuestPowerUser"
)
```

## Role Comparison

| Role | Use Case | Permissions | Recommended |
|------|----------|-------------|-------------|
| **BxpEmbed** | Basic embedded dashboard viewing | Very limited, read-only specific dashboards | ✅ For public embedding |
| **GuestPowerUser** (custom) | Power users needing broad access | Read all dashboards/charts, explore data, screenshots | ✅ For trusted embedded apps |
| **Gamma** | Read-only users | Read shared dashboards and charts | ✅ For internal viewers |
| **Alpha** | Data analysts | Create and edit content | ⚠️ Use with caution |
| **Admin** | Full system access | Everything including user management | ❌ Not for guest tokens |

## Troubleshooting

### Screenshots Not Working

```bash
# Check if Redis is running
redis-cli ping
# Should return: PONG

# Check if Chrome/Chromium is installed for screenshots
which google-chrome
which chromium-browser

# Check Superset logs
docker logs superset_app  # or your container name
tail -f /var/log/superset/superset.log
```

### Guest Token Returns 403 Forbidden

1. **Role doesn't exist**: Check that the role name matches exactly (case-sensitive)
2. **Role lacks permissions**: Add the necessary permissions in Settings → List Roles
3. **JWT secret mismatch**: Verify `GUEST_TOKEN_JWT_SECRET` matches in both configs
4. **Audience mismatch**: Verify `GUEST_TOKEN_JWT_AUDIENCE` matches

### Token Works But Can't Access Dashboard

1. **Dashboard permissions**: The role needs `can read on Dashboard`
2. **Dataset permissions**: The role needs access to the underlying datasets
3. **Database permissions**: The role might need `can read on Database`

## Security Best Practices

1. ✅ **Create custom roles** with only needed permissions
2. ✅ **Never use Admin role** for guest tokens
3. ✅ **Keep JWT secrets secure** - don't commit to git
4. ✅ **Use HTTPS** for your Superset instance
5. ✅ **Set appropriate token expiration** (default: 1 hour)
6. ✅ **Implement row-level security (RLS)** if needed
7. ✅ **Monitor token usage** via Superset logs
8. ✅ **Rotate JWT secrets periodically**

## Example: Complete MCP Usage

```python
# 1. Generate a guest token for a specific dashboard with custom role
response = superset_guest_token_generate(
    resource_type="dashboard",
    resource_id=432,
    role_name="GuestPowerUser",
    user_username="embedded_viewer_001",
    user_first_name="Embedded",
    user_last_name="Viewer"
)

# Response:
# {
#   "message": "Guest token generated successfully",
#   "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
#   "resource_type": "dashboard",
#   "resource_id": 432,
#   "role": "GuestPowerUser",
#   "expires_in": 3600
# }

# 2. Use the token to cache a screenshot
token = response["token"]

# Set the token in your MCP context, then:
superset_dashboard_cache_screenshot(dashboard_id=432)

# 3. Get the screenshot
superset_dashboard_get_screenshot(dashboard_id=432, cache_key="...")
```

## Quick Reference: Your Config Values

```python
SUPERSET_BASE_URL=https://your-superset-instance.com
GUEST_TOKEN_JWT_SECRET=your_jwt_secret_here
GUEST_TOKEN_JWT_AUDIENCE=your_audience_here
GUEST_ROLE_NAME=BxpEmbed  # Default role
```

## Next Steps

1. ✅ Update superset_config.py with thumbnail cache config
2. ✅ Add screenshot feature flags
3. ✅ Create custom `GuestPowerUser` role in Superset UI
4. ✅ Restart Superset
5. ✅ Test token generation with `test_guest_token.py`
6. ✅ Generate tokens with `role_name` parameter in MCP
7. ✅ Use tokens to access dashboards and screenshots

For more details, see `superset_config_additions.py` and `GUEST_TOKEN_SETUP.md`.
