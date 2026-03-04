import os

# =============================================================================
# ADDITIONS FOR YOUR SUPERSET CONFIG TO SUPPORT ADMIN GUEST TOKENS
# =============================================================================
# Add these sections to your existing superset_config.py

# 1. Enable additional feature flags for guest tokens and screenshots
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

    # ADD THESE NEW FLAGS:
    "THUMBNAILS": True,  # Enable thumbnail generation
    "ENABLE_DASHBOARD_SCREENSHOT_ENDPOINTS": True,  # Enable screenshot API
    "DYNAMIC_PLUGINS": True,  # Optional: for plugin support
}

# 2. Guest token configuration (you already have this, but here's the complete setup)
GUEST_ROLE_NAME = os.environ.get('GUEST_ROLE_NAME', 'BxpEmbed')  # Default role for guest tokens
GUEST_TOKEN_JWT_SECRET = os.environ['GUEST_TOKEN_JWT_SECRET']
GUEST_TOKEN_JWT_EXP_SECONDS = 3600  # 1 hour
GUEST_TOKEN_JWT_AUDIENCE = os.environ.get('GUEST_TOKEN_JWT_AUDIENCE', 'basebone')

# 3. Configure cache for thumbnails/screenshots (IMPORTANT!)
# By default, Superset uses NullCache which doesn't actually cache anything
# You need to configure a real cache backend like Redis

from flask_caching import Cache

# Configure thumbnail cache
THUMBNAIL_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 86400,  # 24 hours
    'CACHE_KEY_PREFIX': 'superset_thumbnails_',
    'CACHE_REDIS_HOST': 'localhost',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,  # Use a different DB than Celery
}

# Configure data cache
DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 86400,
    'CACHE_KEY_PREFIX': 'superset_data_',
    'CACHE_REDIS_HOST': 'localhost',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 2,
}

# 4. Screenshot configuration
SCREENSHOT_LOCATE_WAIT = 10  # Seconds to wait for page to load
SCREENSHOT_LOAD_WAIT = 60    # Seconds to wait for charts to render

# =============================================================================
# IMPORTANT: ROLE CONFIGURATION IN SUPERSET UI
# =============================================================================
# Guest tokens work by assigning a role to the guest user.
# The permissions available to the guest token depend entirely on the role.
#
# To use Admin role with guest tokens:
# 1. The JWT token generator in MCP will embed the role in the token
# 2. Superset will honor that role when the guest accesses resources
#
# However, for security reasons, you should:
# - Create a custom role instead of using 'Admin'
# - Give it only the permissions needed for your use case
#
# To create a custom role in Superset:
# 1. Go to Settings > List Roles
# 2. Click the + button to add a new role
# 3. Name it something like "GuestAdmin" or "EmbedFull"
# 4. Assign permissions based on what you need:
#    - can read on Dashboard
#    - can read on Chart
#    - can read on Dataset
#    - can explore on Superset
#    - can dashboard on Superset
#    - can screenshot on Dashboard (for screenshots)
#    - etc.
#
# Then use that role name when generating tokens in MCP:
# superset_guest_token_generate(
#     resource_type="dashboard",
#     resource_id=432,
#     role_name="GuestAdmin"  # Your custom role
# )

# =============================================================================
# SECURITY CONSIDERATIONS FOR ADMIN GUEST TOKENS
# =============================================================================
#
# Using 'Admin' role for guest tokens is NOT RECOMMENDED because:
# 1. Admin has full permissions including:
#    - Deleting dashboards and charts
#    - Modifying database connections
#    - Changing user roles and permissions
#    - Accessing all data without restrictions
# 2. Guest tokens are meant for embedded/limited access
# 3. Tokens can be intercepted or leaked
#
# RECOMMENDED APPROACH:
# Create a custom role with specific permissions:

# Example custom role configuration (do this in Superset UI):
"""
Role Name: "GuestPowerUser"
Permissions:
- can read on Dashboard
- can read on Chart
- can read on Dataset
- can read on Database
- can explore on Superset
- can dashboard on Superset
- can slice on Superset
- can screenshot on Dashboard
- can cache dashboard screenshot on Dashboard
- can read on SavedQuery
- can read on Query
- can sqllab on Superset (if SQL Lab access needed)

This gives extensive read access without destructive permissions.
"""

# =============================================================================
# ALTERNATIVE: PROGRAMMATIC ROLE CREATION (Advanced)
# =============================================================================
# If you want to create the custom role programmatically, you can add this
# to your superset_config.py, but it's usually better to do it via UI:

def create_guest_admin_role():
    """
    Creates a custom GuestAdmin role with extensive read permissions
    This should be run once to set up the role
    """
    from superset import security_manager
    from flask_appbuilder.security.sqla.models import Role, Permission

    role_name = "GuestPowerUser"

    # Check if role exists
    role = security_manager.find_role(role_name)
    if not role:
        # Create the role
        role = security_manager.add_role(role_name)

        # Define permissions to add
        permissions = [
            ('can_read', 'Dashboard'),
            ('can_read', 'Chart'),
            ('can_read', 'Dataset'),
            ('can_read', 'Database'),
            ('can_explore', 'Superset'),
            ('can_dashboard', 'Superset'),
            ('can_slice', 'Superset'),
            ('can_screenshot', 'Dashboard'),
            ('can_cache_dashboard_screenshot', 'Dashboard'),
            ('can_read', 'SavedQuery'),
            ('can_read', 'Query'),
        ]

        # Add permissions to role
        for permission_name, view_menu_name in permissions:
            pv = security_manager.find_permission_view_menu(
                permission_name, view_menu_name
            )
            if pv:
                security_manager.add_permission_role(role, pv)

    return role

# Uncomment this if you want to create the role automatically on startup:
# from superset import app
# with app.app_context():
#     create_guest_admin_role()

# =============================================================================
# USAGE WITH MCP
# =============================================================================
# After setting up the custom role, generate tokens with it:

"""
# In your MCP client or via Claude:

# Generate token with custom power user role
superset_guest_token_generate(
    resource_type="dashboard",
    resource_id=432,
    role_name="GuestPowerUser"  # Use your custom role
)

# Or if you really need Admin (not recommended):
superset_guest_token_generate(
    resource_type="dashboard",
    resource_id=432,
    role_name="Admin"  # Full admin access - use carefully!
)

# The token will include the specified role and Superset will
# apply that role's permissions when the guest accesses resources
"""
