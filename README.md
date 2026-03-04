# Superset MCP Integration
[![smithery badge](https://smithery.ai/badge/@aptro/superset-mcp)](https://smithery.ai/server/@aptro/superset-mcp)

MCP server for interacting with Apache Superset, enabling AI agents to connect to and control a Superset instance programmatically.

## Setup Instructions

### Installing via Smithery

To install Superset Integration for Claude Desktop automatically via [Smithery](https://smithery.ai/server/@aptro/superset-mcp):

```bash
npx -y @smithery/cli install @aptro/superset-mcp --client claude
```

### Manual Installation

1. **Set Up Superset Locally**

   Run this script to start Superset locally:
   ```bash
   git clone --branch 4.1.1 --depth 1 https://github.com/apache/superset && \
   cd superset && \
   docker compose -f docker-compose-image-tag.yml up
   ```

   Once Superset is running, you should be able to access it at http://localhost:8088 with default credentials:
   - Username: admin
   - Password: admin

2. **Clone This Repository**

   Clone this repository to your local machine.

3. **Configure Environment Variables**

   Create a `.env` file in the root directory with your Superset credentials.

   **Option 1: Using Username/Password (for local Superset instances)**
   ```
   SUPERSET_BASE_URL=http://localhost:8088  # Change to your Superset URL
   SUPERSET_USERNAME=your_username
   SUPERSET_PASSWORD=your_password
   ```

   **Option 2: Using JWT Token**
   ```
   SUPERSET_BASE_URL=https://your-superset-instance.com
   SUPERSET_JWT_TOKEN=your_jwt_token_here
   ```

   **Option 3: Using Session Cookie (for OAuth-enabled Superset instances like Google OAuth)**
   ```
   SUPERSET_BASE_URL=https://your-superset-instance.com
   SUPERSET_SESSION_COOKIE=your_session_cookie_here
   ```

   To get your session cookie:
   1. Log in to Superset in your browser
   2. Open Developer Tools (F12)
   3. Go to Application > Cookies > your Superset URL
   4. Copy the value of the `session` cookie
   5. Paste it into the `.env` file

   **Option 4: Using Guest Tokens (for embedded dashboards)**

   If your Superset instance is configured with guest token support for embedded dashboards, add these settings from your Superset configuration:
   ```
   SUPERSET_BASE_URL=https://your-superset-instance.com
   GUEST_TOKEN_JWT_SECRET=your_secret_from_superset_config
   GUEST_TOKEN_JWT_AUDIENCE=your_audience_from_superset_config
   GUEST_ROLE_NAME=your_guest_role_name
   ```

   The MCP server can then generate guest tokens on-demand for accessing specific dashboards/charts.

   Note: Priority order is: Session Cookie > JWT Token > Username/Password > Stored Token > Guest Token (generated on-demand)

4. **Install Dependencies**

   ```bash
   uv pip install .
   ```

5. **Install MCP Config for Claude**

   To use with Claude Desktop app:
   ```bash
   mcp install main.py
   ```

## Usage with Claude

After setup, you can interact with your Superset instance via Claude using natural language requests. Here are some examples:

### Dashboard Management

- **View dashboards**: "Show me all my Superset dashboards"
- **Get dashboard details**: "Show me the details of dashboard with ID 5"
- **Create dashboard**: "Create a new dashboard titled 'Sales Overview'"
- **Update dashboard**: "Update dashboard 3 to have the title 'Updated Sales Report'"
- **Delete dashboard**: "Delete dashboard with ID 7"
- **Get dashboard screenshot**: "Get a screenshot of dashboard 432"
- **Get dashboard thumbnail**: "Get a thumbnail of dashboard 5"

### Chart Management

- **List all charts**: "What charts do I have in my Superset instance?"
- **View chart details**: "Show me the details of chart with ID 10"
- **Create chart**: "Create a new bar chart using dataset 3"
- **Update chart**: "Update chart 5 to use a line visualization instead of bar"
- **Delete chart**: "Delete chart with ID 12"
- **Export chart as image**: "Export chart 10 as an image"

### Database and Dataset Operations

- **List databases**: "Show me all databases connected to Superset"
- **List datasets**: "What datasets are available in my Superset instance?"
- **Get database tables**: "What tables are available in database with ID 1?"
- **Execute SQL**: "Run this SQL query on database 1: SELECT * FROM users LIMIT 10"
- **Create dataset**: "Create a new dataset from table 'customers' in database 2"
- **Update database**: "Update the connection settings for database 3"
- **Delete database**: "Delete database connection with ID 4"
- **Validate SQL**: "Is this SQL valid for database 2: SELECT * FROM customers JOIN orders"
- **Get database catalogs**: "Show me the catalogs available in database 1"
- **Get database functions**: "What functions are available in database 2?"
- **Check related objects**: "What dashboards and charts use database 1?"

### SQL Lab Features

- **Execute queries**: "Run this SQL query: SELECT COUNT(*) FROM orders"
- **Format SQL**: "Format this SQL query: SELECT id,name,age FROM users WHERE age>21"
- **Estimate query cost**: "Estimate the cost of this query: SELECT * FROM large_table"
- **Get saved queries**: "Show me all my saved SQL queries"
- **Get query results**: "Get the results of query with key 'abc123'"

### User and System Information

- **View user info**: "Who am I logged in as?"
- **Get user roles**: "What roles do I have in Superset?"
- **View recent activity**: "Show me recent activity in my Superset instance"
- **Get menu data**: "What menu items do I have access to?"
- **Get base URL**: "What is the URL of the Superset instance I'm connected to?"

### Guest Token Management

- **Generate guest token**: "Generate a guest token for dashboard 432"
- **Generate with custom role**: "Generate a guest token for dashboard 432 with role 'Admin'"

### Tag Management

- **List tags**: "Show me all tags in my Superset instance"
- **Create tag**: "Create a new tag called 'Finance'"
- **Delete tag**: "Delete the tag with ID 5"
- **Tag an object**: "Add the tag 'Finance' to dashboard 3"
- **Remove tag**: "Remove the tag 'Finance' from chart 7"

## Available MCP Tools

This plugin offers the following MCP tools that Claude can use:

### Authentication
- `superset_auth_check_token_validity` - Check if the current access token is valid
- `superset_auth_refresh_token` - Refresh the access token
- `superset_auth_authenticate_user` - Authenticate with Superset

### Dashboards
- `superset_dashboard_list` - List all dashboards
- `superset_dashboard_get_by_id` - Get a specific dashboard
- `superset_dashboard_create` - Create a new dashboard
- `superset_dashboard_update` - Update an existing dashboard
- `superset_dashboard_delete` - Delete a dashboard
- `superset_dashboard_cache_screenshot` - Cache a screenshot of a dashboard
- `superset_dashboard_get_screenshot` - Get a cached screenshot of a dashboard
- `superset_dashboard_get_thumbnail` - Get a thumbnail image of a dashboard

### Guest Tokens
- `superset_guest_token_generate` - Generate a guest token for embedded dashboards or charts

### Charts
- `superset_chart_list` - List all charts
- `superset_chart_get_by_id` - Get a specific chart
- `superset_chart_create` - Create a new chart
- `superset_chart_update` - Update an existing chart
- `superset_chart_delete` - Delete a chart
- `superset_chart_export_image` - Export a chart as an image

### Databases
- `superset_database_list` - List all databases
- `superset_database_get_by_id` - Get a specific database
- `superset_database_create` - Create a new database connection
- `superset_database_get_tables` - List tables in a database
- `superset_database_schemas` - Get schemas for a database
- `superset_database_test_connection` - Test a database connection
- `superset_database_update` - Update an existing database connection
- `superset_database_delete` - Delete a database connection
- `superset_database_get_catalogs` - Get catalogs for a database
- `superset_database_get_connection` - Get database connection information
- `superset_database_get_function_names` - Get function names supported by a database
- `superset_database_get_related_objects` - Get charts and dashboards associated with a database
- `superset_database_validate_sql` - Validate arbitrary SQL against a database
- `superset_database_validate_parameters` - Validate database connection parameters

### Datasets
- `superset_dataset_list` - List all datasets
- `superset_dataset_get_by_id` - Get a specific dataset
- `superset_dataset_create` - Create a new dataset

### SQL Lab
- `superset_sqllab_execute_query` - Execute a SQL query
- `superset_sqllab_get_saved_queries` - List saved SQL queries
- `superset_sqllab_format_sql` - Format a SQL query
- `superset_sqllab_get_results` - Get query results
- `superset_sqllab_estimate_query_cost` - Estimate query cost
- `superset_sqllab_export_query_results` - Export query results to CSV
- `superset_sqllab_get_bootstrap_data` - Get SQL Lab bootstrap data

### Queries
- `superset_query_list` - List all queries
- `superset_query_get_by_id` - Get a specific query
- `superset_query_stop` - Stop a running query

### Saved Queries
- `superset_saved_query_get_by_id` - Get a specific saved query
- `superset_saved_query_create` - Create a new saved query

### User Information
- `superset_user_get_current` - Get current user info
- `superset_user_get_roles` - Get user roles

### Activity
- `superset_activity_get_recent` - Get recent activity data

### System
- `superset_menu_get` - Get menu data
- `superset_config_get_base_url` - Get the base URL of the Superset instance

### Tags
- `superset_tag_list` - List all tags
- `superset_tag_create` - Create a new tag
- `superset_tag_get_by_id` - Get a specific tag
- `superset_tag_objects` - Get objects associated with tags
- `superset_tag_delete` - Delete a tag
- `superset_tag_object_add` - Add a tag to an object
- `superset_tag_object_remove` - Remove a tag from an object

### Exploration Tools
- `superset_explore_form_data_create` - Create form data for chart exploration
- `superset_explore_form_data_get` - Get form data for chart exploration
- `superset_explore_permalink_create` - Create a permalink for chart exploration
- `superset_explore_permalink_get` - Get a permalink for chart exploration

### Advanced Data Types
- `superset_advanced_data_type_convert` - Convert a value to an advanced data type
- `superset_advanced_data_type_list` - List available advanced data types

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| SUPERSET_BASE_URL | URL of your Superset instance | http://localhost:8088 |
| SUPERSET_USERNAME | Username for Superset (for username/password auth) | None |
| SUPERSET_PASSWORD | Password for Superset (for username/password auth) | None |
| SUPERSET_JWT_TOKEN | JWT token for direct token-based authentication | None |
| SUPERSET_SESSION_COOKIE | Session cookie for OAuth-based authentication (highest priority) | None |
| GUEST_TOKEN_JWT_SECRET | Secret key for generating guest tokens (from Superset config) | None |
| GUEST_TOKEN_JWT_AUDIENCE | JWT audience for guest tokens (from Superset config) | None |
| GUEST_ROLE_NAME | Guest role name for embedded dashboards (from Superset config) | None |

## Troubleshooting

- If you encounter authentication issues, verify your credentials in the `.env` file
  - For JWT token authentication, ensure you have a valid JWT token (not a placeholder like `JWT_AUDIENCE`)
  - JWT tokens should be in the format: `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ...` (three parts separated by dots)
- Make sure Superset is running and accessible at the URL specified in your `.env` file
- Check that you're using a compatible version of Superset (tested with version 4.1.1)
- Ensure the port used by the MCP server is not being used by another application
- For screenshot functionality:
  - The Superset instance must have the `THUMBNAILS` and `ENABLE_DASHBOARD_SCREENSHOT_ENDPOINTS` feature flags enabled
  - A proper cache backend (like Redis) must be configured instead of NullCache
  - If these requirements aren't met, screenshots may not be available

## Security Notes

- Your Superset credentials are stored only in your local `.env` file
- The access token is stored in `.superset_token` file in the project directory
- All authentication happens directly between the MCP server and your Superset instance
- No credentials are transmitted to Claude or any third parties
- For production use, consider using more secure authentication methods

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT
