"""Authentication constants."""

import os

import polars_cloud.polars_cloud as pcr

CLIENT_ID = "POLARS_CLOUD_CLIENT_ID"
CLIENT_SECRET = "POLARS_CLOUD_CLIENT_SECRET"
ACCESS_TOKEN_ENV = "POLARS_CLOUD_ACCESS_TOKEN"
ACCESS_TOKEN_PATH_ENV = "POLARS_CLOUD_CONFIG_DIR"

POLARS_CLOUD_DOMAIN = os.getenv("POLARS_CLOUD_DOMAIN")
AUTH_DOMAIN = f"auth.{POLARS_CLOUD_DOMAIN or 'prd.cloud.pola.rs'}"
FRONTEND_DOMAIN = (
    f"frontend.{POLARS_CLOUD_DOMAIN}" if POLARS_CLOUD_DOMAIN else "cloud.pola.rs"
)

LOGIN_CLIENT_ID = "PolarsCloud"
LOGIN_AUDIENCE = "account"

ACCESS_TOKEN_FILENAME = "cloud_access_token"
REFRESH_TOKEN_FILENAME = "cloud_refresh_token"

API_CLIENT = pcr.ApiClient()
