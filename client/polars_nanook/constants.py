"""Authentication constants."""

import polars_nanook.polars_nanook as pcr

CLIENT_ID = "NANOOK_CLIENT_ID"
CLIENT_SECRET = "NANOOK_CLIENT_SECRET"
ACCESS_TOKEN_ENV = "NANOOK_ACCESS_TOKEN"
ACCESS_TOKEN_PATH_ENV = "NANOOK_CONFIG_DIR"

DOMAIN = pcr.legacy_env_var("NANOOK_DOMAIN")
AUTH_DOMAIN = f"auth.{DOMAIN or 'prd.nanook.pola.rs'}"
FRONTEND_DOMAIN = f"frontend.{DOMAIN}" if DOMAIN else "nanook.pola.rs"

ACCESS_TOKEN_FILENAME = "cloud_access_token"
REFRESH_TOKEN_FILENAME = "cloud_refresh_token"

API_CLIENT = pcr.ApiClient()
