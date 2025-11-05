use std::thread::sleep;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use serde::Deserialize;
use serde_json::json;

use crate::client::AuthToken;
use crate::client::utils::{Tokens, write_tokens};
use crate::constants::{AUTH_DOMAIN, LOGIN_AUDIENCE, LOGIN_CLIENT_ID};
use crate::error::ApiError;

#[derive(Deserialize)]
struct AuthResponse {
    device_code: String,
    user_code: String,
    verification_uri_complete: String,
    expires_in: u64,
    interval: u64,
}

#[allow(clippy::result_large_err)]
pub async fn login_new(
    connection_pool: reqwest_middleware::ClientWithMiddleware,
) -> Result<AuthToken, ApiError> {
    let device: AuthResponse = connection_pool
        .post(format!(
            "https://{}/realms/Polars/protocol/openid-connect/auth/device",
            *AUTH_DOMAIN
        ))
        .form(&json!({"client_id": LOGIN_CLIENT_ID, "audience": LOGIN_AUDIENCE}))
        .send()
        .await?
        .json()
        .await?;

    // Allow user to give us permission
    webbrowser::open(&device.verification_uri_complete).unwrap();
    println!("Please complete the login process in your browser.");
    println!(
        "If your browser did not open automatically, please go to the URL: {}",
        device.verification_uri_complete
    );
    println!("Your login code is: {}", device.user_code);

    // Get token with device code
    let url = format!(
        "https://{}/realms/Polars/protocol/openid-connect/token",
        *AUTH_DOMAIN
    );

    let data = json!({
        "client_id": LOGIN_CLIENT_ID,
        "device_code": device.device_code,
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
    });

    let start_time = Instant::now();
    let mut last_check = Instant::now();
    while start_time.elapsed() < Duration::from_secs(device.expires_in) {
        if last_check.elapsed() < Duration::from_secs(device.interval) {
            sleep(Duration::from_millis(50));
            continue;
        }
        last_check = Instant::now();

        let response = connection_pool.post(url.clone()).form(&data).send().await?;

        if response.status().is_success() {
            let tokens = response.json::<Tokens>().await.map_err(ApiError::from)?;
            let _ = write_tokens(&tokens.access_token, &tokens.refresh_token);
            return Ok(AuthToken::AccessToken {
                token: tokens.access_token,
                refresh_token: tokens.refresh_token,
            });
        }
    }
    Err(ApiError::AuthLoadError(
        anyhow!("Logging in has timed out, Please try again").into(),
    ))
}
