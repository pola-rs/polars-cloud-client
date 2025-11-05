use std::time::Duration;

use anyhow::anyhow;
use http::Extensions;
use reqwest::{Request, Response};
use reqwest_middleware::{Middleware, Next};

pub struct RetryTransientMiddleware {
    pub max_retries: u32,
    pub wait_period: Duration,
}

#[async_trait::async_trait]
impl Middleware for RetryTransientMiddleware {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let mut n_tries = 0;
        loop {
            let duplicate_request = req.try_clone().ok_or_else(|| {
                reqwest_middleware::Error::Middleware(anyhow!(
                    "Request object is not cloneable. Are you passing a streaming body?"
                        .to_string()
                ))
            })?;

            let result = next.clone().run(duplicate_request, extensions).await;
            n_tries += 1;
            if result.is_ok() || n_tries > self.max_retries {
                return result;
            }

            if let Err(reqwest_middleware::Error::Reqwest(error)) = &result {
                // Check if error is transient, consider other error types to be fatal
                if error.is_timeout() || error.is_connect() {
                    println!(
                        "{error}: retrying in {} second(s)",
                        self.wait_period.as_secs() * n_tries as u64
                    );
                    tokio::time::sleep(self.wait_period * n_tries).await;
                    continue;
                }
            }

            // If error is fatal return even if we haven't hit max_retries
            return result;
        }
    }
}
