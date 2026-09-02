use std::time::Duration;

use anyhow::anyhow;
use http::Extensions;
use reqwest::{Request, Response, StatusCode};
use reqwest_middleware::{Middleware, Next};

pub type IsTransient = fn(&reqwest_middleware::Result<Response>) -> bool;

pub struct RetryTransientMiddleware {
    pub max_retries: u32,
    pub wait_period: Duration,
    pub is_transient: IsTransient,
}

impl RetryTransientMiddleware {
    pub fn new(max_retries: u32, wait_period: Duration) -> Self {
        Self {
            max_retries,
            wait_period,
            is_transient: default_is_transient,
        }
    }
}

fn default_is_transient(result: &reqwest_middleware::Result<Response>) -> bool {
    match result {
        Ok(response) => response.status() == StatusCode::TOO_MANY_REQUESTS,
        Err(reqwest_middleware::Error::Reqwest(error)) => error.is_timeout() || error.is_connect(),
        Err(_) => false,
    }
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
            if n_tries > self.max_retries {
                return result;
            }

            if (self.is_transient)(&result) {
                tracing::debug!(
                    "retrying in {} second(s)",
                    self.wait_period.as_secs() * n_tries as u64
                );
                tokio::time::sleep(self.wait_period * n_tries).await;
                continue;
            }

            return result;
        }
    }
}
