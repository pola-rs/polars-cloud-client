pub mod peek_trailers;
pub mod replay;

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{FutureExt as _, StreamExt as _, TryFutureExt as _, future, stream};
use tonic::{Code, Status};
use tower::retry::backoff::{Backoff, ExponentialBackoffMaker, MakeBackoff};
pub use tower::retry::budget::Budget;
use tower::util::rng::HasherRng;
use tower::{Layer, Service, ServiceExt};
use tracing::{debug, trace};

pub use self::peek_trailers::PeekTrailersBody;
pub use self::replay::ReplayBody;

/// A HTTP retry strategy.
pub trait Policy<E>: Clone + Sized {
    /// Determines if a response should be retried.
    fn is_retryable(&self, result: Result<&http::Response<PeekTrailersBody>, &E>) -> bool;

    /// Prepare headers for the next request.
    fn set_headers(&self, dst: &mut http::HeaderMap, orig: &http::HeaderMap) {
        *dst = orig.clone();
    }

    /// Prepare extensions for the next request.
    fn set_extensions(&self, _dst: &mut http::Extensions, _orig: &http::Extensions) {}

    fn params(&self) -> Params;
}

pub trait RetryRequest {
    fn retry_unavailable(self, params: Params) -> Self;
}

impl<T> RetryRequest for tonic::Request<T> {
    fn retry_unavailable(mut self, params: Params) -> Self {
        let ext = self.extensions_mut();
        ext.insert(RetryUnavailable::new(params));
        self
    }
}
pub struct NoRetry;

#[derive(Clone, Debug)]
pub struct RetryUnavailable {
    params: Params,
}

impl RetryUnavailable {
    pub fn new(params: Params) -> Self {
        Self { params }
    }
}

impl Policy<tonic::transport::Error> for RetryUnavailable {
    fn is_retryable(
        &self,
        result: Result<&http::Response<PeekTrailersBody>, &tonic::transport::Error>,
    ) -> bool {
        match result {
            Ok(response) => {
                let headers = response
                    .body()
                    .peek_trailers()
                    .unwrap_or_else(|| response.headers());
                let status = Status::from_header_map(headers);
                status.is_some_and(|status| status.code() == Code::Unavailable)
            },
            Err(_) => true,
        }
    }

    fn params(&self) -> Params {
        self.params.clone()
    }
}

#[derive(Clone, Debug)]
pub struct Params {
    pub max_retries: usize,
    pub max_request_bytes: usize,
    pub backoff: Option<ExponentialBackoffMaker>,
}

impl Default for Params {
    fn default() -> Self {
        let backoff = ExponentialBackoffMaker::new(
            Duration::from_millis(50),
            Duration::from_secs(5),
            0.15,
            HasherRng::new(),
        )
        .unwrap();
        Self {
            max_retries: usize::MAX,
            max_request_bytes: usize::MAX,
            backoff: Some(backoff),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HttpRetryLayer<P, S> {
    _phantom: PhantomData<fn() -> (P, S)>,
}

impl<P, S> HttpRetryLayer<P, S> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<P, S> Default for HttpRetryLayer<P, S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P, S> Layer<S> for HttpRetryLayer<P, S> {
    type Service = HttpRetry<P, S>;
    fn layer(&self, inner: S) -> Self::Service {
        HttpRetry {
            inner,
            _marker: PhantomData,
        }
    }
}

/// A Retry middleware that attempts to extract a `P` typed request extension to
/// instrument retries. When the request extension is not set, requests are not
/// retried.
#[derive(Clone, Debug)]
pub struct HttpRetry<P, S> {
    inner: S,
    _marker: PhantomData<fn() -> P>,
}

impl<P, S> HttpRetry<P, S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<P, S, E> Service<http::Request<tonic::body::Body>> for HttpRetry<P, S>
where
    P: Policy<E>,
    P: Clone + Send + Sync + std::fmt::Debug + 'static,
    S: Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = E,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    E: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = future::Either<
        <S as Service<http::Request<tonic::body::Body>>>::Future,
        Pin<
            Box<
                dyn Future<Output = Result<http::Response<tonic::body::Body>, Self::Error>>
                    + Send
                    + 'static,
            >,
        >,
    >;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<tonic::body::Body>) -> Self::Future {
        // Retries are configured from request extensions so that they can be
        // configured from both policy and request headers.
        let Some(policy) = req.extensions_mut().remove::<P>() else {
            // If there is no policy, there is no need to retry. This avoids
            // buffering logic in the default case.
            trace!(retryable = false, "Request lacks a retry policy");
            return future::Either::Left(self.inner.call(req));
        };

        let params = policy.params();

        // Since this request is retryable, we need to setup the request body to
        // be buffered/cloneable. If the request body is too large to be cloned,
        // the retry policy is ignored.
        let req = {
            let (head, body) = req.into_parts();
            match ReplayBody::try_new(body, params.max_request_bytes) {
                Ok(body) => http::Request::from_parts(head, body),
                Err(body) => {
                    debug!(retryable = false, "Request body is too large to be retried");
                    return future::Either::Left(
                        self.inner.call(http::Request::from_parts(head, body)),
                    );
                },
            }
        };
        debug!(retryable = true, policy = ?policy);

        // Take the inner service, replacing it with a clone. This allows the
        // readiness from poll_ready to be preserved.
        //
        // Retry::poll_ready is just a pass-through to the inner service, so we
        // can rely on the fact that we've taken the ready inner service handle.
        let pending = self.inner.clone();
        let svc = std::mem::replace(&mut self.inner, pending);
        let call = send_req_with_retries(svc, req, policy, params);
        future::Either::Right(Box::pin(call))
    }
}

#[inline(always)]
fn map_body<E>(
    result: Result<http::Response<PeekTrailersBody>, E>,
) -> Result<http::Response<tonic::body::Body>, E> {
    result.map(|resp| resp.map(tonic::body::Body::new))
}

async fn send_req_with_retries<
    E,
    S: Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = E,
        >,
>(
    // `svc` must be made ready before calling this function.
    mut svc: S,
    request: http::Request<ReplayBody>,
    policy: impl Policy<E>,
    params: Params,
) -> Result<http::Response<tonic::body::Body>, E> {
    // Initial request.
    let mut backup = mk_backup(&request, &policy);
    let mut result = send_req(&mut svc, request).await;
    if !policy.is_retryable(result.as_ref()) {
        tracing::trace!("Success on first attempt");
        return map_body(result);
    }
    if matches!(backup.body().is_capped(), None | Some(true)) {
        // The body was either too large, or we received an early response
        // before the request body was completed read. We cannot safely
        // attempt to send this request again.
        return result.map(|resp| resp.map(tonic::body::Body::new));
    }

    // The response was retryable, so continue trying to dispatch backup
    // requests.
    let mut backoff = params.backoff.map(|mut b| {
        let mut backoff = b.make_backoff();
        stream::repeat_with(move || backoff.next_backoff())
    });
    for n in 1..=params.max_retries {
        if let Some(backoff) = backoff.as_mut() {
            backoff.next().await;
        }

        // The service must be buffered to be cloneable; so if it's not ready,
        // then a circuit breaker is active and requests will be load shed.
        let Some(Ok(svc)) = svc.ready().now_or_never() else {
            return result.map(|resp| resp.map(tonic::body::Body::new));
        };

        tracing::debug!(retry.attempt = n);
        let request = backup;
        backup = mk_backup(&request, &policy);
        result = send_req(svc, request).await;
        if !policy.is_retryable(result.as_ref()) {
            tracing::debug!("Retry success");
            return map_body(result);
        }
        if matches!(backup.body().is_capped(), None | Some(true)) {
            return map_body(result);
        }
    }

    // The result is retryable but we've run out of attempts.
    tracing::debug!("Retry limit exceeded");
    map_body(result)
}

// Make the request and wait for the response. We proactively poll the
// response body for its next frame to convert the response into a
async fn send_req<
    E,
    S: Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = E,
        >,
>(
    svc: &mut S,
    req: http::Request<ReplayBody>,
) -> Result<http::Response<PeekTrailersBody>, E> {
    svc.call(req.map(tonic::body::Body::new))
        .and_then(|rsp| async move {
            tracing::debug!("Peeking at the response trailers");
            let rsp = PeekTrailersBody::map_response(rsp).await;
            Ok(rsp)
        })
        .await
}

fn mk_backup<E>(
    orig: &http::Request<ReplayBody>,
    policy: &impl Policy<E>,
) -> http::Request<ReplayBody> {
    let mut dst = http::Request::new(orig.body().clone());
    *dst.method_mut() = orig.method().clone();
    *dst.uri_mut() = orig.uri().clone();
    *dst.version_mut() = orig.version();
    policy.set_headers(dst.headers_mut(), orig.headers());
    policy.set_extensions(dst.extensions_mut(), orig.extensions());
    dst
}
