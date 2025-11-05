use std::fmt::{Display, Write};
use std::future::{Future, IntoFuture};
use std::pin::Pin;

use futures_util::FutureExt;
use http::HeaderValue;
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use polars_axum_models::Pagination;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::ApiError::StatusError;

pub struct ApiRequestBuilder<'a> {
    client: &'a reqwest_middleware::ClientWithMiddleware,
    rest_type: http::Method,
    url: String,
    auth_header: String,
    has_parameters: bool,
    json_data: Option<Vec<u8>>,
}

impl<'a> ApiRequestBuilder<'a> {
    pub fn new(
        client: &'a reqwest_middleware::ClientWithMiddleware,
        rest_type: http::Method,
        domain: String,
        auth_header: String,
    ) -> Self {
        Self {
            client,
            rest_type,
            url: domain,
            auth_header,
            has_parameters: false,
            json_data: None,
        }
    }

    pub fn parameter_vec<T, I>(self, name: &str, values: I) -> Self
    where
        T: Display,
        I: IntoIterator<Item = T>,
    {
        let mut out = String::new();
        let mut iter = values.into_iter();
        if let Some(first) = iter.next() {
            write!(&mut out, "{first}").unwrap();
            for item in iter {
                write!(&mut out, ",{item}").unwrap();
            }
        }
        self.parameter(name, out)
    }

    pub fn parameter<T: Display>(mut self, name: &str, value: T) -> Self {
        if self.has_parameters {
            self.url = format!("{}&{name}={value}", self.url);
        } else {
            self.url = format!("{}?{name}={value}", self.url);
            self.has_parameters = true;
        }
        self
    }

    pub fn parameter_opt<T: Display>(self, name: &str, value: Option<T>) -> Self {
        let Some(value) = value else {
            return self;
        };
        self.parameter(name, value)
    }

    pub fn parameter_vec_opt<T, I>(self, name: &str, values: Option<I>) -> Self
    where
        T: Display,
        I: IntoIterator<Item = T>,
    {
        let Some(values) = values else {
            return self;
        };
        self.parameter_vec(name, values)
    }

    pub fn pagination(self, pagination: &Pagination) -> Self {
        self.parameter("page", pagination.page)
            .parameter("limit", pagination.limit)
            .parameter("offset", pagination.offset)
    }

    pub fn json<T: Serialize>(mut self, data: T) -> Self {
        self.json_data = Some(serde_json::to_vec(&data).unwrap());
        self
    }
}

impl IntoFuture for ApiRequestBuilder<'_> {
    type Output = reqwest_middleware::Result<ApiRequestResult>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        let builder = match self.rest_type {
            http::Method::GET => self.client.get(self.url),
            http::Method::POST => self.client.post(self.url),
            http::Method::PUT => self.client.put(self.url),
            http::Method::DELETE => self.client.delete(self.url),
            http::Method::HEAD => self.client.head(self.url),
            http::Method::PATCH => self.client.patch(self.url),
            _ => unreachable!(),
        };

        let builder = builder.header(AUTHORIZATION, self.auth_header.clone());

        let builder = if let Some(json_data) = self.json_data {
            builder
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(json_data)
        } else {
            builder
        };

        let future = builder
            .send()
            .map(|x| x.map(|response| ApiRequestResult { response }));
        Box::pin(future)
    }
}

pub struct ApiRequestResult {
    response: reqwest::Response,
}

impl ApiRequestResult {
    pub async fn json<T: DeserializeOwned>(self) -> crate::error::Result<T> {
        if !self.response.status().is_success() {
            return Err(StatusError {
                status: self.response.status(),
                url: self.response.url().clone(),
                body: self.response.text().await?,
            });
        }

        Ok(self.response.json::<T>().await?)
    }

    pub fn response(self) -> reqwest::Result<reqwest::Response> {
        self.response.error_for_status()
    }

    pub async fn empty(self) -> crate::error::Result<()> {
        if !self.response.status().is_success() {
            return Err(StatusError {
                status: self.response.status(),
                url: self.response.url().clone(),
                body: self.response.text().await?,
            });
        }

        Ok(())
    }
}
