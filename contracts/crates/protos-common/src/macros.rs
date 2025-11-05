#[macro_export]
macro_rules! method {
    ($fname:ident() -> $ret_ty:ty) => {
        $crate::method! {$fname(()) -> $ret_ty}
    };
    ($fname:ident($request_ty:ty)) => {
        $crate::method! {$fname($request_ty) -> ()}
    };
    ($fname:ident()) => {
        $crate::method! {$fname(()) -> ()}
    };
    ($fname:ident($request_ty:ty) -> $ret_ty:ty) => {
        fn $fname<'life0, 'async_trait>(
            &'life0 self,
            request: tonic::Request<$request_ty>,
        ) -> ::core::pin::Pin<
            Box<
                dyn ::core::future::Future<Output = Result<tonic::Response<$ret_ty>, tonic::Status>>
                    + ::core::marker::Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let (metadata, extensions, body) = request.into_parts();
                let body = body
                    .try_into()
                    .map_err($crate::macros::StatusOrInfallible::to_status)?;
                match self
                    .$fname(Request::from_parts(metadata, extensions, body))
                    .await
                {
                    Ok(response) => Ok(response.map(Into::into)),
                    Err(e) => Err(e.into()),
                }
            })
        }
    };
}

pub trait StatusOrInfallible {
    fn to_status(self) -> Status;
}

impl StatusOrInfallible for Status {
    fn to_status(self) -> Status {
        self
    }
}

impl StatusOrInfallible for Infallible {
    fn to_status(self) -> Status {
        match self {}
    }
}

#[macro_export]
macro_rules! map_trait {
    (impl $adapted_trait:ident for $trait:ty {
        $($fname:ident($($arg:path)?) $(-> $ret:path)?;)+
    }) => {
        impl<T: $adapted_trait + Send + Sync + 'static> $trait for T
            where tonic::Status: From<T::Error>, {
            $($crate::method!{$fname($($arg)?) $( -> $ret)?})+
        }
    };
}
use std::convert::Infallible;

use tonic::Status;
