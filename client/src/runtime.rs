use std::time::Duration;

use pyo3::{PyErr, Python};

pub struct Runtime(pub tokio::runtime::Runtime);

impl Runtime {
    pub fn new() -> Self {
        Self(tokio::runtime::Runtime::new().unwrap())
    }

    pub fn block_on<F: Future + Send>(&self, future: F) -> Result<F::Output, PyErr>
    where
        <F as Future>::Output: Send,
    {
        self.0.block_on(async {
            tokio::select! {
                res = check_signals() => Err(res),
                res = future => Ok(res)
            }
        })
    }
}

async fn check_signals() -> PyErr {
    loop {
        match Python::attach(|py| py.check_signals()) {
            Ok(()) => {
                // No signals to handle, continue waiting
                tokio::time::sleep(Duration::from_millis(100)).await
            },
            Err(e) => {
                // Signals detected that need handling
                return e;
            },
        }
    }
}
