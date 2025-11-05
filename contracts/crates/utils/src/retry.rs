use std::cell::Cell;
use std::time::{Duration, Instant};

pub struct Exponential {
    multiply: f64,
    initial: Duration,
    maximum: Option<Duration>,
}

impl Exponential {
    pub fn new(initial: Duration) -> Self {
        Self {
            multiply: 2.0,
            initial,
            maximum: None,
        }
    }

    pub fn multiply(mut self, by: f64) -> Self {
        self.multiply = by;
        self
    }

    pub fn maximum(mut self, maximum: Duration) -> Self {
        self.maximum = Some(maximum);
        self
    }
}

pub struct Fixed(Duration);
impl Fixed {
    pub fn new(duration: Duration) -> Self {
        Self(duration)
    }
}

pub trait Backoff: Sized {
    fn next_duration(&self, last_duration: Option<Duration>) -> Option<Duration>;

    fn deadline(self, deadline: Duration) -> Deadline<Self> {
        Deadline::new(self, deadline)
    }
}

impl Backoff for Exponential {
    fn next_duration(&self, last_duration: Option<Duration>) -> Option<Duration> {
        let Some(last_duration) = last_duration else {
            return Some(self.initial);
        };

        let duration = last_duration.mul_f64(self.multiply);
        match self.maximum {
            None => Some(duration),
            Some(maximum) => Some(duration.min(maximum)),
        }
    }
}

impl Backoff for Fixed {
    fn next_duration(&self, _last_duration: Option<Duration>) -> Option<Duration> {
        Some(self.0)
    }
}

pub struct Deadline<B: Backoff> {
    deadline: Cell<Option<Instant>>,
    max_time: Duration,
    inner: B,
}

impl<B: Backoff> Deadline<B> {
    pub fn new(backoff: B, deadline: Duration) -> Self {
        Self {
            inner: backoff,
            max_time: deadline,
            deadline: Default::default(),
        }
    }
}

impl<B: Backoff> Backoff for Deadline<B> {
    fn next_duration(&self, last_duration: Option<Duration>) -> Option<Duration> {
        let now = Instant::now();
        let deadline = self.deadline.get().unwrap_or_else(|| now + self.max_time);
        self.deadline.set(Some(deadline));
        let next_duration = self.inner.next_duration(last_duration)?;
        let now = Instant::now();
        if now > deadline {
            None
        } else {
            Some((deadline - now).min(next_duration))
        }
    }
}

#[derive(Debug)]
pub enum OperationResult<T, E> {
    Ok(T),
    Retry(E),
    Err(E),
}

impl<T, E> From<Result<T, E>> for OperationResult<T, E> {
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(v) => Self::Ok(v),
            Err(e) => Self::Retry(e),
        }
    }
}

#[macro_export]
macro_rules! retry {
    ($backoff:expr, $task:expr, $sleep:expr) => {
        $crate::retry!($backoff, $task, $sleep, warn_after = Duration::ZERO);
    };
    ($backoff:expr, $task:expr, $sleep:expr, warn_after = $warn_after:expr) => {
        async {
            let backoff = $backoff;
            let mut last_duration = None;
            let mut last_warned = std::time::Instant::now();
            loop {
                match $crate::retry::OperationResult::from($task.await) {
                    $crate::retry::OperationResult::Ok(v) => return Ok(v),
                    $crate::retry::OperationResult::Retry(e) => {
                        if last_warned.elapsed() >= $warn_after {
                            tracing::warn!("Retrying {:?}", e);
                            last_warned = std::time::Instant::now();
                        } else {
                            tracing::debug!("Retrying {:?}", e);
                        }
                        last_duration =
                            $crate::retry::Backoff::next_duration(&backoff, last_duration);
                        let Some(duration) = last_duration else {
                            return Err(e);
                        };
                        $sleep(duration).await;
                    },
                    $crate::retry::OperationResult::Err(e) => return Err(e),
                }
            }
        }
    };
}
