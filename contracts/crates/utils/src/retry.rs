use std::cell::Cell;
use std::hash::{BuildHasher, Hasher, RandomState};
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

#[derive(Clone, Copy, Debug)]
pub struct Linear {
    initial: Duration,
    step: Duration,
    maximum: Option<Duration>,
    jitter: f64,
}

impl Linear {
    pub fn new(initial: Duration, step: Duration) -> Self {
        Self {
            initial,
            step,
            maximum: None,
            jitter: 0.0,
        }
    }

    pub fn maximum(mut self, maximum: Duration) -> Self {
        self.maximum = Some(maximum);
        self
    }

    /// Randomly scales each duration by a factor in `[1 - fraction, 1 + fraction]`.
    pub fn jitter(mut self, fraction: f64) -> Self {
        self.jitter = fraction;
        self
    }

    /// Delay before the zero-based `attempt`-th retry.
    pub fn delay(&self, attempt: u32) -> Duration {
        let base = self.clamp(
            self.initial
                .saturating_add(self.step.saturating_mul(attempt)),
        );
        let jitter_factor = 1.0 + self.jitter * (2.0 * random_unit_interval() - 1.0);
        self.clamp(base.mul_f64(jitter_factor))
    }

    pub fn into_backoff(self) -> LinearBackoff {
        LinearBackoff {
            linear: self,
            attempt: Cell::new(0),
        }
    }

    fn clamp(&self, duration: Duration) -> Duration {
        self.maximum
            .map_or(duration, |maximum| duration.min(maximum))
    }
}

pub struct LinearBackoff {
    linear: Linear,
    attempt: Cell<u32>,
}

fn random_unit_interval() -> f64 {
    let bits = RandomState::new().build_hasher().finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
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

impl Backoff for LinearBackoff {
    fn next_duration(&self, _last_duration: Option<Duration>) -> Option<Duration> {
        let attempt = self.attempt.get();
        self.attempt.set(attempt.saturating_add(1));
        Some(self.linear.delay(attempt))
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
        $crate::retry!($backoff, $task, $sleep, warn_after = Duration::ZERO)
    };
    ($backoff:expr, $task:expr, $sleep:expr, warn_after = $warn_after:expr) => {
        $crate::retry!($backoff, $task, $sleep, warn_after = $warn_after, warning = "Retrying")
    };
    ($backoff:expr, $task:expr, $sleep:expr, warn_after = $warn_after:expr, warning = $warning:literal) => {
        async {
            let backoff = $backoff;
            let mut last_duration = None;
            let mut last_warned = std::time::Instant::now();
            let start = std::time::Instant::now();
            loop {
                match $crate::retry::OperationResult::from($task.await) {
                    $crate::retry::OperationResult::Ok(v) => return Ok(v),
                    $crate::retry::OperationResult::Retry(error) => {
                        let elapsed = start.elapsed();
                        if last_warned.elapsed() >= $warn_after {
                            tracing::warn!(error = ?error, elapsed_ms = start.elapsed().as_millis(), $warning);
                            last_warned = std::time::Instant::now();
                        } else {
                            tracing::debug!(error = ?error, elapsed_ms = start.elapsed().as_millis(), $warning);
                        }
                        last_duration =
                            $crate::retry::Backoff::next_duration(&backoff, last_duration);
                        let Some(duration) = last_duration else {
                            return Err(error);
                        };
                        $sleep(duration).await;
                    },
                    $crate::retry::OperationResult::Err(error) => return Err(error),
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_grows_by_step_and_caps_at_maximum() {
        let linear = Linear::new(Duration::from_millis(5), Duration::from_millis(5))
            .maximum(Duration::from_millis(12));

        let delays: Vec<_> = (0..4).map(|attempt| linear.delay(attempt)).collect();

        assert_eq!(delays, [5, 10, 12, 12].map(Duration::from_millis));
    }
}
