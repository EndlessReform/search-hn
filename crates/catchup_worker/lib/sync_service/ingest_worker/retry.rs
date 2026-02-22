use std::future::Future;
use std::time::Duration;

use super::super::types::RetryPolicy;

/// Terminal state returned by the shared retry runner.
#[derive(Debug)]
pub struct RetryTerminal<E> {
    pub error: E,
    pub attempts: u32,
    pub exhausted_retryable: bool,
}

/// Executes one async operation under the shared micro-retry policy.
///
/// The caller supplies `is_retryable` to classify each error. Retry delays are derived from
/// `RetryPolicy` using deterministic per-item jitter so concurrent workers don't synchronize
/// their retries.
pub async fn run_with_retry<T, E, F, Fut, R>(
    retry_policy: &RetryPolicy,
    item_id: i64,
    mut op: F,
    mut is_retryable: R,
) -> Result<(T, u32), RetryTerminal<E>>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    R: FnMut(&E) -> bool,
{
    let max_attempts = retry_policy.max_attempts.max(1);

    for attempt in 1..=max_attempts {
        match op(attempt).await {
            Ok(value) => return Ok((value, attempt)),
            Err(error) => {
                let retryable = is_retryable(&error);
                if retryable && attempt < max_attempts {
                    let delay = compute_backoff_delay(retry_policy, attempt, item_id);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    continue;
                }
                return Err(RetryTerminal {
                    error,
                    attempts: attempt,
                    exhausted_retryable: retryable && attempt == max_attempts,
                });
            }
        }
    }

    unreachable!("retry runner should return from loop")
}

pub fn compute_backoff_delay(policy: &RetryPolicy, attempt: u32, item_id: i64) -> Duration {
    if policy.initial_backoff.is_zero() && policy.jitter.is_zero() {
        return Duration::ZERO;
    }

    let shift = u32::min(attempt.saturating_sub(1), 20);
    let exponential_ms = policy
        .initial_backoff
        .as_millis()
        .saturating_mul(1u128 << shift);
    let capped_ms = exponential_ms.min(policy.max_backoff.as_millis());

    let jitter_ms = if policy.jitter.is_zero() {
        0
    } else {
        let jitter_cap = policy.jitter.as_millis();
        deterministic_jitter(item_id, attempt, jitter_cap)
    };

    let total_ms = capped_ms.saturating_add(jitter_ms);
    Duration::from_millis(total_ms.min(u64::MAX as u128) as u64)
}

fn deterministic_jitter(item_id: i64, attempt: u32, jitter_cap: u128) -> u128 {
    if jitter_cap == 0 {
        return 0;
    }

    let mut x = (item_id as u64) ^ (attempt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;

    (x as u128) % (jitter_cap + 1)
}
