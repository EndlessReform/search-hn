//! Experimental boosts over a frozen candidate set, never a new retrieval policy.
//! Multiplication preserves relevance as the foundation. Both boosts are bounded;
//! zero restores RRF exactly, including keyword-only fallback's different scale.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
pub struct Tuning {
    #[serde(default)]
    pub freshness: u8,
    #[serde(default)]
    pub votes: u8,
}
impl Tuning {
    pub fn valid(self) -> bool {
        self.freshness <= 100 && self.votes <= 100
    }
    pub fn score(self, rrf: f64, freshness: f64, popularity: f64) -> f64 {
        rrf * (1.0
            + 4.0 * f64::from(self.freshness) / 100.0 * freshness
            + 4.0 * f64::from(self.votes) / 100.0 * popularity)
    }
}

/// Fixed 30-day half-life, with future timestamps clamped to age zero.
/// Votes saturate at 1000 and grow logarithmically below that cap.
pub fn signals(time: Option<i64>, votes: Option<i64>, now: i64) -> (f64, f64) {
    let freshness = time.map_or(0.0, |time| {
        2f64.powf(-((now - time).max(0) as f64) / (30.0 * 86400.0))
    });
    let popularity = (votes.unwrap_or(0).clamp(0, 1000) as f64).ln_1p() / 1000f64.ln_1p();
    (freshness, popularity)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn neutral_and_bounded_boosts() {
        assert_eq!(Tuning::default().score(0.017, 1.0, 1.0), 0.017);
        assert_eq!(signals(Some(0), Some(1000), 30 * 86400), (0.5, 1.0));
        assert_eq!(signals(None, None, 0), (0.0, 0.0));
        assert_eq!(signals(Some(100), Some(5000), 0), (1.0, 1.0));
        let tuning = Tuning {
            freshness: 100,
            votes: 0,
        };
        assert!(tuning.score(0.016, 1.0, 0.0) > tuning.score(0.018, 0.0, 0.0));
        assert!(!Tuning {
            freshness: 101,
            votes: 0
        }
        .valid());
    }
}
