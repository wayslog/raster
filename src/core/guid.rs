//! 128-bit identifiers for sessions and checkpoint tokens (FASTER `Guid`).
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Guid(pub u128);

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Guid {
    /// Unique within the process and, with high probability, across processes.
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        let local = 0u8;
        let seed = nanos
            ^ ((std::process::id() as u64) << 32)
            ^ (&local as *const u8 as u64).rotate_left(17);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        let high = mix(seed ^ mix(count));
        let low = mix(high ^ count.rotate_left(29) ^ nanos);
        Self(((high as u128) << 64) | low as u128)
    }

    pub fn parse(text: &str) -> Option<Self> {
        let hex: String = text.chars().filter(|c| *c != '-').collect();
        (hex.len() == 32).then(|| u128::from_str_radix(&hex, 16).ok().map(Self))?
    }
}

impl fmt::Display for Guid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v = self.0;
        write!(
            f,
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            (v >> 96) as u32,
            (v >> 80) as u16,
            (v >> 64) as u16,
            (v >> 48) as u16,
            v & 0xffff_ffff_ffff
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_and_round_trips() {
        let ids: std::collections::HashSet<_> = (0..10_000).map(|_| Guid::new()).collect();
        assert_eq!(ids.len(), 10_000);
        let id = Guid::new();
        assert_eq!(Guid::parse(&id.to_string()), Some(id));
        assert_eq!(Guid::parse("not-a-guid"), None);
    }
}
