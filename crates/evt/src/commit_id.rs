//! Commit-ID generation: ULID-format identifiers built from the standard library alone.
//!
//! A commit ID is 26 Crockford base32 characters: a 48-bit millisecond Unix timestamp followed
//! by 80 bits of entropy. The entropy comes from a process-random seed (std's `RandomState`,
//! which the operating system seeds) mixed with an atomic counter, and IDs from one process are
//! strictly increasing within a millisecond, so they sort by creation time.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ENTROPY_MASK: u128 = (1 << 80) - 1;
const TIMESTAMP_MASK: u64 = (1 << 48) - 1;

/// Produces a commit ID for a commit whose command carries none. Inject one with
/// [`Store::with_commit_id_generator`](crate::Store::with_commit_id_generator) to control IDs,
/// for example in tests.
pub type CommitIdGenerator = Arc<dyn Fn() -> String + Send + Sync>;

/// The default generator: [`generate`].
pub(crate) fn default_generator() -> CommitIdGenerator {
    Arc::new(generate)
}

struct State {
    last_ms: u64,
    entropy: u128,
}

/// Generates a ULID-format identifier from the system clock. IDs are unique and increasing within
/// one process.
pub fn generate() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64);

    static STATE: Mutex<State> = Mutex::new(State {
        last_ms: 0,
        entropy: 0,
    });

    generate_at(&STATE, now)
}

fn generate_at(state: &Mutex<State>, now_ms: u64) -> String {
    let mut state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // A clock that steps backwards keeps the last timestamp, so IDs never decrease.
    let mut ms = now_ms.max(state.last_ms) & TIMESTAMP_MASK;
    if ms == state.last_ms && state.entropy < ENTROPY_MASK {
        state.entropy += 1;
    } else {
        // A new millisecond, or an exhausted entropy range (2^80 IDs in one millisecond).
        if ms == state.last_ms {
            ms = (ms + 1) & TIMESTAMP_MASK;
        }
        state.last_ms = ms;
        state.entropy = random_entropy();
    }

    encode((u128::from(ms) << 80) | state.entropy)
}

/// 80 fresh bits: the process-random seed hashed with an atomic counter.
fn random_entropy() -> u128 {
    static SEED: OnceLock<RandomState> = OnceLock::new();
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let seed = SEED.get_or_init(RandomState::new);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);

    let mut high = seed.build_hasher();
    high.write_u64(counter);
    let mut low = seed.build_hasher();
    low.write_u64(!counter);
    low.write_u8(1);

    ((u128::from(high.finish()) << 16) | u128::from(low.finish() & 0xffff)) & ENTROPY_MASK
}

fn encode(value: u128) -> String {
    let mut encoded = [0u8; 26];
    for (index, byte) in encoded.iter_mut().enumerate() {
        let shift = 5 * (25 - index);
        *byte = ALPHABET[((value >> shift) & 0x1f) as usize];
    }

    String::from_utf8(encoded.to_vec()).expect("the alphabet is ASCII")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> Mutex<State> {
        Mutex::new(State {
            last_ms: 0,
            entropy: 0,
        })
    }

    fn decode_timestamp(id: &str) -> u64 {
        id.bytes().take(10).fold(0u64, |acc, byte| {
            let digit = ALPHABET.iter().position(|c| *c == byte).unwrap() as u64;

            (acc << 5) | digit
        })
    }

    #[test]
    fn ids_are_26_crockford_characters_with_the_timestamp_prefix() {
        let id = generate_at(&fresh(), 1_800_000_000_123);

        assert_eq!(id.len(), 26);
        assert!(id.bytes().all(|byte| ALPHABET.contains(&byte)), "{id}");
        assert_eq!(decode_timestamp(&id), 1_800_000_000_123);
        assert!(
            id.as_bytes()[0] <= b'7',
            "a 48-bit timestamp fits 3 bits in the first character"
        );
    }

    #[test]
    fn ids_increase_within_a_millisecond_and_never_go_backwards() {
        let state = fresh();
        let ids: Vec<String> = (0..1000)
            .map(|_| generate_at(&state, 2_000_000_000_000))
            .collect();
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "strictly increasing"
        );

        // A clock stepping back keeps sorting after what was already issued.
        let after_step_back = generate_at(&state, 1_000);
        assert!(ids.last().unwrap() < &after_step_back);
    }

    #[test]
    fn ids_from_separate_milliseconds_differ_in_their_entropy() {
        let state = fresh();
        let first = generate_at(&state, 3_000_000_000_000);
        let second = generate_at(&state, 3_000_000_000_001);

        assert!(first < second);
        assert_ne!(first[10..], second[10..], "fresh entropy per millisecond");
    }

    #[test]
    fn the_default_generator_uses_the_clock() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let id = generate();

        assert!(decode_timestamp(&id) >= before);
    }
}
