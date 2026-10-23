/// FNV-1a 64-bit hash, written by hand.
///
/// We use this (and NOT std's DefaultHasher) for shard routing because
/// its output is stable across Rust versions and processes.
/// A key must map to the same shard after a restart.
pub fn fnv1a64(key: &str) -> u64 {
    const OFFSET_BASIS: u64 = 14695981039346656037;
    const PRIME: u64 = 1099511628211;

    let mut hash = OFFSET_BASIS;
    for b in key.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_vectors() {
        // Hard-coded so any accidental change to the hash is caught.
        // Values computed from the FNV-1a spec.
        assert_eq!(fnv1a64(""), 14695981039346656037);
        assert_eq!(fnv1a64("a"), 12638187200555641996);
        assert_eq!(fnv1a64("hello"), 11831194018420276491);
        assert_eq!(fnv1a64("color"), 8644027876048135736);
    }
}
