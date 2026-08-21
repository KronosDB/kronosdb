//! Weighted consistent-hash ring for routing-key stickiness.
//!
//! Each handler contributes `load_factor` virtual nodes to the ring, so
//! placement is weight-proportional and a membership change only moves the
//! keys owned by the handler that joined or left — unlike `hash % n`, which
//! reshuffles nearly every key. Rings are rebuilt on registration changes
//! (rare) and read on dispatch (hot), so build cost is irrelevant and
//! lookup is a binary search.
//!
//! Hashing is FNV-1a, implemented here rather than via `std`'s hasher so
//! the placement is deterministic across builds — a property the cluster
//! messaging fabric (ADR-0007) will rely on when several nodes must agree
//! on key ownership.

/// A weighted consistent-hash ring mapping keys to handler-list indices.
pub struct Ring {
    /// (vnode hash, index into the handler list this ring was built from),
    /// sorted by hash.
    vnodes: Vec<(u64, usize)>,
}

impl Ring {
    /// Builds a ring from `(stable_id, weight)` pairs, one per handler, in
    /// handler-list order. Weights are clamped to [1, 1024] vnodes.
    pub fn build<'a>(handlers: impl Iterator<Item = (&'a str, i32)>) -> Self {
        let mut vnodes = Vec::new();
        for (idx, (id, weight)) in handlers.enumerate() {
            let vnode_count = weight.clamp(1, 1024) as u32;
            for v in 0..vnode_count {
                vnodes.push((vnode_hash(id, v), idx));
            }
        }
        vnodes.sort_unstable();
        Self { vnodes }
    }

    /// Returns the handler-list index owning `key`, or `None` on an empty ring.
    pub fn lookup(&self, key: &str) -> Option<usize> {
        if self.vnodes.is_empty() {
            return None;
        }
        let h = mix64(fnv1a(key.as_bytes()));
        // First vnode at or after the key's position; wrap to the start.
        let at = self.vnodes.partition_point(|&(vh, _)| vh < h);
        let (_, idx) = self.vnodes[at % self.vnodes.len()];
        Some(idx)
    }
}

fn vnode_hash(id: &str, vnode: u32) -> u64 {
    let mut h = fnv1a(id.as_bytes());
    for byte in vnode.to_le_bytes() {
        h = (h ^ byte as u64).wrapping_mul(FNV_PRIME);
    }
    mix64(h)
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h = (h ^ b as u64).wrapping_mul(FNV_PRIME);
    }
    h
}

/// murmur3 finalizer. FNV disperses poorly in the high bits, and ring
/// placement orders by the full u64 — without this, vnodes cluster and
/// weight proportionality collapses.
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring_of(ids: &[(&str, i32)]) -> Ring {
        Ring::build(ids.iter().map(|&(id, w)| (id, w)))
    }

    #[test]
    fn empty_ring_returns_none() {
        let ring = ring_of(&[]);
        assert_eq!(ring.lookup("order-1"), None);
    }

    #[test]
    fn lookup_is_deterministic() {
        let ring = ring_of(&[("a", 100), ("b", 100), ("c", 100)]);
        for key in ["order-1", "order-2", "x"] {
            assert_eq!(ring.lookup(key), ring.lookup(key));
        }
    }

    #[test]
    fn distribution_follows_weights() {
        // "a" has 3x the weight of "b" — expect roughly 3x the keys.
        let ring = ring_of(&[("a", 300), ("b", 100)]);
        let mut counts = [0usize; 2];
        for i in 0..10_000 {
            counts[ring.lookup(&format!("key-{i}")).unwrap()] += 1;
        }
        let ratio = counts[0] as f64 / counts[1] as f64;
        assert!(
            (2.0..4.5).contains(&ratio),
            "weight ratio 3.0 expected, got {ratio:.2} ({counts:?})"
        );
    }

    #[test]
    fn membership_change_moves_only_departed_share() {
        // With hash % n, removing one of four handlers remaps ~75% of keys.
        // On the ring, only the departed handler's share (~25%) may move.
        let ids = ["a", "b", "c", "d"];
        let before = ring_of(&[("a", 100), ("b", 100), ("c", 100), ("d", 100)]);
        let after = ring_of(&[("a", 100), ("b", 100), ("c", 100)]);

        let keys: Vec<String> = (0..5_000).map(|i| format!("key-{i}")).collect();
        let moved = keys
            .iter()
            .filter(|k| {
                let b = ids[before.lookup(k).unwrap()];
                let a = ids[after.lookup(k).unwrap()];
                b != "d" && b != a
            })
            .count();
        // Keys not owned by the departed handler must not move at all.
        assert_eq!(moved, 0, "{moved} surviving-handler keys were remapped");
    }
}
