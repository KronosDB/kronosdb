//! The membership filter of a sealed segment's keys.
//!
//! A segment's key count is known when it seals, so the filter is sized
//! exactly then: a binary fuse filter with 8-bit fingerprints (about 9 bits a
//! key, a false positive rate near 0.4%). It is stored inside the segment's
//! `.idx` and answers from the mapped bytes; nothing is deserialized.
//!
//! The filter is persisted, so everything that decides its answer is part of
//! the file format: the key hash below and the filter's own hashing, which is
//! `xorf`'s. That is why `xorf` is pinned to one version and why a test here
//! holds a filter built from fixed keys to fixed bytes: a release that hashed
//! differently would read existing filters as "definitely absent" for keys
//! that are there.

use xorf::{BinaryFuse8, BinaryFuse8Ref, DmaSerializable, Filter, FilterRef};

use crate::error::Error;

/// Bytes of the filter's descriptor, ahead of its fingerprints.
pub(crate) const DESCRIPTOR_LEN: usize = BinaryFuse8::DESCRIPTOR_LEN;

/// The stable 64-bit hash a key enters the filter as: MurmurHash64A with a
/// fixed seed. Stable across releases and platforms by construction (it is
/// part of the `.idx` format), cheap on short keys, and spread well enough
/// for a filter. A collision between two keys costs a false positive for one
/// of them, which the key directory then answers.
pub fn key_hash(key: &[u8]) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;
    const SEED: u64 = 0x4b52_4f4e_4f53_4442; // "KRONOSDB"

    let mut h = SEED ^ (key.len() as u64).wrapping_mul(M);
    let (words, tail) = key.as_chunks::<8>();
    for word in words {
        let mut k = u64::from_le_bytes(*word);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    if !tail.is_empty() {
        let mut last = [0u8; 8];
        last[..tail.len()].copy_from_slice(tail);
        h ^= u64::from_le_bytes(last);
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

/// Builds the filter section for keys with these hashes: the descriptor,
/// then the fingerprints. `None` when there is nothing to filter (no keys)
/// or construction failed, which leaves the segment without a filter: every
/// probe then goes to the key directory, which is correct, only slower.
pub(crate) fn build(mut hashes: Vec<u64>) -> Option<Vec<u8>> {
    // The filter needs distinct keys; two keys that hash alike are one key
    // to it.
    hashes.sort_unstable();
    hashes.dedup();
    if hashes.is_empty() {
        return None;
    }
    match BinaryFuse8::try_from(&hashes) {
        Ok(filter) => {
            let mut out = vec![0u8; DESCRIPTOR_LEN];
            filter.dma_copy_descriptor_to(&mut out);
            out.extend_from_slice(filter.dma_fingerprints());
            Some(out)
        }
        Err(error) => {
            tracing::warn!(
                keys = hashes.len(),
                error,
                "could not build a segment's key filter; its reads will consult the key directory"
            );
            None
        }
    }
}

/// A filter section read in place.
#[derive(Clone, Copy)]
pub(crate) struct MappedFilter<'a> {
    descriptor: &'a [u8],
    fingerprints: &'a [u8],
}

impl<'a> MappedFilter<'a> {
    /// Reads a filter section. The bytes must have passed their checksum;
    /// this checks that the descriptor and the fingerprints agree on the
    /// filter's shape, which is what keeps a probe inside the fingerprints.
    pub(crate) fn parse(section: &'a [u8]) -> Result<Self, Error> {
        let corrupted = |what: &str| Error::Corrupted {
            message: format!(".idx key filter {what}"),
        };
        if section.len() < DESCRIPTOR_LEN {
            return Err(corrupted("is shorter than its descriptor"));
        }
        let (descriptor, fingerprints) = section.split_at(DESCRIPTOR_LEN);
        let word = |at: usize| u32::from_le_bytes(descriptor[at..at + 4].try_into().unwrap());
        let (length, mask, count_length) = (word(8), word(12), word(16));
        let expected = u64::from(count_length) + 2 * u64::from(length);
        if !length.is_power_of_two()
            || mask != length - 1
            || count_length % length != 0
            || fingerprints.len() as u64 != expected
        {
            return Err(corrupted("descriptor does not match its fingerprints"));
        }
        Ok(Self {
            descriptor,
            fingerprints,
        })
    }

    pub(crate) fn contains(&self, hash: u64) -> bool {
        BinaryFuse8Ref::from_dma(self.descriptor, self.fingerprints).contains(&hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_hash_is_frozen() {
        // Part of the .idx format: these values may never change.
        assert_eq!(key_hash(b""), 0x9146_2240_6682_3752);
        assert_eq!(key_hash(b"orderId"), 0xb67f_e671_ad93_6773);
        assert_eq!(
            key_hash(b"\x07\0\0\0orderIdo-123456789"),
            0x3761_69df_dc3e_c8c5
        );
    }

    #[test]
    fn a_filter_is_frozen_too() {
        // xorf builds deterministically; a release of it that built or
        // probed differently must fail here, not in a customer's reads.
        let hashes: Vec<u64> = (0..1000u64).map(|k| key_hash(&k.to_le_bytes())).collect();
        let section = build(hashes.clone()).unwrap();
        assert_eq!(section.len(), 1428);
        assert_eq!(crc32c::crc32c(&section), 0x27a5_684f);
        let filter = MappedFilter::parse(&section).unwrap();
        assert!(hashes.iter().all(|&h| filter.contains(h)));
    }

    #[test]
    fn false_positives_are_rare_and_negatives_never_happen() {
        let present: Vec<u64> = (0..50_000u64)
            .map(|k| key_hash(format!("present-{k}").as_bytes()))
            .collect();
        let section = build(present.clone()).unwrap();
        let filter = MappedFilter::parse(&section).unwrap();
        assert!(present.iter().all(|&h| filter.contains(h)));
        let false_positives = (0..100_000u64)
            .filter(|k| filter.contains(key_hash(format!("absent-{k}").as_bytes())))
            .count();
        let rate = false_positives as f64 / 100_000.0;
        assert!(rate < 0.006, "false positive rate {rate}");
        // About 9 bits a key.
        assert!(section.len() < 50_000 * 10 / 8, "{} bytes", section.len());
    }

    #[test]
    fn small_and_empty_key_sets() {
        assert!(build(Vec::new()).is_none());
        for n in 1..40u64 {
            let hashes: Vec<u64> = (0..n).map(|k| key_hash(&k.to_be_bytes())).collect();
            // Duplicates collapse instead of failing the build.
            let mut doubled = hashes.clone();
            doubled.extend_from_slice(&hashes);
            let section = build(doubled).unwrap();
            let filter = MappedFilter::parse(&section).unwrap();
            assert!(hashes.iter().all(|&h| filter.contains(h)), "{n} keys");
        }
    }

    #[test]
    fn a_descriptor_that_disagrees_with_its_fingerprints_is_refused() {
        let hashes: Vec<u64> = (0..100u64).map(|k| key_hash(&k.to_le_bytes())).collect();
        let section = build(hashes).unwrap();
        let mut short = section.clone();
        short.pop();
        assert!(matches!(
            MappedFilter::parse(&short),
            Err(Error::Corrupted { .. })
        ));
        let mut bad_mask = section.clone();
        bad_mask[12] ^= 0x01;
        assert!(MappedFilter::parse(&bad_mask).is_err());
        assert!(MappedFilter::parse(&section[..4]).is_err());
    }
}
