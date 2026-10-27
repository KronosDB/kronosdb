//! Record CRCs with the CPU's CRC32C instruction.
//!
//! A record's CRC32C covers its flags byte and its body. The `crc32c` crate
//! already uses the instruction, but for a record-sized input (a few hundred
//! bytes) its cost is mostly its own: aligning the buffer, a byte loop at each
//! end, and two calls to cover the flags byte and then the body. Calling the
//! instruction directly is about three times faster for a 488-byte record
//! (`examples/crc_bench.rs`). Where there is no instruction the crate is used.
//!
//! Checking several records at once in lockstep, to hide the instruction's
//! latency, was tried and measured no faster than a loop over records: the
//! out-of-order core overlaps independent records on its own.

/// Where one record's checked bytes are inside a buffer.
#[derive(Clone, Copy)]
pub struct Span {
    /// The record's flags byte.
    pub flags: u8,
    /// The body: `data[body_start..body_end]`.
    pub body_start: usize,
    pub body_end: usize,
    /// The CRC stored in the record's header.
    pub stored: u32,
}

/// The CRC the writer stores for a record: CRC32C over the flags byte, then
/// the body.
pub fn record_crc(flags: u8, body: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("sse4.2") {
        // SAFETY: the feature was just detected.
        return unsafe { hw::record_crc(flags, body) };
    }
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("crc") {
        // SAFETY: the feature was just detected.
        return unsafe { hw::record_crc(flags, body) };
    }
    portable_record_crc(flags, body)
}

/// Index of the first span whose stored CRC does not match its bytes.
pub fn first_mismatch(data: &[u8], spans: &[Span]) -> Option<usize> {
    spans
        .iter()
        .position(|s| record_crc(s.flags, &data[s.body_start..s.body_end]) != s.stored)
}

fn portable_record_crc(flags: u8, body: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&[flags]), body)
}

/// The CRC32C instruction of the CPU. The state is the raw register: seeded
/// with all ones and inverted at the end, like the crate does.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
mod hw {
    #[cfg(target_arch = "x86_64")]
    mod isa {
        use std::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};

        #[inline]
        #[target_feature(enable = "sse4.2")]
        pub fn word(crc: u32, word: u64) -> u32 {
            _mm_crc32_u64(u64::from(crc), word) as u32
        }

        #[inline]
        #[target_feature(enable = "sse4.2")]
        pub fn byte(crc: u32, byte: u8) -> u32 {
            _mm_crc32_u8(crc, byte)
        }
    }

    #[cfg(target_arch = "aarch64")]
    mod isa {
        use std::arch::aarch64::{__crc32cb, __crc32cd};

        #[inline]
        #[target_feature(enable = "crc")]
        pub fn word(crc: u32, word: u64) -> u32 {
            __crc32cd(crc, word)
        }

        #[inline]
        #[target_feature(enable = "crc")]
        pub fn byte(crc: u32, byte: u8) -> u32 {
            __crc32cb(crc, byte)
        }
    }

    macro_rules! implement {
        ($feature:literal) => {
            /// # Safety
            /// The CPU must support the instruction.
            #[target_feature(enable = $feature)]
            pub unsafe fn record_crc(flags: u8, body: &[u8]) -> u32 {
                let mut crc = isa::byte(!0, flags);
                let mut words = body.chunks_exact(8);
                for word in &mut words {
                    crc = isa::word(crc, u64::from_le_bytes(word.try_into().unwrap()));
                }
                for &byte in words.remainder() {
                    crc = isa::byte(crc, byte);
                }
                !crc
            }
        };
    }

    #[cfg(target_arch = "x86_64")]
    implement!("sse4.2");
    #[cfg(target_arch = "aarch64")]
    implement!("crc");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pseudo(seed: &mut u64) -> u8 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        (*seed >> 24) as u8
    }

    /// Records of every length from 0 to 70 and some long ones, laid out back
    /// to back: the check agrees with the crate on all of them, and finds a
    /// single flipped bit anywhere.
    #[test]
    fn the_check_agrees_with_the_crate() {
        let mut seed = 0x1234_5678_9abc_def1u64;
        let mut data = Vec::new();
        let mut spans = Vec::new();
        let lengths: Vec<usize> = (0..=70).chain([255, 256, 488, 1000, 4097]).collect();
        for (n, &len) in lengths.iter().enumerate() {
            let flags = (n % 3) as u8 * 0x10;
            let start = data.len();
            data.extend((0..len).map(|_| pseudo(&mut seed)));
            let body = &data[start..];
            let expected = portable_record_crc(flags, body);
            assert_eq!(record_crc(flags, body), expected, "len {len}");
            spans.push(Span {
                flags,
                body_start: start,
                body_end: start + len,
                stored: expected,
            });
        }
        assert_eq!(first_mismatch(&data, &spans), None);
        for count in 0..spans.len() {
            assert_eq!(first_mismatch(&data, &spans[..count]), None);
        }

        for victim in 0..spans.len() {
            let s = spans[victim];
            if s.body_end == s.body_start {
                continue;
            }
            for bit in [0usize, 7, (s.body_end - s.body_start) * 8 - 1] {
                let mut damaged = data.clone();
                damaged[s.body_start + bit / 8] ^= 1 << (bit % 8);
                assert_eq!(
                    first_mismatch(&damaged, &spans),
                    Some(victim),
                    "len {}",
                    lengths[victim]
                );
            }
        }
        // The flags byte is covered too.
        let mut flagged = spans.clone();
        flagged[10].flags ^= 0x10;
        assert_eq!(first_mismatch(&data, &flagged), Some(10));
    }
}
