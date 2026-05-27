#![allow(dead_code)]

pub const SELECTORS: [[u8; 4]; 8] = [
    [0x38, 0xed, 0x17, 0x39],
    [0x7f, 0xf3, 0x6a, 0xb5],
    [0x18, 0xcb, 0xaf, 0xe5],
    [0x5c, 0x11, 0xd7, 0x95],
    [0xb6, 0xf9, 0xde, 0x95],
    [0x79, 0x1a, 0xc9, 0x47],
    [0x41, 0x4b, 0xf3, 0x89],
    [0xc0, 0x4b, 0x8d, 0x59],
];

pub fn find_selector(data: &[u8]) -> Option<usize> {
    if data.len() < 4 {
        return None;
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        return find_selector_simd(data);
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    find_selector_scalar(data)
}

pub fn selector_backend() -> &'static str {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        "avx2"
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        "scalar"
    }
}

pub fn find_selector_scalar(data: &[u8]) -> Option<usize> {
    let selector = [data[0], data[1], data[2], data[3]];
    SELECTORS
        .iter()
        .position(|candidate| *candidate == selector)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn find_selector_simd(data: &[u8]) -> Option<usize> {
    use std::arch::x86_64::*;

    if data.len() < 4 {
        return None;
    }

    let target = _mm_cvtsi32_si128(i32::from_le_bytes([data[0], data[1], data[2], data[3]]));
    for (idx, selector) in SELECTORS.iter().enumerate() {
        let candidate = _mm_cvtsi32_si128(i32::from_le_bytes(*selector));
        let cmp = _mm_cmpeq_epi8(target, candidate);
        if (_mm_movemask_epi8(cmp) & 0x0f) == 0x0f {
            return Some(idx);
        }
    }
    None
}
