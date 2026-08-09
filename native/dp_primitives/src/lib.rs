// Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
// SPDX-License-Identifier: Apache-2.0

use std::mem::{align_of, size_of};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::slice;

use dashu_int::IBig;
use dashu_ratio::RBig;
use zeroize::Zeroize;

mod discrete_gaussian;
use discrete_gaussian::{SemanticPrfRandom, sample_discrete_gaussian};

const ABI_VERSION: u32 = 2;
const MAX_VECTOR_LENGTH: usize = 4_194_304;
const MAX_EXACT_INTEGER: u64 = 1_u64 << 53;
const KEY_LENGTH: usize = 32;
const MAX_DOMAIN_LENGTH: usize = 1024;
const MECHANISM_ID: &[u8] = b"cks20-discrete-gaussian-i64-hmac-sha256-v1\0";

const STATUS_OK: i32 = 0;
const STATUS_INVALID_ARGUMENT: i32 = 1;
const STATUS_SAMPLER_ERROR: i32 = 2;
const STATUS_INTERNAL_ERROR: i32 = 3;

fn valid_slice_pointer<T>(pointer: *const T, length: usize) -> bool {
    !pointer.is_null()
        && (pointer as usize).is_multiple_of(align_of::<T>())
        && length <= isize::MAX as usize / size_of::<T>()
}

#[allow(clippy::too_many_arguments)] // Mirrors the fixed C ABI exactly.
unsafe fn add_discrete_gaussian(
    input: *const i64,
    length: usize,
    scale: u64,
    key: *const u8,
    key_length: usize,
    domain: *const u8,
    domain_length: usize,
    output: *mut i64,
) -> Result<(), i32> {
    if length == 0
        || length > MAX_VECTOR_LENGTH
        || scale == 0
        || scale > MAX_EXACT_INTEGER
        || !valid_slice_pointer(input, length)
        || !valid_slice_pointer(output.cast_const(), length)
        || key_length != KEY_LENGTH
        || !valid_slice_pointer(key, key_length)
        || domain_length == 0
        || domain_length > MAX_DOMAIN_LENGTH
        || !valid_slice_pointer(domain, domain_length)
    {
        return Err(STATUS_INVALID_ARGUMENT);
    }

    // Copy before invoking the sampler, both to permit in-place use and to
    // keep the only unsafe reads at this validated ABI boundary.
    let values = unsafe { slice::from_raw_parts(input, length) }.to_vec();
    let mut secret = [0_u8; KEY_LENGTH];
    secret.copy_from_slice(unsafe { slice::from_raw_parts(key, key_length) });
    if secret.iter().all(|byte| *byte == 0) {
        secret.zeroize();
        return Err(STATUS_INVALID_ARGUMENT);
    }
    let domain = unsafe { slice::from_raw_parts(domain, domain_length) };
    let mut rng = SemanticPrfRandom::new(secret, domain).map_err(|_| STATUS_SAMPLER_ERROR)?;
    let scale = RBig::from(scale);
    let release = values
        .into_iter()
        .map(|value| {
            let noisy = IBig::from(value)
                + sample_discrete_gaussian(scale.clone(), &mut rng)
                    .map_err(|_| STATUS_SAMPLER_ERROR)?;
            Ok(match i64::try_from(&noisy) {
                Ok(value) => value,
                Err(_) if noisy < IBig::ZERO => i64::MIN,
                Err(_) => i64::MAX,
            })
        })
        .collect::<Result<Vec<_>, i32>>()?;
    unsafe { slice::from_raw_parts_mut(output, length) }.copy_from_slice(&release);
    Ok(())
}

#[unsafe(no_mangle)]
/// Add exact discrete-Gaussian noise through the C ABI.
///
/// # Safety
///
/// For nonzero `length`, `input` must address `length` initialized, readable
/// `i64` values and `output` must address `length` writable `i64` values. The
/// regions may overlap because all input is copied before output is written.
/// `key` must address `key_length` readable bytes and `domain` must address
/// `domain_length` readable bytes. No referenced region may be accessed
/// concurrently for the duration of the call.
pub unsafe extern "C" fn dsflower_dp_add_discrete_gaussian_i64(
    input: *const i64,
    length: usize,
    scale: u64,
    key: *const u8,
    key_length: usize,
    domain: *const u8,
    domain_length: usize,
    output: *mut i64,
) -> i32 {
    match catch_unwind(AssertUnwindSafe(|| unsafe {
        add_discrete_gaussian(
            input,
            length,
            scale,
            key,
            key_length,
            domain,
            domain_length,
            output,
        )
    })) {
        Ok(Ok(())) => STATUS_OK,
        Ok(Err(status)) => status,
        Err(_) => STATUS_INTERNAL_ERROR,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn dsflower_dp_primitives_abi_version() -> u32 {
    ABI_VERSION
}

#[unsafe(no_mangle)]
pub extern "C" fn dsflower_dp_primitives_mechanism_id() -> *const std::ffi::c_char {
    MECHANISM_ID.as_ptr().cast()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_constants_are_stable() {
        assert_eq!(dsflower_dp_primitives_abi_version(), 2);
        let mechanism = unsafe { std::ffi::CStr::from_ptr(dsflower_dp_primitives_mechanism_id()) };
        assert_eq!(
            mechanism.to_bytes(),
            b"cks20-discrete-gaussian-i64-hmac-sha256-v1"
        );
    }

    #[test]
    fn rejects_invalid_public_geometry() {
        let input = [0_i64];
        let mut output = [0_i64];
        let key = [7_u8; KEY_LENGTH];
        let domain = b"test/domain";
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    0,
                    1,
                    key.as_ptr(),
                    key.len(),
                    domain.as_ptr(),
                    domain.len(),
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    1,
                    std::ptr::null(),
                    key.len(),
                    domain.as_ptr(),
                    domain.len(),
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    0,
                    key.as_ptr(),
                    key.len(),
                    domain.as_ptr(),
                    domain.len(),
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    1,
                    key.as_ptr(),
                    key.len() + 1,
                    domain.as_ptr(),
                    domain.len(),
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    MAX_EXACT_INTEGER + 1,
                    key.as_ptr(),
                    key.len(),
                    domain.as_ptr(),
                    domain.len(),
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    1,
                    key.as_ptr(),
                    key.len(),
                    std::ptr::null(),
                    domain.len(),
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        let oversized_domain = [1_u8; MAX_DOMAIN_LENGTH + 1];
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    1,
                    key.as_ptr(),
                    key.len(),
                    oversized_domain.as_ptr(),
                    oversized_domain.len(),
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    std::ptr::null(),
                    1,
                    1,
                    key.as_ptr(),
                    key.len(),
                    domain.as_ptr(),
                    domain.len(),
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    1,
                    key.as_ptr(),
                    key.len(),
                    domain.as_ptr(),
                    domain.len(),
                    std::ptr::null_mut(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        let zero_key = [0_u8; KEY_LENGTH];
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    1,
                    zero_key.as_ptr(),
                    zero_key.len(),
                    domain.as_ptr(),
                    domain.len(),
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    1,
                    key.as_ptr(),
                    key.len() - 1,
                    domain.as_ptr(),
                    domain.len(),
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    1,
                    key.as_ptr(),
                    key.len(),
                    domain.as_ptr(),
                    0,
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
    }

    #[test]
    fn semantic_stream_is_sticky_and_domain_separated() {
        let source = vec![0_i64; 64];
        let key = [11_u8; KEY_LENGTH];
        let first_domain = b"tree/1/level/0";
        let second_domain = b"tree/1/level/1";
        let mut first = vec![0_i64; source.len()];
        let mut replay = vec![0_i64; source.len()];
        let mut second = vec![0_i64; source.len()];
        for (domain, output) in [
            (first_domain.as_slice(), &mut first),
            (first_domain.as_slice(), &mut replay),
            (second_domain.as_slice(), &mut second),
        ] {
            assert_eq!(
                unsafe {
                    dsflower_dp_add_discrete_gaussian_i64(
                        source.as_ptr(),
                        source.len(),
                        8,
                        key.as_ptr(),
                        key.len(),
                        domain.as_ptr(),
                        domain.len(),
                        output.as_mut_ptr(),
                    )
                },
                STATUS_OK
            );
        }
        assert_eq!(first, replay);
        assert_ne!(first, second);

        let mut values = source;
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    values.as_ptr(),
                    values.len(),
                    8,
                    key.as_ptr(),
                    key.len(),
                    first_domain.as_ptr(),
                    first_domain.len(),
                    values.as_mut_ptr(),
                )
            },
            STATUS_OK
        );
        assert_eq!(values, first);
    }
}
