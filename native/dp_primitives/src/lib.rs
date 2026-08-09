// Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
// SPDX-License-Identifier: Apache-2.0

use std::mem::{align_of, size_of};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::slice;

use dashu_int::IBig;
use dashu_ratio::RBig;

mod discrete_gaussian;
use discrete_gaussian::{BufferedSystemRandom, sample_discrete_gaussian};

const ABI_VERSION: u32 = 1;
const MAX_VECTOR_LENGTH: usize = 4_194_304;
const MAX_EXACT_INTEGER: u64 = 1_u64 << 53;
const MECHANISM_ID: &[u8] = b"cks20-discrete-gaussian-i64-system-random-v1\0";

const STATUS_OK: i32 = 0;
const STATUS_INVALID_ARGUMENT: i32 = 1;
const STATUS_SAMPLER_ERROR: i32 = 2;
const STATUS_INTERNAL_ERROR: i32 = 3;

fn valid_slice_pointer<T>(pointer: *const T, length: usize) -> bool {
    !pointer.is_null()
        && (pointer as usize).is_multiple_of(align_of::<T>())
        && length <= isize::MAX as usize / size_of::<T>()
}

unsafe fn add_discrete_gaussian(
    input: *const i64,
    length: usize,
    scale: u64,
    output: *mut i64,
) -> Result<(), i32> {
    if length == 0
        || length > MAX_VECTOR_LENGTH
        || scale == 0
        || scale > MAX_EXACT_INTEGER
        || !valid_slice_pointer(input, length)
        || !valid_slice_pointer(output.cast_const(), length)
    {
        return Err(STATUS_INVALID_ARGUMENT);
    }

    // Copy before invoking the sampler, both to permit in-place use and to
    // keep the only unsafe reads at this validated ABI boundary.
    let values = unsafe { slice::from_raw_parts(input, length) }.to_vec();
    let mut rng = BufferedSystemRandom::new().map_err(|_| STATUS_SAMPLER_ERROR)?;
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
/// Neither region may be accessed concurrently for the duration of the call.
pub unsafe extern "C" fn dsflower_dp_add_discrete_gaussian_i64(
    input: *const i64,
    length: usize,
    scale: u64,
    output: *mut i64,
) -> i32 {
    match catch_unwind(AssertUnwindSafe(|| unsafe {
        add_discrete_gaussian(input, length, scale, output)
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
        assert_eq!(dsflower_dp_primitives_abi_version(), 1);
        let mechanism = unsafe { std::ffi::CStr::from_ptr(dsflower_dp_primitives_mechanism_id()) };
        assert_eq!(
            mechanism.to_bytes(),
            b"cks20-discrete-gaussian-i64-system-random-v1"
        );
    }

    #[test]
    fn rejects_invalid_public_geometry() {
        let input = [0_i64];
        let mut output = [0_i64];
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(input.as_ptr(), 0, 1, output.as_mut_ptr())
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(input.as_ptr(), 1, 0, output.as_mut_ptr())
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    input.as_ptr(),
                    1,
                    MAX_EXACT_INTEGER + 1,
                    output.as_mut_ptr(),
                )
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(std::ptr::null(), 1, 1, output.as_mut_ptr())
            },
            STATUS_INVALID_ARGUMENT
        );
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(input.as_ptr(), 1, 1, std::ptr::null_mut())
            },
            STATUS_INVALID_ARGUMENT
        );
    }

    #[test]
    fn samples_vectors_and_supports_aliasing() {
        let mut values = vec![0_i64; 64];
        assert_eq!(
            unsafe {
                dsflower_dp_add_discrete_gaussian_i64(
                    values.as_ptr(),
                    values.len(),
                    8,
                    values.as_mut_ptr(),
                )
            },
            STATUS_OK
        );
        assert_eq!(values.len(), 64);
    }
}
