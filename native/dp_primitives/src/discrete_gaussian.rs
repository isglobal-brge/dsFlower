// Copyright (c) 2022 President and Fellows of Harvard College
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
//   Copyright 2020 Thomas Steinke
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

//! Exact CKS20 discrete-Gaussian sampler.
//!
//! This is a minimal port of OpenDP 0.15.1 commit c34d3d04a8872a51af523d9a2244be6171173b7d.
//! The arithmetic and rejection rules are unchanged.  The only mechanism-level
//! change is replacing vendored OpenSSL with a domain-separated HMAC-SHA256
//! stream keyed by dsFlower's canonical semantic training identity.

use dashu_base::{Abs, BitTest, Sign};
use dashu_int::{IBig, UBig};
use dashu_ratio::RBig;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use zeroize::Zeroize;

const PRF_BLOCK_LENGTH: usize = 32;
const PRF_DOMAIN: &[u8] = b"dsflower/dp-primitives/prf/v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SamplerError {
    InvalidState,
}

trait RandomBytes {
    fn fill_bytes(&mut self, buffer: &mut [u8]) -> Result<(), SamplerError>;
}

pub(crate) struct SemanticPrfRandom {
    base: Hmac<Sha256>,
    block: [u8; PRF_BLOCK_LENGTH],
    cursor: usize,
    counter: u64,
}

impl SemanticPrfRandom {
    pub(crate) fn new(mut key: [u8; 32], domain: &[u8]) -> Result<Self, SamplerError> {
        let mut base =
            Hmac::<Sha256>::new_from_slice(&key).map_err(|_| SamplerError::InvalidState)?;
        key.zeroize();
        base.update(PRF_DOMAIN);
        base.update(&(domain.len() as u64).to_be_bytes());
        base.update(domain);
        Ok(Self {
            base,
            block: [0_u8; PRF_BLOCK_LENGTH],
            cursor: PRF_BLOCK_LENGTH,
            counter: 0,
        })
    }

    fn refill(&mut self) -> Result<(), SamplerError> {
        let mut mac = self.base.clone();
        mac.update(&self.counter.to_be_bytes());
        self.block.copy_from_slice(&mac.finalize().into_bytes());
        self.counter = self
            .counter
            .checked_add(1)
            .ok_or(SamplerError::InvalidState)?;
        self.cursor = 0;
        Ok(())
    }
}

impl RandomBytes for SemanticPrfRandom {
    fn fill_bytes(&mut self, buffer: &mut [u8]) -> Result<(), SamplerError> {
        let mut written = 0;
        while written < buffer.len() {
            if self.cursor == self.block.len() {
                self.refill()?;
            }
            let count = (buffer.len() - written).min(self.block.len() - self.cursor);
            buffer[written..written + count]
                .copy_from_slice(&self.block[self.cursor..self.cursor + count]);
            written += count;
            self.cursor += count;
        }
        Ok(())
    }
}

impl Drop for SemanticPrfRandom {
    fn drop(&mut self) {
        self.block.zeroize();
        self.cursor = 0;
        self.counter = 0;
    }
}

fn sample_standard_bernoulli(rng: &mut impl RandomBytes) -> Result<bool, SamplerError> {
    let mut buffer = [0_u8; 1];
    rng.fill_bytes(&mut buffer)?;
    Ok(buffer[0] & 1 == 1)
}

fn sample_uniform_ubig_below(
    upper: UBig,
    rng: &mut impl RandomBytes,
) -> Result<UBig, SamplerError> {
    if upper.is_zero() {
        return Err(SamplerError::InvalidState);
    }

    let byte_len = upper.bit_len().div_ceil(8);
    let max = UBig::from_be_bytes(&vec![u8::MAX; byte_len]);
    let threshold = &max - &max % &upper;
    let mut buffer = vec![0_u8; byte_len];

    loop {
        rng.fill_bytes(&mut buffer)?;
        let sample = UBig::from_be_bytes(&buffer);
        if sample < threshold {
            return Ok(sample % &upper);
        }
    }
}

fn sample_bernoulli_rational(
    probability: RBig,
    rng: &mut impl RandomBytes,
) -> Result<bool, SamplerError> {
    let (numerator, denominator) = probability.into_parts();
    let (Sign::Positive, numerator) = numerator.into_parts() else {
        return Err(SamplerError::InvalidState);
    };
    if numerator > denominator {
        return Err(SamplerError::InvalidState);
    }
    sample_uniform_ubig_below(denominator, rng).map(|sample| numerator > sample)
}

fn gcd_ubig(mut left: UBig, mut right: UBig) -> UBig {
    while !right.is_zero() {
        let remainder = &left % &right;
        left = right;
        right = remainder;
    }
    left
}

fn div_rbig_by_ubig_exact(numerator: &UBig, denominator: &UBig, divisor: &UBig) -> RBig {
    assert!(!divisor.is_zero(), "division by zero");
    if numerator.is_zero() {
        return RBig::ZERO;
    }

    // Preserve the OpenDP workaround for dashu#57.  Replacing this expression
    // with ordinary RBig division changes the exact sampler.
    let gcd = gcd_ubig(numerator.clone(), divisor.clone());
    let reduced_numerator = numerator / &gcd;
    let reduced_divisor = divisor / gcd;
    RBig::from_parts(reduced_numerator.into(), denominator * reduced_divisor)
}

fn sample_bernoulli_exp1(
    probability_exponent: RBig,
    rng: &mut impl RandomBytes,
) -> Result<bool, SamplerError> {
    let (numerator, denominator) = probability_exponent.into_parts();
    let (Sign::Positive, numerator) = numerator.into_parts() else {
        return Err(SamplerError::InvalidState);
    };
    if numerator > denominator {
        return Err(SamplerError::InvalidState);
    }

    let mut k = UBig::ONE;
    loop {
        let exponent = div_rbig_by_ubig_exact(&numerator, &denominator, &k);
        if sample_bernoulli_rational(exponent, rng)? {
            k += UBig::ONE;
        } else {
            return Ok(k % 2_u8 == 1);
        }
    }
}

fn sample_bernoulli_exp(
    mut probability_exponent: RBig,
    rng: &mut impl RandomBytes,
) -> Result<bool, SamplerError> {
    if probability_exponent < RBig::ZERO {
        return Err(SamplerError::InvalidState);
    }
    while probability_exponent > RBig::ONE {
        if sample_bernoulli_exp1(RBig::ONE, rng)? {
            probability_exponent -= RBig::ONE;
        } else {
            return Ok(false);
        }
    }
    sample_bernoulli_exp1(probability_exponent, rng)
}

fn sample_geometric_exp_slow(
    probability_exponent: RBig,
    rng: &mut impl RandomBytes,
) -> Result<UBig, SamplerError> {
    let mut count = UBig::ZERO;
    loop {
        if sample_bernoulli_exp(probability_exponent.clone(), rng)? {
            count += UBig::ONE;
        } else {
            return Ok(count);
        }
    }
}

fn sample_geometric_exp_fast(
    probability_exponent: RBig,
    rng: &mut impl RandomBytes,
) -> Result<UBig, SamplerError> {
    if probability_exponent.is_zero() {
        return Ok(UBig::ZERO);
    }
    if probability_exponent < RBig::ZERO {
        return Err(SamplerError::InvalidState);
    }

    let (numerator, denominator) = probability_exponent.into_parts();
    let (Sign::Positive, numerator) = numerator.into_parts() else {
        return Err(SamplerError::InvalidState);
    };
    let mut uniform = sample_uniform_ubig_below(denominator.clone(), rng)?;
    while !sample_bernoulli_exp(
        RBig::from_parts(uniform.as_ibig().clone(), denominator.clone()),
        rng,
    )? {
        uniform = sample_uniform_ubig_below(denominator.clone(), rng)?;
    }
    let geometric = sample_geometric_exp_slow(RBig::ONE, rng)?;
    Ok((geometric * denominator + uniform) / numerator)
}

fn sample_discrete_laplace(scale: RBig, rng: &mut impl RandomBytes) -> Result<IBig, SamplerError> {
    if scale.is_zero() {
        return Ok(IBig::ZERO);
    }
    if scale < RBig::ZERO {
        return Err(SamplerError::InvalidState);
    }

    let (numerator, denominator) = scale.into_parts();
    let (Sign::Positive, numerator) = numerator.into_parts() else {
        return Err(SamplerError::InvalidState);
    };
    let inverse_scale = RBig::from_parts(denominator.as_ibig().clone(), numerator);

    loop {
        let positive = sample_standard_bernoulli(rng)?;
        let magnitude = sample_geometric_exp_fast(inverse_scale.clone(), rng)?
            .as_ibig()
            .clone();
        if positive || !magnitude.is_zero() {
            return Ok(if positive { magnitude } else { -magnitude });
        }
    }
}

pub(crate) fn sample_discrete_gaussian(
    scale: RBig,
    rng: &mut SemanticPrfRandom,
) -> Result<IBig, SamplerError> {
    sample_discrete_gaussian_with_rng(scale, rng)
}

fn sample_discrete_gaussian_with_rng(
    scale: RBig,
    rng: &mut impl RandomBytes,
) -> Result<IBig, SamplerError> {
    if scale.is_zero() {
        return Ok(IBig::ZERO);
    }
    if scale < RBig::ZERO {
        return Err(SamplerError::InvalidState);
    }

    let proposal_scale = RBig::from(scale.clone().floor() + 1_i8);
    let variance = scale.pow(2);
    loop {
        let candidate = sample_discrete_laplace(proposal_scale.clone(), rng)?;
        let centered_magnitude = (&candidate).abs() - variance.clone() / &proposal_scale;
        let bias = centered_magnitude.pow(2) / (variance.clone() * RBig::from(2_u8));
        if sample_bernoulli_exp(bias, rng)? {
            return Ok(candidate);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedEntropy(u64);

    impl FixedEntropy {
        fn new() -> Self {
            Self(0x4242_4242_4242_4242)
        }
    }

    impl RandomBytes for FixedEntropy {
        fn fill_bytes(&mut self, buffer: &mut [u8]) -> Result<(), SamplerError> {
            for byte in buffer {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                *byte = self.0 as u8;
            }
            Ok(())
        }
    }

    #[test]
    fn semantic_prf_has_a_stable_known_answer() {
        let key = core::array::from_fn(|index| index as u8);
        let domain = b"xgb/tree/00000001/level/00000002";
        let mut rng = SemanticPrfRandom::new(key, domain).expect("semantic PRF");
        let mut bytes = [0_u8; 64];
        rng.fill_bytes(&mut bytes).expect("semantic PRF stream");
        assert_eq!(
            bytes,
            [
                0x22, 0x6a, 0x8c, 0x56, 0x2f, 0xb1, 0x4c, 0xe5, 0x71, 0x6f, 0x91, 0x1b, 0x97, 0xc9,
                0xc1, 0x08, 0xcb, 0x8a, 0x10, 0x17, 0x15, 0x1b, 0x98, 0x09, 0x6e, 0x29, 0xcc, 0x34,
                0x34, 0x0d, 0xf6, 0xda, 0xe8, 0x6c, 0xb6, 0x57, 0xa8, 0x0d, 0x81, 0x2a, 0xc7, 0x82,
                0xa7, 0x76, 0xe9, 0x46, 0x6d, 0x6d, 0xbc, 0xaa, 0xdd, 0x39, 0xc8, 0x84, 0x6c, 0xb8,
                0x44, 0x97, 0x7f, 0x4c, 0xec, 0x8b, 0xfe, 0xdb,
            ]
        );
        assert_eq!(rng.cursor, PRF_BLOCK_LENGTH);
    }

    #[test]
    fn exact_sampler_has_a_stable_known_answer() {
        let mut rng = FixedEntropy::new();
        let samples = (0..16)
            .map(|_| sample_discrete_gaussian_with_rng(8.into(), &mut rng))
            .collect::<Result<Vec<_>, _>>()
            .expect("fixed entropy sampler");
        let samples = samples
            .iter()
            .map(|value| i64::try_from(value).expect("small known-answer sample"))
            .collect::<Vec<_>>();
        assert_eq!(
            samples,
            [1, 11, 3, -2, 12, 9, 7, 9, 6, 7, 6, 1, -8, 3, -14, -12]
        );
    }
}
