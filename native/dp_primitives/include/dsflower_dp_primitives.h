/*
 * Copyright 2026 Barcelona Institute for Global Health (ISGlobal)
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef DSFLOWER_DP_PRIMITIVES_H_
#define DSFLOWER_DP_PRIMITIVES_H_

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#  ifdef DSFLOWER_DP_PRIMITIVES_BUILD
#    define DSFLOWER_DP_API __declspec(dllexport)
#  else
#    define DSFLOWER_DP_API __declspec(dllimport)
#  endif
#else
#  define DSFLOWER_DP_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define DSFLOWER_DP_PRIMITIVES_ABI_VERSION 1U
#define DSFLOWER_DP_MAX_VECTOR_LENGTH 4194304U
#define DSFLOWER_DP_MAX_EXACT_INTEGER 9007199254740992ULL
#define DSFLOWER_DP_MECHANISM_ID \
  "cks20-discrete-gaussian-i64-system-random-v1"

enum DsFlowerDpStatus {
  DSFLOWER_DP_OK = 0,
  DSFLOWER_DP_INVALID_ARGUMENT = 1,
  DSFLOWER_DP_SAMPLER_ERROR = 2,
  DSFLOWER_DP_INTERNAL_ERROR = 3
};

/*
 * Add independent samples from an exact discrete Gaussian with integer scale
 * to an i64 vector. The final arbitrary-precision sums are saturated to i64,
 * which is deterministic post-processing of the DP release.
 *
 * `scale` must be in [1, 2^53] and remains an exact integer throughout the
 * arbitrary-precision sampler. Input and output may alias: input is copied
 * before output is written. No seed or caller-controlled RNG crosses this ABI.
 * Exactness is conditional on IID uniform bits; the production implementation
 * relies on the operating-system RNG to realize that source.
 *
 * For nonzero `length`, `input` must address `length` initialized readable
 * int64_t values and `output` must address `length` writable int64_t values.
 * The regions may overlap, but neither may be accessed concurrently during the
 * call. This trusted native boundary is not exposed to submitted app code.
 */
DSFLOWER_DP_API int32_t dsflower_dp_add_discrete_gaussian_i64(
    const int64_t* input, size_t length, uint64_t scale, int64_t* output);

DSFLOWER_DP_API uint32_t dsflower_dp_primitives_abi_version(void);
DSFLOWER_DP_API const char* dsflower_dp_primitives_mechanism_id(void);

#ifdef __cplusplus
}
#endif

#endif  /* DSFLOWER_DP_PRIMITIVES_H_ */
