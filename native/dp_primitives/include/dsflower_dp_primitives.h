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

#define DSFLOWER_DP_PRIMITIVES_ABI_VERSION 2U
#define DSFLOWER_DP_MAX_VECTOR_LENGTH 4194304U
#define DSFLOWER_DP_MAX_EXACT_INTEGER 9007199254740992ULL
#define DSFLOWER_DP_KEY_LENGTH 32U
#define DSFLOWER_DP_MAX_DOMAIN_LENGTH 1024U
#define DSFLOWER_DP_MECHANISM_ID \
  "cks20-discrete-gaussian-i64-hmac-sha256-v1"

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
 * before output is written. `key` must be exactly 32 nonzero bytes derived by
 * the trusted server from its custodial root and the complete semantic training
 * identity. It is never the durable root or an analyst-provided seed. `domain`
 * must contain 1..1024 canonical bytes unique to this mechanism coordinate.
 * Equal key/domain pairs reproduce the same stream; distinct coordinates must
 * use distinct domains. HMAC-SHA256 realizes the pseudorandom bit source. The
 * trusted caller remains responsible for erasing its own derived-key buffer
 * after this function returns.
 *
 * For nonzero `length`, `input` must address `length` initialized readable
 * int64_t values and `output` must address `length` writable int64_t values.
 * `key` and `domain` must address their declared readable byte lengths. The
 * input/output regions may overlap, but no referenced region may be accessed
 * concurrently during the call. This trusted native boundary is not exposed
 * to submitted app code.
 */
DSFLOWER_DP_API int32_t dsflower_dp_add_discrete_gaussian_i64(
    const int64_t* input, size_t length, uint64_t scale,
    const uint8_t* key, size_t key_length,
    const uint8_t* domain, size_t domain_length, int64_t* output);

DSFLOWER_DP_API uint32_t dsflower_dp_primitives_abi_version(void);
DSFLOWER_DP_API const char* dsflower_dp_primitives_mechanism_id(void);

#ifdef __cplusplus
}
#endif

#endif  /* DSFLOWER_DP_PRIMITIVES_H_ */
