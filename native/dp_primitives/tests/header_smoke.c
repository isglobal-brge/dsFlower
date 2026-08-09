/* Copyright 2026 Barcelona Institute for Global Health (ISGlobal) */
#include "dsflower_dp_primitives.h"

static int check_signature(void) {
  int64_t value = 0;
  const uint8_t key[DSFLOWER_DP_KEY_LENGTH] = {1U};
  const uint8_t domain[] = "header-smoke";
  return dsflower_dp_add_discrete_gaussian_i64(
      &value, 1U, 1U, key, sizeof(key), domain, sizeof(domain) - 1U, &value);
}

int main(void) {
  return DSFLOWER_DP_PRIMITIVES_ABI_VERSION == 2U &&
                 DSFLOWER_DP_MAX_VECTOR_LENGTH == 4194304U
             ? check_signature()
             : DSFLOWER_DP_INTERNAL_ERROR;
}
