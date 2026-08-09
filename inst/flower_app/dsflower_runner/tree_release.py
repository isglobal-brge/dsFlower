"""Common sticky Gaussian release primitive for trusted tree adapters."""

import hashlib
import json
import math
import platform
import sys

import numpy as np

from . import dp_harness, seeding


_MAX_COORDINATES = 8_000_000
_RELEASE_DOMAIN = "tree-joint-gaussian/v1"
_NUMERIC_CONTRACT = "dsflower-tree-gaussian-numeric-v1"


def _canonical_vector(value):
    array = np.asarray(value)
    if array.dtype.hasobject or array.dtype.kind not in "iuf" or \
            array.size < 1 or array.size > _MAX_COORDINATES:
        raise ValueError("tree sufficient vector has an unsupported shape")
    canonical = np.ascontiguousarray(array, dtype="<f8")
    if not bool(np.all(np.isfinite(canonical))):
        raise ValueError("tree sufficient vector must be finite")
    if bool(np.any(canonical == 0.0)):
        canonical = canonical.copy()
        canonical[canonical == 0.0] = 0.0
    return canonical


def _policy_hash(value):
    encoded = json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def numeric_execution_profile():
    """Public facts that domain-separate non-identical numeric runtimes."""
    return {
        "byteorder": sys.byteorder,
        "contract": _NUMERIC_CONTRACT,
        "machine": platform.machine().lower(),
        "numpy": np.__version__,
        "rng": "chacha20-box-muller-four-sample/v2",
        "system": platform.system().lower(),
    }


def joint_gaussian_release(
        value, *, mechanism, layout, epsilon, delta, sensitivity,
        num_releases, execution_fingerprint):
    """Release one fixed-layout sufficient vector with semantic sticky noise.

    ``num_releases`` is the fixed transcript count accounted by the caller's
    profile.  Adaptive multi-stage tree mechanisms put a canonical
    ``release_index`` in ``layout`` and call this function exactly that many
    times.  This primitive owns canonicalization, RDP sigma calibration and PRF
    identity; adapters own sensitivity proofs and transcript geometry.
    """
    if not isinstance(mechanism, str) or not mechanism or \
            not isinstance(layout, dict) or \
            not isinstance(execution_fingerprint, str) or \
            not execution_fingerprint:
        raise ValueError("tree release semantics are invalid")
    if type(num_releases) is not int or not 1 <= num_releases <= 1_000_000:
        raise ValueError("tree release count is invalid")
    try:
        sensitivity = float(sensitivity)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("tree release sensitivity is invalid") from exc
    if not math.isfinite(sensitivity) or sensitivity <= 0.0:
        raise ValueError("tree release sensitivity is invalid")
    canonical = _canonical_vector(value)
    sigma = dp_harness.compute_output_sigma(
        epsilon, delta, sensitivity, num_releases=num_releases)
    semantics = {
        "layout": layout,
        "mechanism": mechanism,
        "num_releases": num_releases,
        "sensitivity": sensitivity,
        "sigma": sigma,
    }
    # Raw policy inputs calibrate sigma but are not independent reroll axes.
    # If two policies produce the exact same effective sigma, their mechanism
    # pins and sufficient vector intentionally derive the same noise stream.
    privacy = dict(semantics)
    privacy["policy_hash"] = _policy_hash(privacy)
    execution = {
        "adapter": execution_fingerprint,
        "numeric": numeric_execution_profile(),
    }
    master = bytearray(seeding.master_seed(
        mechanism, {"layout": layout}, privacy, 1,
        private_arrays=(canonical,),
        execution_fingerprint=execution))
    subkey = None
    try:
        subkey = bytearray(seeding.sub_seed(master, _RELEASE_DOMAIN))
        rng = seeding.np_rng(subkey)
        subkey[:] = b"\x00" * len(subkey)
        noise = np.asarray(
            rng.normal(0.0, sigma, size=canonical.shape), dtype=np.float64)
    finally:
        if isinstance(subkey, bytearray):
            subkey[:] = b"\x00" * len(subkey)
        master[:] = b"\x00" * len(master)
    released = canonical + noise
    if released.shape != canonical.shape or not bool(np.all(np.isfinite(released))):
        raise RuntimeError("tree private release is non-finite")
    return released, float(sigma)


__all__ = ["joint_gaussian_release", "numeric_execution_profile"]
