"""Public synthetic tree KAT inputs shared by the test and explicit generator."""
import hashlib
import json
import math
from unittest import mock

import numpy as np

from dsflower_runner import canonical_units, seeding, tree_release
from test_forest_adapter import _manifest


CONTRACT = "dsflower-tree-release-kat-v3"
EXECUTION = "tree-release-kat-adapter-v1"


def request(execution=EXECUTION, epsilon=1.0, delta=1.0e-6):
    manifest = _manifest(trees=2, depth=1)
    manifest["privacy"].update(epsilon=epsilon, delta=delta)
    return tree_release.native_request_identity(
        manifest, execution_fingerprint=execution)


def release(value, layout=None, releases=1, epsilon=1.0, delta=1.0e-6,
            sensitivity=math.sqrt(2.0), mechanism="test-tree-gaussian/v1",
            execution="test-tree-release-v1"):
    # This fixed, public test key is the only mocked input. Runtime facts,
    # sigma calibration, binding, PRF and numerical execution are production.
    with mock.patch.object(seeding, "_node_secret", return_value=bytes(range(32))):
        raw = tree_release._canonical_vector(value)
        identity = request(execution, epsilon, delta)
        units = canonical_units.canonicalize_arrays(
            raw.reshape(-1, 1), np.zeros(raw.size))
        binding = seeding.bind_private_data(identity, units, effective_tensors=(raw,))
        return tree_release.joint_gaussian_release(
            value, mechanism=mechanism,
            layout=({"cells": 4, "release_index": 0} if layout is None else layout),
            epsilon=epsilon, delta=delta, sensitivity=sensitivity,
            num_releases=releases, execution_fingerprint=execution,
            request_identity=identity, data_binding=binding)


def environment_key(runtime, numeric_profile):
    """Select observed environments without hiding stale runner source hashes."""
    return json.dumps({"runtime": {key: value for key, value in runtime.items()
                                   if key != "runner_sha256"},
                       "numeric_profile": numeric_profile},
                      sort_keys=True, separators=(",", ":"), allow_nan=False)


def capture_record():
    identity = request()
    runtime = json.loads(identity.canonical_json)["runtime"]
    numeric = tree_release.numeric_execution_profile()
    released, sigma = release(
        np.asarray([0.0, 1.0, 2.0, 3.0]),
        mechanism="tree-release-kat/v1",
        layout={"coordinates": 4, "release_index": 0}, execution=EXECUTION)
    return {"runtime": runtime, "numeric_profile": numeric,
            "sigma_hex": sigma.hex(),
            "vector_sha256": hashlib.sha256(
                np.ascontiguousarray(released, dtype="<f8").tobytes()).hexdigest()}
