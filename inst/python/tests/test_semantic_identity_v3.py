"""Public request / private binding separation and nuisance invariance for v3."""
import base64
import copy
import json
import os
import sys
from unittest import mock

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "flower_app"))
from dsflower_runner import canonical_units, seeding

SECRET = b"v" * 32
MANIFEST = {"feature_columns": ["x", "z"], "target_column": "y", "dp-unit": "row",
            "data_type": "tabular", "task-type": "classification", "num-features": 2,
            "num-classes": 2, "loss-name": "bce_logits", "num-server-rounds": 2,
            "model-spec-b64": base64.b64encode(json.dumps({"layers": [{"op": "linear", "out": 1}]}).encode()).decode()}
POLICY = {"epsilon": 1.0, "delta": 1e-6, "clipping_norm": 1.0, "adjacency": "replace_one"}
ARRAYS = [np.array([[0.3, -0.2]], dtype=np.float32), np.zeros(1, dtype=np.float32)]
X = np.array([[1., 2.], [3., 4.], [1., 2.]])
Y = np.array([0., 1., 0.])


def request(manifest=None, policy=None, arrays=None, **config):
    return seeding.request_identity("neural-dpsgd/v3", config, policy or POLICY,
        public_arrays=ARRAYS if arrays is None else arrays,
        manifest=MANIFEST if manifest is None else manifest)


def bound(req, x=X, y=Y, **kwargs):
    units = canonical_units.canonicalize_arrays(x, y, secret=SECRET)
    order = units.row_permutation
    return seeding.bind_private_data(req, units, effective_tensors=(x[order], y[order]), **kwargs)


def key(req, binding=None):
    with mock.patch.object(seeding, "_node_secret", return_value=SECRET):
        return seeding.release_key(req, binding or bound(req))


def test_request_is_public_and_geometry_only_changes_binding():
    with mock.patch.object(seeding, "_node_secret", side_effect=AssertionError("private key accessed")):
        req = request()
    a = bound(req, geometry={"n_privacy_units": 3, "noise_multiplier": 2.})
    b = bound(req, geometry={"n_privacy_units": 4, "noise_multiplier": 3.})
    assert a.digest != b.digest
    assert key(req, a) != key(req, b)
    assert "n_privacy_units" not in req.canonical_json.decode()
    assert "noise_multiplier" not in req.canonical_json.decode()


@pytest.mark.parametrize("field,value", [
    ("request-source", {"data_symbol": "renamed"}), ("dataset_id", "renamed"),
    ("source_kind", "parquet"), ("data_file", "/other/path"), ("run_token", "retry"),
    ("session", "another-session"), ("timestamp", "tomorrow"),
    ("strategy", "fedadam"), ("strategy-eta", 0.4), ("cv-job-sha256", "f" * 64),
    ("cv-n-nodes", 99), ("privacy-policy-sha256", "a" * 64),
])
def test_nuisance_fields_do_not_change_request(field, value):
    changed = dict(MANIFEST, **{field: value})
    if field == "strategy-eta":
        changed["strategy"] = "fedadam"
    assert request().digest == request(changed).digest


@pytest.mark.parametrize("field,value", [("epsilon", 2.), ("delta", np.nextafter(1e-6, 1.)), ("clipping_norm", 2.)])
def test_raw_policy_parameters_remain_semantic_even_when_sigma_rounds_equal(field, value):
    assert key(request()) != key(request(policy=dict(POLICY, **{field: float(value)})))


def test_source_and_initial_model_content_cannot_be_omitted():
    req = request()
    with pytest.raises(ValueError, match="complete canonical source"):
        seeding.bind_private_data(req, None, effective_tensors=(X, Y))
    value = json.loads(req.canonical_json)
    value.pop("contract")
    value["initialisation"]["initial_model_sha256"] = None
    with pytest.raises(ValueError, match="initial model"):
        seeding.build_request_identity(**value)
    arrays = [a.copy() for a in ARRAYS]
    arrays[0][0, 0] += 0.1
    assert key(req) != key(request(arrays=arrays))


def test_permutation_and_duplicate_multiset_binding():
    req = request()
    order = [2, 0, 1]
    assert key(req) == key(req, bound(req, X[order], Y[order]))
    assert key(req) != key(req, bound(req, X[:2], Y[:2]))


def test_equal_sufficient_statistics_do_not_equate_sources():
    req = request()
    units_a = canonical_units.canonicalize_arrays(X, Y, secret=SECRET)
    units_b = canonical_units.canonicalize_arrays(X + 1., Y, secret=SECRET)
    statistic = np.array([1., 2.])
    a = seeding.bind_private_data(req, units_a, effective_tensors=(statistic,))
    b = seeding.bind_private_data(req, units_b, effective_tensors=(statistic,))
    assert key(req, a) != key(req, b)


@pytest.mark.parametrize("object_name", ["coordinate", "selection", "model", "initialisation", "public_arrays", "privacy", "strategy", "runtime"])
def test_closed_public_objects_reject_unknown_fields(object_name):
    value = json.loads(request().canonical_json)
    value.pop("contract")
    value[object_name]["unexpected"] = 1
    with pytest.raises(ValueError):
        seeding.build_request_identity(**value)


def test_duplicate_nonfinite_json_rejected():
    for text in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}'):
        with pytest.raises(ValueError):
            seeding.decode_json(text)


def test_cross_request_binding_cannot_be_reused():
    req = request()
    with pytest.raises(ValueError, match="different public request"):
        key(request(policy=dict(POLICY, epsilon=2.)), bound(req))


def test_local_strategy_zero_equivalence_and_positive_mu():
    avg = request()
    zero = request(dict(MANIFEST, strategy="fedprox", **{"strategy-mu": 0.}))
    prox = request(dict(MANIFEST, strategy="fedprox", **{"strategy-mu": 0.1}))
    assert key(avg) == key(zero)
    assert key(avg) != key(prox)


@pytest.mark.parametrize("path", [
    ("runtime", "backend"), ("runtime", "packages"),
    ("model", "spec"), ("model", "spec", "layers", 0),
    ("selection", "preprocessing"), ("selection", "targets", 0),
    ("selection", "target_encoding"), ("training", "optimizer"),
    ("training", "scheduler"), ("privacy", "budget_allocation"),
    ("public_arrays", "schema", 0), ("mechanism", "profile"),
    ("mechanism", "profile", "parameters"),
])
def test_nested_objects_reject_unknown_nonce_fields(path):
    value = json.loads(request().canonical_json)
    value.pop("contract")
    target = value
    for field in path:
        target = target[field]
    target["nonce"] = "new draw"
    with pytest.raises(ValueError):
        seeding.build_request_identity(**value)


@pytest.mark.parametrize("path", [
    ("runtime", "backend", "kind"), ("runtime", "packages", "numpy"),
    ("model", "loss"), ("selection", "features", 0),
    ("selection", "targets", 0, "column"),
    ("selection", "preprocessing", "extractor_profile"),
    ("training", "learning_rate"), ("training", "optimizer", "momentum"),
    ("mechanism", "profile", "id"), ("initialisation", "mode"),
])
def test_nominal_scalar_slots_cannot_hide_arbitrary_objects(path):
    value = json.loads(request().canonical_json)
    value.pop("contract")
    target = value
    for field in path[:-1]:
        target = target[field]
    target[path[-1]] = {"nonce": "new draw"}
    with pytest.raises((ValueError, TypeError)):
        seeding.build_request_identity(**value)


def test_runtime_profile_is_closed_without_changing_baseline_fingerprint():
    base = request()
    for invalid in ({}, {"nonce": "test"}, {"contract": "native", "native_bundle_sha256": "f" * 64, "path": "/tmp"}):
        with pytest.raises(ValueError):
            seeding.request_identity("neural-dpsgd/v3", {}, POLICY,
                manifest=MANIFEST, execution_fingerprint=invalid)
    native = seeding.request_identity("neural-dpsgd/v3", {}, POLICY,
        manifest=MANIFEST, execution_fingerprint={"contract": "test-native-v1", "native_bundle_sha256": "f" * 64})
    assert json.loads(native.canonical_json)["runtime"]["native_bundle_sha256"] == "f" * 64
    assert set(json.loads(base.canonical_json)["runtime"]["packages"]) == {
        "cryptography", "numpy", "opacus", "torch", "torchvision"}


def test_invalid_partial_selection_does_not_silently_fall_back():
    with pytest.raises(ValueError, match="selection"):
        request(**{"request-selection": {"features": ["nonce"]}})


def test_real_policy_spellings_and_typed_level_aliases_normalize():
    assert request(policy=dict(POLICY, epsilon=1)).digest == request().digest
    with pytest.raises(ValueError):
        request(policy=dict(POLICY, epsilon=True))
    aliases = [
        {"type": "character", "values": ["control", "case"]},
        [{"type": "string", "value": "control"}, {"type": "string", "value": "case"}],
    ]
    assert request(dict(MANIFEST, **{"target-levels": aliases[0]})).digest == request(
        dict(MANIFEST, **{"target-levels": aliases[1]})).digest
    invalid = {"type": "numeric", "values": [0, 1], "nonce": "new draw"}
    with pytest.raises(ValueError):
        request(dict(MANIFEST, **{"target-levels": invalid}))


@pytest.mark.parametrize("field", ["image-size", "vision-extractor-profile", "segmentation-selection", "segmentation-preprocessing", "mask-vocabulary"])
def test_tabular_identity_drops_inactive_image_preprocessing(field):
    assert request().digest == request(dict(MANIFEST, **{field: "inactive-nonce"})).digest


@pytest.mark.parametrize("loss,classes,labels,width", [
    ("bce_logits", 2, 2, 1), ("mse", 2, 2, 1),
    ("cross_entropy", 5, 2, 5), ("ordinal", 5, 2, 4),
    ("multilabel_bce", 2, 7, 7),
])
def test_model_shape_is_effective_output_not_raw_class_count(loss, classes, labels, width):
    manifest = dict(MANIFEST, **{"loss-name": loss, "num-classes": classes, "num-labels": labels,
        "model-spec-b64": base64.b64encode(json.dumps({"layers": [{"op": "linear", "out": "@out"}]}).encode()).decode()})
    assert json.loads(request(manifest).canonical_json)["model"]["output_shape"] == [width]


@pytest.mark.parametrize("operation", ["holdout-evaluate", "cv-oof-release"])
def test_atomic_neural_evaluation_retains_fedprox_training_contract(operation):
    from dsflower_runner import validation
    layout = validation.validation_layout("classification", bins=4)
    manifest = dict(MANIFEST, **{"dp-track": "neural", "strategy": "fedprox", "strategy-mu": .25})
    prox = validation.build_validation_request(layout, 1., 1e-6, manifest=manifest, operation=operation)
    avg = validation.build_validation_request(layout, 1., 1e-6,
        manifest={k: v for k, v in dict(manifest, strategy="fedavg").items() if k != "strategy-mu"}, operation=operation)
    assert json.loads(prox.canonical_json)["strategy"]["mu"] == .25
    assert prox.digest != avg.digest
    with pytest.raises(ValueError, match="unsupported"):
        validation.build_validation_request(layout, 1., 1e-6, manifest=dict(manifest, **{"dp-track": "validation"}))


@pytest.mark.parametrize("geometry", [{"n_privacy_units": True}, {"sample_rate": {"nonce": 1}}, {"output_sigma": -1.}, {"vector_size": 1.5}])
def test_private_geometry_is_typed_and_bounded(geometry):
    with pytest.raises(ValueError, match="geometry"):
        bound(request(), geometry=geometry)


def test_row_policy_ignores_patient_canonicalization_metadata():
    row = request()
    staged = request(dict(MANIFEST, **{"patient-id-canonicalization": "trim-utf8-v2"}),
                     policy=dict(POLICY, unit_canonicalization="row-v1"))
    assert staged.digest == row.digest
    value = json.loads(staged.canonical_json)
    assert value["selection"]["unit_canonicalization"] == "row-content-occurrence-v1"
    assert value["privacy"]["unit_canonicalization"] == "row-content-occurrence-v1"
    patient = request(dict(MANIFEST, **{"dp-unit": "patient", "patient_column": "id"}))
    assert json.loads(patient.canonical_json)["selection"]["unit_canonicalization"] == "trim-utf8-v2"


def test_canonical_ast_preserves_admitted_symbolic_width_over_literal_cap():
    from dsflower_runner import model_spec
    layers = [{"op": "linear", "out": 1}, {"op": "linear", "out": "@in"}, {"op": "linear", "out": "@out"}]
    def cfg(value):
        return dict(MANIFEST, **{"num-features": 9000,
            "model-spec-b64": base64.b64encode(json.dumps({"layers": value}).encode()).decode()})
    admitted = cfg(layers)
    assert model_spec.canonical_spec(admitted)["layers"][1]["out"] == 9000
    assert json.loads(request(admitted).canonical_json)["model"]["spec"]["layers"][1]["out"] == 9000
    literal = [layers[0], {"op": "linear", "out": 9000}, layers[2]]
    with pytest.raises(ValueError, match="width.*cap"):
        model_spec.canonical_spec(cfg(literal))
