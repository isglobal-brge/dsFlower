"""Request identity stays distinct even when selected private values coincide."""

import os
import sys
from unittest import mock

import numpy as np
import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "flower_app"))
from dsflower_runner import client_app, seeding, tier2_lib


PRIVATE = (np.asarray([[1.0, 1.0], [2.0, 2.0]], dtype=np.float32),
           np.asarray([0.0, 1.0], dtype=np.float32))
PUBLIC = (np.asarray([[0.25, 0.5]], dtype=np.float32),)
POLICY = {"policy_hash": "1" * 64, "sigma": 2.0}

# Independent examples of each request field admitted by the node. Each pair
# keeps the effective private and public arrays fixed while changing one choice.
SELECTION_VALUES = {
    "request-source": ({"source": "table", "data_symbol": "cohort_a"},
                       {"source": "table", "data_symbol": "cohort_b"}),
    "dataset_id": ("cohort_a", "cohort_b"),
    "source_kind": ("staged_parquet", "image_bundle"),
    "data_type": ("tabular", "image"),
    "dp-track": ("neural", "tier2"),
    "task-type": ("classification", "regression"),
    "target_column": ("outcome_a", "outcome_b"),
    "feature_columns": (["x", "z"], ["x", "copy_of_z"]),
    "patient_column": ("patient_id", "subject_id"),
    "dp-unit": ("patient", "row"),
    "patient-id-canonicalization": ("trim-utf8-v2", "trim-utf8-v3"),
    "target-levels": (["no", "yes"], ["yes", "no"]),
    "target-bounds": ([0.0, 1.0], [0.0, 2.0]),
    "feature-bounds": ({"lower": [0, 0], "upper": [1, 1]},
                       {"lower": [0, 0], "upper": [2, 1]}),
    "target-preencoded": (False, True),
    "association-preencoded": (False, True),
    "model-spec-b64": ("bW9kZWwtYQ==", "bW9kZWwtYg=="),
    "loss-name": ("mse", "huber"),
    "num-features": (2, 3), "num-classes": (2, 3), "num-labels": (2, 3),
    "backbone": ("resnet18", "resnet34"), "image-size": (128, 224),
    "vision-extractor-profile": ("profile-a", "profile-b"),
    "survival-config": ({"distribution": "weibull"}, {"distribution": "lognormal"}),
    "image_asset": ("images", "scans"), "image_path_col": ("image", "scan"),
    "mask_asset": ("masks", "annotations"), "mask_path_col": ("mask", "label_mask"),
    "sample_id_col": ("image_id", "sample_id"),
    "subject_id_col": ("subject_id", "person_id"),
    "mask_empty_col": ("is_empty", "empty_mask"),
    "mask-vocabulary": ("0,255", "0,1"),
    "segmentation-alpha": (0.5, 1.0), "segmentation-smooth": (1.0, 2.0),
    "segmentation-selection": ("first-image", "last-image"),
    "segmentation-preprocessing": ("nearest-v1", "nearest-v2"),
    "segmentation-checkpoint-sha256": ("a" * 64, "b" * 64),
    "segmentation-output-shape": ("1,128,128", "1,224,224"),
    "segmentation-decoder-init": ("public:first", "public:second"),
    "segmentation-public-manifest-sha256": ("a" * 64, "b" * 64),
    "segmentation-public-checkpoint-sha256": ("a" * 64, "b" * 64),
    "validation-model-track": ("neural", "native_tree"),
    "validation-task": ("binary", "multiclass"), "validation-bins": (8, 16),
    "validation-contract-sha256": ("a" * 64, "b" * 64),
    "validation-artifact-format": ("torch-v1", "torch-v2"),
    "validation-artifact-sha256": ("a" * 64, "b" * 64),
    "validation-profile-sha256": ("a" * 64, "b" * 64),
    "validation-public-schema-sha256": ("a" * 64, "b" * 64),
    "association-contract": ("association-v1", "association-v2"),
    "association-contract-sha256": ("a" * 64, "b" * 64),
    "association-job-sha256": ("a" * 64, "b" * 64),
    "association-n-nodes": (2, 3), "association-privacy-unit": ("row", "patient"),
    "association-unit-semantics": ("binary-row", "binary-patient"),
    "resampling-version": (1, 2), "resampling-method": ("holdout", "holdout-v2"),
    "resampling-assignment": ("hmac-v1", "hmac-v2"),
    "resampling-test-numerator": (1, 2), "resampling-test-denominator": (5, 10),
    "resampling-privacy-unit": ("row", "patient"),
    "resampling-unit-canonicalization": ("row-index-v1", "trim-utf8-v2"),
    "resampling-contract-sha256": ("a" * 64, "b" * 64),
    "holdout-validation-bins": (8, 16),
    "cv-version": (1, 2), "cv-method": ("kfold", "kfold-v2"),
    "cv-assignment": ("hmac-v1", "hmac-v2"), "cv-folds": (3, 5),
    "cv-privacy-unit": ("row", "patient"),
    "cv-unit-canonicalization": ("row-index-v1", "trim-utf8-v2"),
    "cv-contract-sha256": ("a" * 64, "b" * 64),
    "cv-validation-bins": (8, 16), "cv-n-nodes": (2, 3),
    "cv-job-sha256": ("a" * 64, "b" * 64),
    "strategy": ("fedavg", "fedadam"),
    "strategy-eta": (0.1, 0.2), "strategy-eta-l": (0.1, 0.2),
    "strategy-beta-1": (0.9, 0.8), "strategy-beta-2": (0.999, 0.99),
    "strategy-tau": (0.001, 0.01), "strategy-server-learning-rate": (0.1, 0.2),
    "strategy-server-momentum": (0.0, 0.9),
}


def manifest():
    return {"data_type": "tabular", "target_column": "outcome_a",
            "feature_columns": ["x", "z"], "patient_column": "patient_id",
            "dp-unit": "patient", "patient-id-canonicalization": "trim-utf8-v2"}


def key(selected, *, private=PRIVATE, cfg=None, pins=None):
    config, _ = client_app._neural_seed_contract(
        {"loss-name": "mse"} if cfg is None else cfg,
        {"loss_name": "mse", "round_index": 1} if pins is None else pins,
        {}, manifest=selected)
    with mock.patch.object(seeding, "_node_secret", return_value=b"s" * 32):
        return seeding.master_seed(
            "neural-dpsgd/v1", config, POLICY, 1,
            public_arrays=PUBLIC, private_arrays=private,
            execution_fingerprint={"backend": "request-selection-test"})


def noise(master):
    return seeding.np_rng(seeding.sub_seed(master, "noise")).normal(size=32)


def test_identical_target_columns_do_not_reuse_noise_on_either_dataset():
    first = manifest()
    second = dict(first, target_column="outcome_b")
    # The two original columns are equal; the neighbour changes only outcome_b.
    neighbour = PRIVATE[1].copy()
    neighbour[0] = 1.0
    original_a, original_b = key(first), key(second)
    neighbour_a = key(first)
    neighbour_b = key(second, private=(PRIVATE[0], neighbour))
    assert original_a != original_b
    assert neighbour_a != neighbour_b
    assert original_a == neighbour_a
    assert original_b != neighbour_b
    assert noise(original_a).tobytes() != noise(original_b).tobytes()
    assert noise(neighbour_a).tobytes() != noise(neighbour_b).tobytes()


@pytest.mark.parametrize("columns", [["z", "x"], ["x", "copy_of_z"]])
def test_feature_order_and_identity_bind_identical_feature_tensors(columns):
    first = manifest()
    second = dict(first, feature_columns=columns)
    assert key(first) != key(second)
    assert noise(key(first)).tobytes() != noise(key(second)).tobytes()


def test_sticky_retry_and_changed_private_arrays():
    selected = manifest()
    reordered = dict(reversed(list(selected.items())))
    first = key(selected)
    replay = key(reordered, private=tuple(value.copy() for value in PRIVATE))
    assert first == replay
    assert noise(first).tobytes() == noise(replay).tobytes()
    for index in range(len(PRIVATE)):
        changed = [value.copy() for value in PRIVATE]
        changed[index].flat[0] += 1.0
        assert key(selected, private=tuple(changed)) != first


def test_analyst_request_selection_text_cannot_replace_manifest_selection():
    selected = manifest()
    trusted = key(selected)
    forged = {"loss-name": "mse", "request-selection": {
        "target_column": "forged", "feature_columns": ["forged"]},
        "target_column": "forged", "feature_columns": ["forged"]}
    assert key(selected, cfg=forged) == trusted
    assert key(dict(selected, target_column="outcome_b"), cfg=forged) != trusted


@pytest.mark.parametrize("field,value", [
    ("run_token", "new-run"), ("run-token", "new-run"),
    ("message-id", "new-message"), ("manifest-path", "/new/manifest.json"),
    ("data_file", "new/staging.csv"), ("samples_file", "new/samples.csv"),
    ("survival_file", "new/subjects.csv"), ("staged-at", "tomorrow"),
    ("results-dir", "/new/results"),
    ("request-selection", {"target_column": "forged"}),
])
def test_operational_metadata_is_not_a_request_selection(field, value):
    selected = manifest()
    assert key(dict(selected, **{field: value})) == key(selected)


@pytest.mark.parametrize("field,values", SELECTION_VALUES.items())
def test_every_manifest_selection_binds_key_noise_and_sticky_retry(field, values):
    first = dict(manifest(), **{field: values[0]})
    second = dict(manifest(), **{field: values[1]})
    original, changed = key(first), key(second)
    assert original != changed
    assert noise(original).tobytes() != noise(changed).tobytes()
    assert key(first) == original
    assert noise(key(first)).tobytes() == noise(original).tobytes()


def test_selection_fixture_covers_every_admitted_field():
    assert set(SELECTION_VALUES) == seeding._REQUEST_SELECTION_KEYS


def test_large_ordered_selections_and_model_specs_fit_the_outer_key_budget():
    selected = dict(manifest(), feature_columns=["x_%d" % i for i in range(16_384)])
    selected["model-spec-b64"] = "a" * 600_000
    cfg = {"loss-name": "mse", "model-spec-b64": selected["model-spec-b64"]}
    first = key(selected, cfg=cfg)
    assert key(selected, cfg=cfg) == first
    selected["feature_columns"] = list(reversed(selected["feature_columns"]))
    assert key(selected, cfg=cfg) != first


@pytest.mark.parametrize("field,value", [
    ("source", "resource"), ("data_symbol", "other_operand"),
    ("dataset_id", "other_dataset"), ("source_kind", "image_bundle"),
])
def test_each_trusted_source_selector_binds_identical_private_data(field, value):
    source = {"source": "descriptor", "data_symbol": "cohort",
              "dataset_id": "dataset", "source_kind": "staged_parquet"}
    selected = dict(manifest(), **{"request-source": source})
    first = key(selected)
    changed = dict(selected, **{"request-source": dict(source, **{field: value})})
    assert key(changed) != first
    assert noise(key(changed)).tobytes() != noise(first).tobytes()
    assert key(selected) == first


@pytest.mark.parametrize("targets", [["other_time", "event"], ["time", "other_event"],
                                    ["event", "time"]])
def test_survival_time_event_identity_and_order_bind_identical_tensors(targets):
    selected = dict(manifest(), **{"task-type": "survival",
                                  "target_column": ["time", "event"]})
    assert key(selected) != key(dict(selected, target_column=targets))


@pytest.mark.parametrize("levels", [[1, 0], ["0", "1"], [0, 2]])
def test_vocabulary_order_type_and_identity_bind_identical_encoded_targets(levels):
    selected = dict(manifest(), **{"target-levels": [0, 1]})
    assert key(selected) != key(dict(selected, **{"target-levels": levels}))


@pytest.mark.parametrize("asset", ["images", "masks"])
@pytest.mark.parametrize("field", ["type", "kind", "path_col"])
def test_selected_asset_roles_bind_key_but_relocated_files_do_not(asset, field):
    selected = dict(manifest(), data_type="image", assets={
        "images": {"type": "image", "kind": "collection", "path_col": "image_path"},
        "masks": {"type": "mask", "kind": "collection", "path_col": "mask_path"}},
        **{"loss-name": "segmentation_bce_dice"})
    first = key(selected)
    moved = dict(selected, assets={name: dict(value, root="/new/root", file="new.csv",
                                             uri="new://location")
                                   for name, value in selected["assets"].items()})
    moved["assets"]["unused"] = {"path_col": "irrelevant"}
    assert key(moved) == first
    moved["assets"][asset][field] += "_other"
    assert key(moved) != first


@pytest.mark.parametrize("sample_aggregate", [False, True])
def test_hook_noise_execution_and_bound_update_bind_node_selection(sample_aggregate):
    cfg = {"app_params": {}, "round_index": 1, "num_rounds": 1,
           "task": "regression", "num_classes": 2}
    policy = dict(POLICY, sample_aggregate=sample_aggregate)

    def derive(selected, digest="a" * 64, private=PRIVATE):
        with mock.patch.object(tier2_lib, "_pinned_user_package", return_value=digest), \
                mock.patch.object(tier2_lib.seeding, "_node_secret", return_value=b"s" * 32):
            selection = seeding.request_selection(selected)
            master = tier2_lib.hook_master_seed(
                "test_hook", PUBLIC, *private, cfg, policy,
                request_selection=selection)
            execution = tier2_lib.hook_execution_seed(
                "test_hook", PUBLIC, cfg, policy, request_selection=selection)
        bound = tier2_lib.seeding.bind_seed(master, "hook-update", PUBLIC)
        return master, execution, bound

    selected = manifest()
    first = derive(selected)
    assert derive(selected) == first
    renamed = derive(dict(selected, target_column="outcome_b"))
    recoded = derive(selected, digest="b" * 64)
    assert all(a != b for a, b in zip(first, renamed))
    assert all(a != b for a, b in zip(first, recoded))
    changed = derive(selected, private=(PRIVATE[0] + 1.0, PRIVATE[1]))
    assert changed[0] != first[0]
    assert changed[1] == first[1]  # execution remains independent of private data
    assert changed[2] != first[2]
    assert noise(first[2]).tobytes() == noise(derive(selected)[2]).tobytes()
    assert noise(first[2]).tobytes() != noise(renamed[2]).tobytes()
