"""Release boundaries use complete source units and replay before private draws."""
import os
import sys
from unittest import mock

import numpy as np
import pytest

TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS)
sys.path.insert(0, os.path.join(TESTS, "..", "..", "flower_app"))
from dsflower_runner import canonical_units, epi_association, native_tree_engine, seeding, tree_release, validation


@pytest.fixture(autouse=True)
def custodial_state(tmp_path, monkeypatch):
    secret = tmp_path / "node-secret"
    secret.write_text("61" * 32 + "\n")
    secret.chmod(0o600)
    monkeypatch.setenv("DSFLOWER_NODE_SECRET_FILE", str(secret))
    monkeypatch.delenv("DSFLOWER_NEIGHBOURHOOD_STATE_DIR", raising=False)
    monkeypatch.delenv("DSFLOWER_NEIGHBOURHOOD_STORE_UUID", raising=False)
    monkeypatch.setenv("DSFLOWER_NEIGHBOURHOOD_K", "3")


def _units(n=8, patient=False):
    return canonical_units.canonicalize_arrays(
        np.arange(n, dtype=float).reshape(-1, 1), np.arange(n) % 2,
        unit_ids=[str(i) for i in range(n)] if patient else None)


@pytest.mark.parametrize("task,options", [
    ("classification", {"n_classes": 2, "bins": 4}),
    ("classification", {"n_classes": 3, "bins": 4}),
    ("ordinal", {"n_classes": 3, "bins": 4}),
    ("multilabel", {"n_labels": 2, "bins": 4}),
    ("regression", {}), ("count", {}),
    ("segmentation", {}), ("survival", {"horizons": [1., 2.]}),
])
def test_validation_layouts_replay_full_vector_and_sigma_before_noise(task, options):
    layout = validation.validation_layout(task, **options)
    def release(n):
        return validation.private_sufficient_vector(
            np.full(layout["size"], n, dtype=float), layout,
            epsilon=1.0, delta=1e-5, source_units=_units(n))
    with mock.patch.object(seeding, "np_rng", wraps=seeding.np_rng) as draws:
        first, sigma = release(8)
        exact, exact_sigma = release(8)
        near, near_sigma = release(7)
        far, far_sigma = release(5)
    assert draws.call_count == 2
    assert first.tobytes() == exact.tobytes() == near.tobytes()
    assert first.tobytes() != far.tobytes()
    assert sigma == exact_sigma == near_sigma == far_sigma


@pytest.mark.parametrize("unit", ["row", "patient"])
def test_association_replays_full_payload_with_original_source_records(unit):
    def release(n):
        ids = [str(i) for i in range(n)] if unit == "patient" else None
        raw = epi_association.association_sufficient_vector(
            np.arange(n) % 2, np.arange(n) % 3,
            outcome_levels=(0, 1), exposure_levels=(0, 1),
            privacy_unit=unit, unit_ids=ids)
        return epi_association.private_association_vector(
            raw, privacy_unit=unit, epsilon=1.0, delta=1e-5)
    with mock.patch.object(seeding, "np_rng", wraps=seeding.np_rng) as draws:
        first, sigma = release(8)
        near, near_sigma = release(7)
        far, far_sigma = release(5)
    assert draws.call_count == 2
    assert first.tobytes() == near.tobytes()
    assert first.tobytes() != far.tobytes()
    assert sigma == near_sigma == far_sigma


@pytest.mark.parametrize("engine", ["extra_trees", "random_forest", "lightgbm", "catboost"])
def test_native_engines_anchor_complete_sanitized_artifact(engine):
    from test_native_tree_engine import (_manifest, random_forest_request,
        boosting_request, forest_request)
    if engine == "extra_trees":
        request = forest_request(trees=2, depth=1)
    elif engine == "random_forest":
        request = random_forest_request()
    else:
        request = boosting_request(engine)
    manifest = _manifest(request)
    X = np.column_stack((np.arange(8) + 20., np.arange(8) / 10.))
    y = np.arange(8, dtype=float) % 2
    def release(n, order=None):
        order = np.arange(n) if order is None else order
        return native_tree_engine.train_model(manifest, X[:n][order], y[:n][order])
    with mock.patch.object(tree_release, "joint_gaussian_release",
                           wraps=tree_release.joint_gaussian_release) as draws:
        first = release(8)
        calls = draws.call_count
        assert calls > 0
        assert release(8, np.arange(7, -1, -1)) == first
        assert release(7) == first
        assert draws.call_count == calls
        far = release(5)
        assert draws.call_count > calls
    assert far != first
    ensemble, _ = native_tree_engine.build_ensemble(manifest, [first])
    assert np.isfinite(native_tree_engine.parse_ensemble(manifest, ensemble).predict(X)).all()


def test_xgboost_anchor_wraps_one_native_invocation_and_keeps_full_binding():
    from dsflower_runner import xgboost_adapter
    from test_xgboost_adapter import _manifest, XGBoostPrfAndBoundaryTests
    manifest = _manifest()
    X = np.column_stack((np.arange(8) + 20., np.arange(8) / 10.))
    y = np.arange(8, dtype=float) % 2
    bundle = XGBoostPrfAndBoundaryTests._bundle("f" * 64)
    bindings = []
    def native(prepared):
        bindings.append(prepared._data_binding.digest)
        return b"sanitized-model-" + prepared._data_binding.digest
    def release(n):
        return native_tree_engine.train_model(manifest, X[:n], y[:n],
            unit_ids=[str(i) for i in range(n)], xgboost_bundle=bundle)
    with mock.patch.object(xgboost_adapter, "train_xgboost_native", side_effect=native) as train, \
            mock.patch.object(xgboost_adapter, "sanitize_xgboost_artifact",
                              side_effect=lambda _manifest, artifact: (artifact, "digest")):
        first = release(8)
        assert release(7) == first
        assert train.call_count == 1
        assert release(5) != first
        assert train.call_count == 2
    assert bindings[0] != bindings[1]


def test_patient_all_visits_are_one_unit_for_validation_replay():
    layout = validation.validation_layout("regression")
    ids = np.repeat(["a", "b", "c", "d"], 3)
    X = np.arange(12, dtype=float).reshape(-1, 1)
    def release(values):
        units = canonical_units.canonicalize_arrays(values, np.zeros(12), ids)
        return validation.private_sufficient_vector(np.full(layout["size"], values.sum()),
            layout, epsilon=1., delta=1e-5, source_units=units)[0]
    first = release(X)
    changed = X.copy()
    changed[:3] += 100
    with mock.patch.object(seeding, "np_rng", side_effect=AssertionError("fresh noise on near patient")):
        assert release(changed).tobytes() == first.tobytes()


def test_neural_oof_retains_complete_parent_units_replays_and_purges():
    from types import SimpleNamespace
    from flwr.common import ConfigRecord, RecordDict
    from dsflower_runner import client_app, resampling, task
    contract = resampling.cross_validation_contract(3, "row")
    manifest = {"dp-unit": "row", "patient_column": None,
                "cv-job-sha256": "e" * 64,
                **resampling.cross_validation_manifest_fields(contract)}
    cfg = {"loss-name": "bce_logits", "task-type": "classification",
           "num-classes": 2, "cv-validation-bins": 4}
    layout = validation.cross_validation_layout_from_config(cfg)
    def release(n):
        context = SimpleNamespace(state=RecordDict())
        units = _units(n)
        client_app._parent_source_binding(context, units)
        # Round-trip the private Flower Context as between isolated fold tasks.
        parent = context.state["dsflower-resampling-source-v3"]
        context.state["dsflower-resampling-source-v3"] = ConfigRecord(dict(parent))
        assert tuple(parent["unit-records"]) == units.records
        for fold in range(1, 4):
            client_app._store_cv_sufficient(context, fold,
                np.full(layout["size"], n, dtype=float), layout,
                public_arrays=[np.full((1, 2), fold, dtype=np.float32)])
        result = client_app._cross_validation_release(context, cfg,
            {"epsilon": 1., "delta": 1e-5})[0]
        assert "dsflower-resampling-source-v3" not in context.state
        assert client_app._CV_OOF_META_KEY not in context.state
        assert client_app._CV_OOF_TOTAL_KEY not in context.state
        return result
    with mock.patch.object(task, "_load_manifest", return_value=manifest), \
            mock.patch.object(seeding, "np_rng", wraps=seeding.np_rng) as draws:
        first = release(8)
        assert release(7).tobytes() == first.tobytes()
        assert draws.call_count == 1
        assert release(5).tobytes() != first.tobytes()
        assert draws.call_count == 2


def test_holdout_and_oof_distance_use_parent_records_not_only_test_side():
    layout = validation.validation_layout("regression")
    for role, operation in (("test", "holdout-evaluate"), ("oof", "cv-oof-release")):
        # Different public operation coordinates have independent anchors.
        request = validation.build_validation_request(layout, 1., 1e-5,
            operation=operation,
            fold_model_sha256=([{"fold": i, "model_sha256": str(i) * 64}
                               for i in range(1, 3)] if role == "oof" else None))
        def release(n):
            return validation.private_sufficient_vector(np.zeros(layout["size"]),
                layout, epsilon=1., delta=1e-5, source_units=_units(n),
                request_identity=request,
                subset={"role": role, "assignment_sha256": str(n) * 64})[0]
        with mock.patch.object(seeding, "np_rng", wraps=seeding.np_rng) as draws:
            first = release(8)
            assert release(7).tobytes() == first.tobytes()
            assert release(5).tobytes() != first.tobytes()
            assert draws.call_count == 2


def test_real_xgboost_neighbourhood_with_verified_bundle():
    """Two short native fits; exact/shuffled/near requests never invoke the ABI."""
    from dsflower_runner import xgboost_adapter, xgboost_bundle
    from test_xgboost_adapter import _manifest
    bundle_path = os.environ.get("DSFLOWER_XGBOOST_BUNDLE")
    if not bundle_path:
        pytest.skip("DSFLOWER_XGBOOST_BUNDLE is required for the real native fit")
    bundle = xgboost_bundle.load_verified_xgboost_bundle(bundle_path)
    manifest = _manifest()
    manifest["engine_params"]["num_boost_round"]["value"] = 1
    manifest["engine_params"]["max_depth"]["value"] = 1
    X = np.column_stack((np.arange(8) * 10. + 10., np.arange(8) / 10.))
    y = np.arange(8, dtype=float) % 2
    ids = np.asarray(["patient-%s" % index for index in range(8)])
    prepared_inputs = []
    original_prepare = xgboost_adapter.prepare_xgboost_training
    def prepare(*args, **kwargs):
        prepared = original_prepare(*args, **kwargs)
        assert len(prepared._noise_key) == 32
        prepared_inputs.append(prepared)
        return prepared
    def release(n, order=None):
        order = np.arange(n) if order is None else order
        return native_tree_engine.train_model(manifest, X[:n][order], y[:n][order],
            unit_ids=ids[:n][order], xgboost_bundle=bundle)
    with mock.patch.object(xgboost_adapter, "prepare_xgboost_training", side_effect=prepare), \
            mock.patch.object(xgboost_adapter, "train_xgboost_native",
                              wraps=xgboost_adapter.train_xgboost_native) as native:
        first = release(8)
        assert native.call_count == 1
        assert release(8) == first
        assert release(8, np.arange(7, -1, -1)) == first
        assert release(7) == first
        assert native.call_count == 1
        far = release(5)
        assert native.call_count == 2
    assert prepared_inputs[0]._data_binding.digest != prepared_inputs[-1]._data_binding.digest
    assert len(prepared_inputs) == 5
    assert all(value._noise_key == bytearray(32) for value in prepared_inputs)
    # Discrete noisy models can collide, so freshness is checked by native
    # invocation and source binding, not an assumption that artifact bits differ.
    for artifact in (first, far):
        sanitized, digest = xgboost_adapter.sanitize_xgboost_artifact(manifest, artifact)
        assert sanitized == artifact
        ensemble, _ = native_tree_engine.build_ensemble(manifest, [artifact])
        prediction = np.asarray(native_tree_engine.parse_ensemble(manifest, ensemble).predict(X))
        assert prediction.shape == (8,)
        assert np.isfinite(prediction).all()
