"""Fixed public predictors and patient-decomposable private metric contracts."""

import base64
import copy
import json
import math
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
from scipy import stats
import torch

from test_segmentation_public_init import registry, pinned, message, pins
from test_survival_contracts import config as survival_config
from dsflower_runner import (client_app, params, resampling, seeding,
                             segmentation, segmentation_checkpoints as checkpoints,
                             server_app, survival, task, validation)


def test_segmentation_sufficient_statistics_and_sensitivity():
    layout = validation.validation_layout("segmentation")
    truth = np.zeros((4, 2, 128, 128), dtype=np.float32)
    truth[:, 1] = 1
    truth[0, 0] = 1
    truth[1, 0, :64] = 1
    truth[3, 1] = 0  # invalid subject retained as a zero-statistic contribution
    prediction = np.zeros((4, 1, 128, 128), dtype=np.float32)
    prediction[0] = 1
    prediction[1, :, :, :64] = 1
    prediction[3] = 1
    rows = validation.validation_contributions(truth, prediction, layout)
    np.testing.assert_array_equal(rows, [[1, 1, 1, 1], [1, .25, .5, .5],
                                         [1, 0, 0, 0], [1, 0, 0, 0]])
    assert max(np.linalg.norm(a-b) for a in rows for b in rows) <= math.sqrt(3)
    assert validation._validation_release_sensitivity(layout, include_zero_neighbor=True) == 2
    assert validation.validation_metrics(rows.sum(0), layout) == {
        "n": 4., "foreground_dice": 2.5 / 3.}
    assert validation.validation_metrics(np.zeros(4), layout)["foreground_dice"] == 0
    with pytest.raises(ValueError, match="geometry"):
        validation.validation_contributions(truth[:, :, :64], prediction, layout)
    with pytest.raises(ValueError, match="pixel bound"):
        validation.validation_sufficient_vector(truth, prediction, dict(layout, max_pixels=1))


@pytest.mark.parametrize("distribution", ["weibull", "lognormal", "hazard"])
def test_survival_metrics_match_independent_parametric_likelihood_and_known_status(distribution):
    config = (survival_config(edges=[0., 5., 10., 20.]) if distribution == "hazard"
              else survival_config(distribution))
    loss = "discrete_hazard_nll" if distribution == "hazard" else "aft_" + distribution + "_nll"
    cfg = {"loss-name": loss, "task-type": "survival", "validation-task": "survival",
           "survival-config": config, "validation-survival-horizons": "[5,10,20]",
           "validation-survival-nll-bound": 20.}
    layout = validation.layout_from_config(cfg)
    target = np.array([[3, 1, 1], [5, 0, 1], [9, 1, 1], [20, 0, 1], [1, 0, 0]], float)
    width = 3 if distribution == "hazard" else 1
    model = torch.nn.Linear(1, width)
    with torch.no_grad():
        model.weight.zero_()
        model.bias.zero_()
    predictions = validation.metric_predictions(model, np.ones((5, 1)), target, cfg, layout)
    if distribution == "hazard":
        # Training likelihood divides every subject's summed interval NLL by K.
        expected_nll = np.log(2) * np.array([1, 1, 2, 3]) / 3
        expected_curves = np.tile([.5, .25, .125], (5, 1))
    else:
        reference = (stats.weibull_min(c=1, scale=5) if distribution == "weibull"
                     else stats.lognorm(s=1, scale=5))
        expected_nll = -np.where(target[:4, 1] == 1,
                                 reference.logpdf(target[:4, 0]), reference.logsf(target[:4, 0]))
        expected_curves = np.tile(reference.sf([5, 10, 20]), (5, 1))
    np.testing.assert_allclose(predictions[:4, 0], expected_nll, rtol=1e-6)
    np.testing.assert_allclose(predictions[:, 1:], expected_curves, rtol=1e-6)
    rows = validation.validation_contributions(target, predictions, layout)
    assert np.all((rows >= 0) & (rows <= 1))
    np.testing.assert_array_equal(rows[-1], np.zeros(layout["size"]))
    result = validation.validation_metrics(rows.sum(0), layout)
    assert result["negative_log_likelihood"] == pytest.approx(np.mean(expected_nll), abs=1e-6)
    assert result["brier"]["eligible_n"] == [4, 3, 3]
    known = [[0, 1, 1, 1], [0, 0, 1], [0, 0, 1]]
    curves = expected_curves[0]
    expected_brier = [np.mean((curve-np.asarray(y))**2) for curve, y in zip(curves, known)]
    np.testing.assert_allclose(result["brier"]["scores"], expected_brier)
    assert "concordance_index" not in result
    assert layout == validation.holdout_layout_from_config(cfg)
    assert layout == validation.cross_validation_layout_from_config(cfg)


def test_survival_symmetric_clipping_negative_density_and_invalid_sensitivity():
    layout = validation.validation_layout("survival", horizons=[5., 10.], nll_bound=20.)
    y = np.array([[2, 1, 1], [10, 0, 1], [1, 0, 0]], float)
    predictions = np.array([[-100, 1, 1], [100, 0, 0], [100, 1, 1]], float)
    rows = validation.validation_contributions(y, predictions, layout)
    assert rows[0, 1] == 0 and rows[1, 1] == 1
    assert max(np.linalg.norm(a-b) for a in rows for b in rows) <= layout["sensitivity"]
    assert max(np.linalg.norm(a) for a in rows) <= layout["sensitivity"]
    metrics = validation.validation_metrics(rows.sum(0), layout)
    assert metrics["negative_log_likelihood"] == 0
    assert validation.validation_metrics(rows[2], layout)["negative_log_likelihood"] is None
    for horizon in ([], [0], [10, 5], [float("nan")], [True]):
        with pytest.raises(ValueError):
            validation.validation_layout("survival", horizons=horizon)


@pytest.mark.parametrize("name", ["segmentation", "survival"])
def test_two_node_gaussian_pool_matches_plain_bounded_statistics_within_noise(name):
    if name == "segmentation":
        layout = validation.validation_layout(name)
        y = np.ones((8, 1, 128, 128))
        prediction = y.copy()
        prediction[:4, :, :64] = 0
    else:
        layout = validation.validation_layout(name, horizons=[5, 10])
        y = np.tile([[3, 1, 1], [10, 0, 1]], (4, 1))
        prediction = np.tile([[2, .6, .3], [1, .9, .8]], (4, 1))
    plain = validation.validation_sufficient_vector(y, prediction, layout)
    releases = []
    for index, part in enumerate((slice(0, 4), slice(4, 8))):
        with mock.patch.object(seeding, "_node_secret", return_value=bytes([index + 1])*32):
            released, sigma = validation.private_validation_vector(
                y[part], prediction[part], layout, epsilon=1e6, delta=1e-6)
        releases.append(released)
    pooled = np.sum(releases, axis=0)
    np.testing.assert_allclose(pooled, plain, atol=8*math.sqrt(2)*sigma)
    actual, expected = (validation.validation_metrics(value, layout) for value in (pooled, plain))
    if name == "segmentation":
        assert actual["foreground_dice"] == pytest.approx(expected["foreground_dice"], abs=.01)
    else:
        np.testing.assert_allclose(actual["brier"]["scores"], expected["brier"]["scores"], atol=.01)
        assert actual["negative_log_likelihood"] == pytest.approx(expected["negative_log_likelihood"], abs=.1)


def validation_fixture(registry):
    cfg = dict(pinned(registry), **{"validation-task": "segmentation", "validation-bins": 32,
                                    "validation-model-track": "neural"})
    model = params.load_user_model(cfg, segmentation.FEATURE_DIM, "segmentation_bce_dice")
    params.set_torch_params(model, registry.arrays)
    X = np.zeros((3, segmentation.FEATURE_DIM), np.float32)
    y = np.ones((3, 2, 128, 128), np.float32)
    return cfg, model, X, y


def test_fixed_declared_segmentation_checkpoint_one_and_two_node_validation(registry):
    cfg, model, X, y = validation_fixture(registry)
    with mock.patch.object(segmentation, "prepare_encoder", return_value=(None, "cpu")), \
            mock.patch.object(segmentation, "load_subject_tensors", return_value=(X, y, np.array(["a", "b", "c"]), 3)):
        single = validation.private_model_validation(registry.context, cfg, {"epsilon": 1e6, "delta": 1e-6}, 1, registry.arrays)[0]
    layout = validation.layout_from_config(cfg)
    expected = validation.validation_sufficient_vector(y, validation.neural_predictions(model, X, cfg["loss-name"]), layout)
    for result, plain in ((single, expected), (single*2, expected*2)):
        assert validation.validation_metrics(result, layout)["foreground_dice"] == pytest.approx(
            validation.validation_metrics(plain, layout)["foreground_dice"], abs=.01)


@pytest.mark.parametrize("mutation", ["digest", "shape", "policy_resource", "policy_none"])
def test_fixed_checkpoint_rejections_precede_private_access(registry, mutation):
    cfg, _, _, _ = validation_fixture(registry)
    arrays = [a.copy() for a in registry.arrays]
    if mutation == "digest":
        arrays[0].flat[0] += 1
    elif mutation == "shape":
        arrays[0] = arrays[0].reshape(-1)
    else:
        policy = "resource_only" if mutation == "policy_resource" else "none"
        cfg[checkpoints.POLICY_KEY] = policy
        registry.node[checkpoints.POLICY_KEY] = policy
        (registry.run / "manifest.json").write_text(json.dumps(registry.node))
    with mock.patch.object(segmentation, "load_subject_tensors") as private, \
            mock.patch.object(task, "load_data") as tabular:
        with pytest.raises(ValueError):
            validation.private_model_validation(registry.context, cfg, {"epsilon": 1, "delta": 1e-6}, 1, arrays)
    private.assert_not_called()
    tabular.assert_not_called()


def test_every_cv_fold_restarts_exact_public_material_and_partition_is_invariant(registry):
    contract = resampling.cross_validation_contract(2, "patient")
    fields = resampling.cross_validation_manifest_fields(contract)
    payload = checkpoints.client_payload(registry.local_directory)
    cfg = dict(registry.cfg, **fields, **{checkpoints.TRANSPORT_KEY:
               base64.b64encode(json.dumps(payload).encode()).decode()})
    ids = np.array(["patient-%d" % i for i in range(40)])
    before = resampling.cross_validation_folds(contract, n_rows=len(ids), unit_ids=ids)
    for fold in (1, 2):
        _, record = server_app._cross_validation_initial_arrays(cfg, fold)
        for expected, actual in zip(registry.arrays, record.to_numpy_ndarrays()):
            np.testing.assert_array_equal(actual, expected)
        changed = dict(registry.node, **fields, **{checkpoints.MANIFEST_KEY: str(fold)*64, "patient-id-canonicalization": "trim-utf8-v2"})
        after = resampling.cross_validation_folds(resampling.cross_validation_contract_from_manifest(changed),
                                                   n_rows=len(ids), unit_ids=ids)
        np.testing.assert_array_equal(before, after)
    base, _ = client_app._neural_seed_contract(cfg, dict(pins(), fold_index=1), {}, manifest=registry.node)
    other, _ = client_app._neural_seed_contract(cfg, dict(pins(), fold_index=2), {}, manifest=registry.node)
    assert base != other


def test_segmentation_empty_cv_fold_retains_pinned_accounting_geometry(registry):
    fields = resampling.cross_validation_manifest_fields(resampling.cross_validation_contract(2, "patient"))
    cfg = dict(pinned(registry), **fields)
    registry.node.update(fields, **{"n_samples": 3, "n_units": 3, "patient-id-canonicalization": "trim-utf8-v2"})
    (registry.run / "manifest.json").write_text(json.dumps(registry.node))
    model = params.load_user_model(cfg, segmentation.FEATURE_DIM, "segmentation_bce_dice")
    X = np.zeros((3, segmentation.FEATURE_DIM), np.float32)
    y = np.ones((3, 2, 128, 128), np.float32)
    training = dict(pins(), n_classes=2, batch_size=2, local_epochs=1)
    with mock.patch.object(segmentation, "prepare_encoder", return_value=(None, "cpu")), \
            mock.patch.object(segmentation, "load_subject_tensors", return_value=(X, y, np.array(["a", "b", "c"]), 3)), \
            mock.patch.object(client_app, "_cross_validation_partition", return_value=(X[:0], y[:0], np.array([], dtype=str))), \
            mock.patch.object(client_app, "_dp_fit", return_value=([], 3)) as fit:
        client_app._train_segmentation(registry.context, cfg, {"epsilon": 2, "delta": 1e-6, "clipping_norm": 1},
                                       training, model, cv_fold=1)
    assert fit.call_args.kwargs["geometry_n_units"] == 3
    assert fit.call_args.kwargs["public_zero_gradient"] is True
    assert fit.call_args.args[1].shape == (1, segmentation.FEATURE_DIM)


@pytest.fixture
def staged_survival():
    import test_survival_contracts as fixtures
    fixture = fixtures.SurvivalRunnerTests()
    fixture.setUp()
    try:
        yield fixture
    finally:
        fixture.tearDown()


@pytest.mark.parametrize("hazard", [False, True])
def test_survival_actual_staged_validation_holdout_and_oof_paths(staged_survival, hazard):
    fixture = staged_survival
    if hazard:
        fixture.hazard_fixture()
    from flwr.common import RecordDict
    spec = {"kind": "sequential", "layers": [{"op": "linear", "out": "@out"}]}
    fixture.manifest.update({"num-features": 2, "model-spec-b64": base64.b64encode(json.dumps(spec).encode()).decode(),
                             "validation-survival-horizons": "[5,10,20]", "validation-survival-nll-bound": 20.,
                             "run_token": "run_" + "c"*32})
    fixture.write_manifest()
    cfg = dict(fixture.manifest)
    # Validation's ordinary tabular protocol omits data-kind, not the manifest.
    cfg.update({"validation-task": "survival", "validation-model-track": "neural"})
    layout = validation.layout_from_config(cfg)
    model = params.load_user_model(cfg, 2, cfg["loss-name"])
    for p in model.parameters():
        p.data.zero_()
    arrays = params.get_torch_params(model)
    with mock.patch.object(seeding, "_node_secret", return_value=b"t"*32):
        direct = validation.private_model_validation(fixture.context, cfg,
            {"epsilon": 1e6, "delta": 1e-6}, 1, arrays)[0]
        X, y, ids, _ = task.load_survival_data(fixture.context, metric_targets=True)
        assert y.shape == (7, 3)
        expected = validation.validation_sufficient_vector(y, validation.metric_predictions(model, X, y, cfg, layout), layout, unit_ids=ids)
        np.testing.assert_allclose(direct, expected, atol=.03)
        holdout = resampling.manifest_fields(resampling.holdout_contract(500000, "patient"))
        fixture.manifest.update(holdout)
        fixture.write_manifest()
        holdout_cfg = dict(cfg, **holdout)
        released = client_app._holdout_neural_release(fixture.context, holdout_cfg,
            {"epsilon": 1e6, "delta": 1e-6}, {"loss_name": cfg["loss-name"]}, model, 2)[0]
        test_x, test_y, test_ids = client_app._holdout_partition(fixture.context, X, y, ids, subset="test")
        test_expected = validation.validation_sufficient_vector(test_y,
            validation.metric_predictions(model, test_x, test_y, cfg, layout), layout, unit_ids=test_ids)
        np.testing.assert_allclose(released, test_expected, atol=.03)
        for key in holdout:
            fixture.manifest.pop(key)
        cv = resampling.cross_validation_manifest_fields(resampling.cross_validation_contract(2, "patient"))
        fixture.manifest.update(cv, **{"cv-job-sha256": "d"*64})
        fixture.context.state = RecordDict()
        fixture.write_manifest()
        cv_cfg = dict(cfg, **cv)
        for fold in (1, 2):
            response = client_app._cross_validation_neural_accumulate(fixture.context, cv_cfg,
                {"loss_name": cfg["loss-name"]}, model, 2, fold)
            np.testing.assert_array_equal(response, [[0.]])
        pooled = client_app._cross_validation_release(fixture.context, cv_cfg,
            {"epsilon": 1e6, "delta": 1e-6})[0]
        np.testing.assert_allclose(pooled, expected, atol=.03)
        assert not fixture.context.state


@pytest.mark.parametrize("name,primary", [("segmentation", .5), ("survival", -2.), ("survival", None)])
def test_new_pooled_metric_layouts_are_saved_without_private_records(tmp_path, name, primary):
    layout = validation.validation_layout(name, horizons=[5] if name == "survival" else None)
    metrics = {"n": 3., "foreground_dice" if name == "segmentation" else "negative_log_likelihood": primary}
    cfg = {"results-dir": str(tmp_path / "results"), "cv-contract-sha256": "1"*64,
           "cv-job-sha256": "2"*64, "min-train-nodes": 2}
    server_app._save_cross_validation(cfg, layout, metrics, 2)
    output = json.loads((tmp_path / "results" / "cv.json").read_text())
    assert output["metrics"] == metrics
    assert output["pooled_only"] is True


@pytest.mark.parametrize("field,value", [
    ("survival_schema", "untrusted"), ("survival_shape", [7, 3, 3]),
    ("survival_file", "../source.csv"), ("survival_target_columns", ["private"]),
])
def test_survival_public_artifact_geometry_is_checked_before_private_reads(staged_survival, field, value):
    fixture = staged_survival
    fixture.manifest[field] = value
    fixture.write_manifest()
    with mock.patch.object(task, "_read_staged_frame") as read:
        with pytest.raises(ValueError):
            task.load_survival_data(fixture.context, metric_targets=True)
    read.assert_not_called()


@pytest.mark.parametrize("name", ["segmentation", "survival"])
def test_metric_postprocessing_remains_finite_for_extreme_public_releases(name):
    layout = validation.validation_layout(name, horizons=[5] if name == "survival" else None)
    for sign in (-1, 1):
        result = validation.validation_metrics(np.full(layout["size"], sign*np.finfo(float).max), layout)
        json.dumps(result, allow_nan=False)
        if name == "survival" and result["negative_log_likelihood"] is not None:
            assert abs(result["negative_log_likelihood"]) <= layout["nll_bound"]


def test_saved_segmentation_artifact_preflight_arrays_and_private_inference(registry, tmp_path):
    import hashlib
    cfg, model, X, y = validation_fixture(registry)
    for key in (checkpoints.INIT_KEY, *checkpoints.NODE_KEYS):
        cfg.pop(key, None)
        registry.node.pop(key, None)
    (registry.run / "manifest.json").write_text(json.dumps(registry.node))
    artifact = tmp_path / "saved-model.pt"
    torch.save(model.state_dict(), artifact)
    cfg.update({"validation-model-path-b64": base64.b64encode(str(artifact).encode()).decode(),
                "validation-artifact-format": "pytorch-state-dict-v1",
                "validation-artifact-sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
                "validation-artifact-size-bytes": artifact.stat().st_size})
    transported = validation.public_model_arrays(cfg)
    for expected, actual in zip(registry.arrays, transported):
        np.testing.assert_array_equal(actual, expected)
    with mock.patch.object(segmentation, "prepare_encoder", return_value=(None, "cpu")), \
            mock.patch.object(segmentation, "load_subject_tensors", return_value=(X, y, np.array(["a", "b", "c"]), 3)):
        released = validation.private_model_validation(registry.context, cfg,
            {"epsilon": 1e6, "delta": 1e-6}, 1, transported)[0]
    assert validation.validation_metrics(released, validation.layout_from_config(cfg))["foreground_dice"] == pytest.approx(1., abs=.01)
    artifact.write_bytes(artifact.read_bytes() + b"changed")
    with mock.patch.object(torch, "load") as decode:
        with pytest.raises(ValueError, match="size pin"):
            validation.public_model_arrays(cfg)
    decode.assert_not_called()


@pytest.mark.parametrize("hazard", [False, True])
def test_saved_survival_artifact_preflight_preserves_fitted_parametrisation(staged_survival, hazard, tmp_path):
    fixture = staged_survival
    if hazard:
        fixture.hazard_fixture()
    cfg = {"loss-name": fixture.manifest["loss-name"],
           "survival-config-b64": fixture.manifest["survival-config-b64"],
           "model-spec-b64": base64.b64encode(json.dumps({"kind": "sequential", "layers": [
               {"op": "linear", "out": "@out"}]}).encode()).decode(),
           "num-features": 2, "num-classes": 2, "num-labels": 2,
           "validation-model-track": "neural", "validation-task": "survival"}
    model = params.load_user_model(cfg, 2, cfg["loss-name"])
    artifact = tmp_path / "model.pt"
    torch.save(model.state_dict(), artifact)
    cfg["validation-model-path-b64"] = base64.b64encode(str(artifact).encode()).decode()
    arrays = validation.public_model_arrays(cfg)
    for actual, expected in zip(arrays, params.get_torch_params(model)):
        np.testing.assert_array_equal(actual, expected)
    # A different hazard K must fail while decoding the public predictor.
    if hazard:
        changed = survival_config(edges=[0., 10., 20.])
        cfg["survival-config-b64"] = base64.b64encode(json.dumps(changed).encode()).decode()
        with pytest.raises(RuntimeError, match="size mismatch"):
            validation.public_model_arrays(cfg)


def test_tabular_checkpoint_with_fourteen_tensors_keeps_numeric_parameter_order():
    import hashlib
    import io
    spec = {"kind": "sequential", "layers": [
        *[{"op": "linear", "out": 2} for _ in range(6)],
        {"op": "linear", "out": "@out"}]}
    model_config = {"loss-name": "bce_logits", "num-features": 2,
                    "num-classes": 2, "num-labels": 2}
    model = params.load_user_model(dict(model_config, **{"model-spec-b64":
        base64.b64encode(json.dumps(spec).encode()).decode()}), 2, "bce_logits")
    arrays = [np.full_like(value, (i+1)/100) for i, value in enumerate(params.get_torch_params(model))]
    assert len(arrays) == 14
    stream = io.BytesIO()
    # ZIP order is transport metadata. Deliberately put tensor10 before tensor2.
    np.savez(stream, **{str(i): arrays[i] for i in sorted(range(len(arrays)), key=str)})
    payload = stream.getvalue()
    manifest = {"role": "tabular_model", "model_config": model_config, "model_spec": spec,
                "checkpoint": {"sha256": hashlib.sha256(payload).hexdigest(), "size_bytes": len(payload)},
                "tensors": [{"name": str(i), "shape": list(value.shape), "dtype": "float32",
                             "sha256": hashlib.sha256(value.tobytes()).hexdigest()}
                            for i, value in enumerate(arrays)]}
    actual = checkpoints._decode_arrays(payload, manifest)
    for index in range(len(arrays)):
        np.testing.assert_array_equal(actual[index], arrays[index])


def test_segmentation_cv_accumulates_both_patient_folds_without_holdout_alias(registry):
    fields = resampling.cross_validation_manifest_fields(resampling.cross_validation_contract(2, "patient"))
    cfg = dict(pinned(registry), **fields)
    registry.node.update(fields, **{"n_samples": 3, "n_units": 3,
        "patient-id-canonicalization": "trim-utf8-v2", "cv-job-sha256": "a"*64})
    (registry.run / "manifest.json").write_text(json.dumps(registry.node))
    layout = validation.cross_validation_layout_from_config(cfg)
    assert layout["task"] == "segmentation"
    assert "holdout-validation-bins" not in cfg
    model = params.load_user_model(cfg, segmentation.FEATURE_DIM, "segmentation_bce_dice")
    params.set_torch_params(model, registry.arrays)
    X = np.zeros((3, segmentation.FEATURE_DIM), np.float32)
    y = np.ones((3, 2, 128, 128), np.float32)
    ids = np.array(["a", "b", "c"])
    with mock.patch.object(segmentation, "prepare_encoder", return_value=(None, "cpu")), \
            mock.patch.object(segmentation, "load_subject_tensors", return_value=(X, y, ids, 3)):
        for fold in (1, 2):
            returned = client_app._cross_validation_neural_accumulate(registry.context,
                cfg, {"loss_name": cfg["loss-name"]}, model, segmentation.FEATURE_DIM, fold)
            np.testing.assert_array_equal(returned, [[0.]])
    expected = validation.validation_sufficient_vector(y,
        validation.metric_predictions(model, X, y, cfg, layout), layout, unit_ids=ids)
    np.testing.assert_array_equal(client_app._load_complete_cv_sufficient(registry.context, layout), expected)


def test_survival_metric_times_preserve_adjacent_horizon_and_hazard_intervals(staged_survival):
    import pandas as pd
    from pathlib import Path
    fixture = staged_survival
    fixture.hazard_fixture()
    boundary = np.nextafter(5., np.inf)
    source_path = Path(fixture.temp.name) / "source.csv"
    source = pd.read_csv(source_path, dtype={"id": str}, keep_default_na=False)
    source["time"] = source["time"].astype(float)
    source.loc[0, "time"] = boundary
    source.to_csv(source_path, index=False)
    fixture.subjects["__survival_time"] = fixture.subjects["__survival_time"].astype(float)
    fixture.subjects.loc[0, ["__survival_time", "__survival_d_1", "__survival_d_2", "__survival_m_2"]] = [boundary, 0, 1, 1]
    fixture.subjects.to_csv(Path(fixture.temp.name) / "subjects.csv", index=False)
    X, y, ids, _ = task.load_survival_data(fixture.context, metric_targets=True)
    assert y.dtype == np.float64 and y[0, 0] == boundary and y[0, 0] > 5
    cfg = dict(fixture.manifest, **{"validation-task": "survival", "validation-survival-horizons": "[5]"})
    model = torch.nn.Linear(2, 3)
    with torch.no_grad():
        model.weight.zero_()
        model.bias.fill_(2.)
    layout = validation.layout_from_config(cfg)
    prediction = validation.metric_predictions(model, X, y, cfg, layout)
    logits = torch.full((1, 3), 2.)
    packed = survival.period_targets([boundary], [1.], [1.], fixture.cfg)
    expected_loss = float(survival.loss_factory(cfg["loss-name"], cfg)(logits, torch.as_tensor(packed)))
    assert prediction[0, 0] == pytest.approx(expected_loss)
    row = validation.validation_contributions(y[:1], prediction[:1], layout)[0]
    assert row[2] == pytest.approx((1.0-prediction[0, 1])**2)
