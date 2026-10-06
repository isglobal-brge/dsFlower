"""The real neural boundary selects a whole durable answer before optimization."""
import copy
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
import torch
from flwr.common import RecordDict

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'flower_app'))
from dsflower_runner import canonical_units as cu, client_app, seeding, segmentation


@pytest.fixture
def node(tmp_path, monkeypatch):
    secret = tmp_path / 'secret'
    secret.write_text('31' * 32)
    secret.chmod(0o600)
    monkeypatch.setenv('DSFLOWER_NODE_SECRET_FILE', str(secret))
    monkeypatch.delenv('DSFLOWER_NEIGHBOURHOOD_DIR', raising=False)
    monkeypatch.setenv('DSFLOWER_NEIGHBOURHOOD_K', '3')
    return tmp_path


def tagged(x, y, ids=None):
    units = cu.canonicalize_arrays(x, y, ids)
    order = units.row_permutation
    return cu.attach_units(np.asarray(x)[order], units), cu.attach_units(np.asarray(y)[order], units), (None if ids is None else np.asarray(ids)[order])


@pytest.mark.parametrize('family', ['tabular', 'structured', 'sequence', 'vision', 'survival'])
def test_neural_family_neighbours_skip_optimizer_and_preserve_round_identity(node, family):
    # Encoders/loaders are fixtures; canonical units, R/B, selection and durable
    # publication are production code. The optimizer sentinel detects any draw.
    x = np.arange(20, dtype=np.float32).reshape(10, 2)
    y = np.arange(10, dtype=np.float32) % 2
    cfg = {'loss-name': 'mse', 'model': family}
    pins = {'loss_name': 'mse', 'n_classes': 2, 'round_index': 1,
            'batch_size': 2, 'local_epochs': 1, 'num_rounds': 2}
    pcfg = {'epsilon': 1., 'delta': 1e-6, 'clipping_norm': 1.}
    if family == 'survival':
        y = np.column_stack((np.arange(10) + 1., np.ones(10), np.ones(10))).astype(np.float32)
        pins['loss_name'] = 'aft_weibull_nll'
        cfg.update({'loss-name': pins['loss_name'], 'survival-config': {
            'schema_version': 1, 'time_unit': 'days', 'time_origin': 'baseline',
            't_min': .01, 'horizon': 100., 'time_scale': 1.,
            'distribution': 'weibull', 'dispersion': 1.}})
    model = torch.nn.Linear(2, 1)
    context = SimpleNamespace(state=RecordDict())
    manifest = {'target_column': 'outcome'}
    if family == 'sequence':
        # Selected source rows include complete ordered sequences; effective
        # public model/loader reshaping stays outside neighbourhood distance.
        manifest['data_type'] = 'sequence'
    if family == 'structured':
        cfg['graph-json'] = '{"nodes":[],"edges":[]}'
    calls = []
    def fresh(*args, **kwargs):
        calls.append(kwargs['master'])
        return [np.array([len(calls), -0.], dtype=np.float32)], int(args[5])
    def run(n, round_index=1, reverse=False):
        xx, yy = x[:n], y[:n]
        if reverse:
            xx, yy = xx[::-1], yy[::-1]
        data = tagged(xx, yy)
        with (mock.patch.object(client_app, 'load_data', return_value=data),
              mock.patch.object(client_app, 'load_image_collection', return_value=data),
              mock.patch.object(client_app.task_module, 'load_survival_data', return_value=(*data, n))):
            return client_app._train_neural(context, cfg, pcfg, {**pins, 'round_index': round_index},
                                            copy.deepcopy(model), 2, family == 'vision')
    from dsflower_runner import vision
    with (mock.patch.object(client_app.task_module, '_load_manifest', return_value=manifest),
          mock.patch.object(client_app.task_module, 'assert_pinned_unit_count'),
          mock.patch.object(client_app, '_initial_model_hash', return_value='a' * 64),
          mock.patch.object(client_app.dp_harness, '_cached_noise_multiplier', return_value=1.5),
          mock.patch.object(client_app, '_dp_fit', side_effect=fresh),
          mock.patch.object(vision, 'prepare_backbone', return_value=(object(), 32, False, 'cpu')),
          mock.patch.object(vision, 'extract_features_from_paths', side_effect=lambda _, paths, *a, **k: np.asarray(paths))):
        first = run(10)
        for current in (run(10), run(9), run(8), run(10, reverse=True)):
            assert current[0][0].tobytes() == first[0][0].tobytes()
            assert current[1] == first[1]
        assert len(calls) == 1
        assert run(7)[0][0].tobytes() != first[0][0].tobytes()
        assert len(calls) == 2
        run(10, round_index=2)
        assert len(calls) == 3


def test_segmentation_whole_patient_and_complete_postprocessing_payload(node):
    from dsflower_runner.segmentation import FEATURE_DIM
    x = np.arange(8 * FEATURE_DIM, dtype=np.float32).reshape(8, FEATURE_DIM)
    y = np.zeros((8, 2, 128, 128), dtype=np.float32)
    ids = np.array(['p%d' % i for i in range(8)])
    pins = {'loss_name': 'segmentation_bce_dice', 'n_classes': 2,
            'round_index': 1, 'batch_size': 2, 'local_epochs': 1, 'num_rounds': 1}
    # Reuse the public admitted segmentation contract exercised by its own suite.
    from test_segmentation_contracts import config, decoder
    cfg = config()
    manifest = {'dp-unit': 'patient', 'target_column': 'mask'}
    model = decoder()
    context = SimpleNamespace(state=RecordDict())
    count = 0
    def fresh(*a, **kw):
        nonlocal count
        count += 1
        return [np.full((2, 3), count, dtype=np.float32)], 8
    def run(changes):
        xx = x.copy()
        xx[:changes] += 1
        data = tagged(xx, y, ids)
        with mock.patch.object(segmentation, 'load_subject_tensors', return_value=(*data, 8)):
            return client_app._train_segmentation(context, cfg,
                {'epsilon': 1., 'delta': 1e-6, 'clipping_norm': 1.}, pins, copy.deepcopy(model))
    with (mock.patch.object(client_app.task_module, '_load_manifest', return_value=manifest),
          mock.patch.object(client_app, '_initial_model_hash', return_value='b' * 64),
          mock.patch.object(segmentation, 'prepare_encoder', return_value=(object(), 'cpu')),
          mock.patch.object(client_app.dp_harness, '_cached_noise_multiplier', return_value=1.5),
          mock.patch.object(client_app, '_dp_fit', side_effect=fresh)):
        first = run(0)
        assert run(1)[0][0].tobytes() == first[0][0].tobytes()
        assert run(2)[0][0].tobytes() == first[0][0].tobytes()
        assert count == 1
        assert run(3)[0][0].tobytes() != first[0][0].tobytes()
        assert count == 2
