"""Public initialization and structural model identity, without private inputs."""
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'flower_app'))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'flower_app' / 'dsflower_runner'))
from dsflower_runner import initialisation as init, model_spec, params, segmentation, server_app


def cfg(spec=None, **changes):
    spec = spec or {'layers': [{'op': 'linear', 'out': 4}, {'op': 'relu'}, {'op': 'linear'}]}
    result = {'model-spec-b64': base64.b64encode(json.dumps(spec).encode()).decode(),
              'num-features': 6, 'num-classes': 2, 'loss-name': 'bce_logits'}
    result.update(changes)
    return result


def arrays(config):
    return params.get_torch_params(server_app._build_initial_model(config))


def exact(left, right):
    assert [(a.shape, a.dtype.str, a.tobytes()) for a in left] == [
        (a.shape, a.dtype.str, a.tobytes()) for a in right]


def test_default_and_explicit_defaults_share_seed():
    first = cfg()
    spec = model_spec.canonical_spec(first)
    second = cfg(spec)
    assert init.initialisation_spec_sha256(first) == init.initialisation_spec_sha256(second)
    exact(arrays(first), arrays(second))


def test_init_ignores_session_policy_fold_and_global_rng():
    first = cfg()
    second = dict(first, **{'run-id': 'other', 'cv-fold': 3, 'epsilon': 0.1,
                            'learning-rate': 2., 'strategy-name': 'fedadam', 'results-dir': '/elsewhere'})
    expected = arrays(first)
    random.seed(30); np.random.seed(9); torch.manual_seed(150)
    python_state, numpy_state, torch_state = random.getstate(), np.random.get_state(), torch.get_rng_state()
    exact(expected, arrays(second))
    assert python_state == random.getstate()
    observed = np.random.get_state()
    assert numpy_state[0] == observed[0] and np.array_equal(numpy_state[1], observed[1])
    assert numpy_state[2:] == observed[2:]
    assert torch.equal(torch_state, torch.get_rng_state())


def test_same_spec_initial_arrays_match_across_fresh_processes():
    runner = str(Path(__file__).resolve().parents[2] / 'flower_app')
    script = ('import json; from dsflower_runner.initialisation import build_initial_model,hash_public_arrays;'
              'from dsflower_runner.params import get_torch_params;'
              'print(hash_public_arrays(get_torch_params(build_initial_model(json.loads(' + repr(json.dumps(cfg())) + ')))))')
    env = dict(os.environ, PYTHONPATH=runner)
    outputs = [subprocess.check_output([sys.executable, '-c', script], env=env) for _ in range(2)]
    assert outputs[0] == outputs[1]


def test_cv_each_fold_starts_at_same_clean_initial_model():
    config = cfg()
    expected = arrays(config)
    for fold in (1, 2, 3):
        model, record = server_app._cross_validation_initial_arrays(config, fold)
        exact(expected, record.to_numpy_ndarrays())
        for p in model.parameters():
            p.data.add_(1)


@pytest.mark.parametrize('kind', ['lstm', 'gru'])
def test_sequence_public_init_replays(kind):
    config = cfg({'layers': [{'op': 'reshape', 'shape': [2, 3]},
                             {'op': kind, 'hidden': 4}, {'op': 'linear'}]})
    exact(arrays(config), arrays(config))


@pytest.mark.parametrize('loss', ['aft_weibull_nll', 'aft_lognormal_nll', 'discrete_hazard_nll'])
def test_survival_public_init_replays(loss):
    survival = {'schema_version': 1, 'time_unit': 'days', 'time_origin': 'baseline',
                't_min': 1., 'horizon': 20.}
    if loss == 'discrete_hazard_nll':
        survival['edges'] = [0., 5., 10., 20.]
    else:
        survival.update(time_scale=5., distribution=loss.split('_')[1], dispersion=1.)
    config = cfg(**{'loss-name': loss, 'survival-config-b64': base64.b64encode(json.dumps(survival).encode()).decode()})
    exact(arrays(config), arrays(config))


def test_vision_head_public_init_replays_without_encoder_access():
    from dsflower_runner import vision
    config = cfg(**{'data-kind': 'image', 'backbone': 'resnet18',
                    'vision-extractor-profile': vision.extractor_profile_for('resnet18'),
                    'image-size': 128, 'num-features': 512})
    with mock.patch.object(vision, 'verified_backbone_bytes', side_effect=AssertionError('encoder IO')):
        exact(arrays(config), arrays(config))


def test_graph_labels_topological_order_and_defaults_are_nuisance():
    graph = {'kind': 'graph', 'nodes': [
        {'name': 'a', 'op': 'linear', 'out': 3, 'in': ['@in']},
        {'name': 'b', 'op': 'linear', 'out': 3, 'in': ['@in']},
        {'name': 'sum', 'op': 'add', 'in': ['a', 'b']},
        {'name': 'out', 'op': 'linear', 'in': ['sum']}], 'output': 'out'}
    renamed = copy.deepcopy(graph)
    rename = {'a': 'left', 'b': 'right', 'sum': 'combined', 'out': 'head'}
    for node in renamed['nodes']:
        node['name'] = rename[node['name']]
        node['in'] = [rename.get(v, v) for v in node['in']]
        if node['op'] == 'linear': node['bias'] = True
    renamed['output'] = 'head'
    renamed['nodes'][:2] = reversed(renamed['nodes'][:2])
    first, second = cfg(graph), cfg(renamed)
    assert model_spec.canonical_spec(first) == model_spec.canonical_spec(second)
    exact(arrays(first), arrays(second))
    swapped = copy.deepcopy(graph)
    swapped['nodes'][2]['in'].reverse()
    # Identical branches can produce equal canonical structures; make branches
    # distinguishable before testing ordered operands.
    graph['nodes'][1]['bias'] = False
    swapped['nodes'][1]['bias'] = False
    assert model_spec.canonical_spec(cfg(graph)) != model_spec.canonical_spec(cfg(swapped))


def test_canonicalization_consumes_no_rng_and_rejects_unreachable_or_unknown():
    state = torch.get_rng_state()
    model_spec.canonical_spec(cfg())
    assert torch.equal(state, torch.get_rng_state())
    for spec in ({'layers': [{'op': 'linear', 'ignored': 1}]},
                 {'kind': 'graph', 'output': 'out', 'nodes': [
                     {'name': 'unused', 'op': 'linear', 'in': ['@in']},
                     {'name': 'out', 'op': 'linear', 'in': ['@in']}]}):
        with pytest.raises(ValueError): model_spec.canonical_spec(cfg(spec))
    with pytest.raises(ValueError, match='cap'):
        model_spec.canonical_spec(cfg({'layers': [{'op': 'linear', 'out': 8193}, {'op': 'linear'}]}))


def test_segmentation_decoder_explicit_defaults_share_admission_and_initialisation():
    from test_segmentation_contracts import config
    first = config()
    normalized = model_spec.canonical_spec(first)
    for layer in normalized['layers']:
        if layer['op'] == 'upsample': layer['mode'] = 'nearest'
    second = dict(first, **{'model-spec-b64': cfg(normalized)['model-spec-b64']})
    segmentation.validate_decoder_spec(normalized)
    exact(arrays(first), arrays(second))


@pytest.mark.parametrize('field,value', [('mode', 'bilinear'), ('unknown', 1)])
def test_segmentation_decoder_unknown_or_non_nearest_fields_fail_closed(field, value):
    spec = segmentation.decoder_spec('pointwise')
    spec['layers'][-1][field] = value
    with pytest.raises(ValueError): segmentation.validate_decoder_spec(spec)


def test_hook_initial_arrays_use_isolated_supported_rngs():
    from dsflower_runner import tier2_lib
    config = {'dp-track': 'egress', 'user-module': 'public_hook', 'num-features': 2,
              'num-server-rounds': 1, 'num-classes': 2, 'task-type': 'classification',
              'app-params-b64': base64.b64encode(b'{}').decode(),
              'app-params-sha256': hashlib.sha256(b'{}').hexdigest()}
    module = SimpleNamespace(initial_arrays=lambda c,n: [np.array([
        random.random(), np.random.random(), torch.rand(1).item()])])
    with mock.patch.object(tier2_lib, 'load_user_module', return_value=module):
        first = server_app._initial_arrays(config, 'egress')[1].to_numpy_ndarrays()
        state = random.getstate(), np.random.get_state(), torch.get_rng_state()
        second = server_app._initial_arrays(config, 'egress')[1].to_numpy_ndarrays()
    exact(first, second)
    assert random.getstate() == state[0]
    assert np.array_equal(np.random.get_state()[1], state[1][1])
    assert torch.equal(torch.get_rng_state(), state[2])


def test_public_seed_formula_and_array_content_binding():
    config = cfg()
    digest = init.initialisation_spec_sha256(config)
    expected = int.from_bytes(hashlib.sha256(b'dsflower/public-init/v1\x00' + bytes.fromhex(digest)).digest()[:8], 'big') & ((1 << 63) - 1)
    assert init.public_initialisation_seed(config) == expected
    weights = arrays(config)
    changed = [a.copy() for a in weights]; changed[0].flat[0] += 1
    assert init.hash_public_arrays(weights) != init.hash_public_arrays(changed)


def test_legacy_graph_checkpoint_maps_original_keys_bijectively_after_hash_checks():
    import io
    from dsflower_runner import segmentation_checkpoints as checkpoints
    graph = {'kind': 'graph', 'output': 'head', 'nodes': [
        {'name': 'second', 'op': 'linear', 'out': 3, 'in': ['@in']},
        {'name': 'first', 'op': 'linear', 'out': 2, 'in': ['@in']},
        {'name': 'join', 'op': 'concat', 'in': ['first', 'second']},
        {'name': 'head', 'op': 'linear', 'in': ['join']}]}
    manifest = {'role': 'tabular_model', 'model_spec': graph,
                'model_config': {'loss-name': 'bce_logits', 'num-features': 6,
                                 'num-classes': 2, 'num-labels': 2}}
    legacy = [np.full(shape, i + 1, np.float32) for i, shape in enumerate(
        checkpoints._manifest_tensor_shapes(manifest))]
    stream = io.BytesIO()
    np.savez(stream, **{str(i): value for i, value in enumerate(legacy)})
    payload = stream.getvalue()
    manifest['checkpoint'] = {'size_bytes': len(payload), 'sha256': hashlib.sha256(payload).hexdigest()}
    manifest['tensors'] = [{'name': str(i), 'shape': list(value.shape), 'dtype': 'float32',
                           'sha256': hashlib.sha256(value.tobytes()).hexdigest()}
                          for i, value in enumerate(legacy)]
    actual = checkpoints._decode_arrays(payload, manifest)
    # The graph visits first then second, while the legacy registration reversed them.
    assert checkpoints._manifest_tensor_order(manifest) == [2, 3, 0, 1, 4, 5]
    exact(actual, [legacy[i] for i in [2, 3, 0, 1, 4, 5]])
    model = init.build_initial_model(cfg(graph))
    params.set_torch_params(model, actual)
    bad = copy.deepcopy(manifest)
    bad['tensors'][0]['sha256'] = '0' * 64
    with pytest.raises(ValueError, match='digest'):
        checkpoints._decode_arrays(payload, bad)


@pytest.mark.parametrize('wire', [b'{"layers":[],"layers":[]}',
                                  b'{"layers":[{"op":"linear","out":NaN}]}'])
def test_model_json_duplicate_keys_and_nonfinite_constants_reject(wire):
    with pytest.raises(ValueError, match='decode'):
        model_spec.canonical_spec({'model-spec-b64': base64.b64encode(wire).decode(), 'num-features': 1})
