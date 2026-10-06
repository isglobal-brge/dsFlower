"""Explicit v3 source-unit and typed-identity constructors for public test fixtures."""
import hashlib
import json
from pathlib import Path
import numpy as np
import pytest
from dsflower_runner import seeding, canonical_units


def fixture_key(mechanism, config, privacy, round_index=1, *, public_arrays=(),
                private_arrays=(), unit_ids=None, execution_fingerprint=None, geometry=None):
    """Synthetic test source equals its arrays; production callers use real loaders."""
    config = dict(config)
    if 'run' not in config and 'pins' not in config:
        config = {'run': config}
    request = seeding.request_identity(mechanism, config, privacy, round_index,
        public_arrays=public_arrays, execution_fingerprint=execution_fingerprint)
    arrays = tuple(np.asarray(value) for value in private_arrays)
    n = len(arrays[0]) if arrays else (len(unit_ids) if unit_ids is not None else 0)
    X = arrays[0] if arrays else np.zeros((n, 0))
    y = arrays[1] if len(arrays) > 1 else np.zeros(n)
    units = canonical_units.canonicalize_arrays(X, y, unit_ids)
    tensors = tuple(value[units.row_permutation] for value in arrays)
    binding = seeding.bind_private_data(request, units, effective_tensors=tensors, geometry=geometry)
    return seeding.release_key(request, binding)


def tagged_arrays(X, y, ids=None):
    """Exact canonical synthetic-source metadata for mocked loader boundaries."""
    units = canonical_units.canonicalize_arrays(X, y, ids)
    p = units.row_permutation
    return (canonical_units.attach_units(np.asarray(X)[p], units),
            canonical_units.attach_units(np.asarray(y)[p], units),
            None if ids is None else np.asarray(ids, dtype=object)[p])


def source_sidecar(directory, manifest, frame, *, columns=None):
    """Serialize the public fixture source with the production closed sidecar ABI."""
    import pandas as pd
    from dsflower_runner import task
    target = manifest.get('target_column', [])
    targets = target if isinstance(target, list) else [target]
    columns = list(manifest.get('feature_columns', [])) + targets if columns is None else list(columns)
    patient = manifest.get('patient_column')
    def scalar(value):
        if value is None or value is pd.NA: return {'type': 'missing'}
        if isinstance(value, (bool, np.bool_)): return {'type': 'bool', 'value': bool(value)}
        if isinstance(value, (int, float, np.integer, np.floating)):
            if np.isnan(value): return {'type': 'nan'}
            if np.isposinf(value): return {'type': 'posinf'}
            if np.isneginf(value): return {'type': 'neginf'}
            return {'type': 'number', 'value': format(value, '.17g')}
        return {'type': 'utf8', 'value': str(value)}
    header = {'schema': 'dsflower-source-projection-v1', 'columns': columns, 'patient_column': patient}
    records = [header]
    for row in frame.to_dict('records'):
        records.append({'values': [scalar(row[column]) for column in columns],
                        'patient_id': None if patient is None else task._canonical_patient_id(row[patient])})
    directory = Path(directory)
    path = directory / 'source-projection.jsonl'
    path.write_text('\n'.join(json.dumps(record, separators=(',', ':')) for record in records)+'\n')
    path.chmod(0o600)
    staged = directory / manifest.get('data_file', manifest.get('samples_file'))
    manifest.update({'semantic-randomness-contract': seeding.SEMANTIC_CONTRACT,
                     'source_projection_file': path.name, 'source_projection_schema': header['schema'],
                     'source_projection_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                     'source_effective_sha256': hashlib.sha256(staged.read_bytes()).hexdigest()})
    return manifest


@pytest.fixture(autouse=True)
def fixture_node_secret(tmp_path, monkeypatch):
    """A real owner-only custodial key, isolated to each public test fixture."""
    secret = tmp_path / "fixture-node-secret"
    secret.write_text("73" * 32 + "\n")
    secret.chmod(0o600)
    monkeypatch.setenv("DSFLOWER_NODE_SECRET_FILE", str(secret))
