"""Actual initial content survives Context restart without reconstructing weights."""
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import numpy as np
import pytest
from flwr.common import RecordDict

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app"))
from dsflower_runner import client_app, seeding


def context(directory):
    return SimpleNamespace(node_config={"manifest-dir": str(directory)}, state=RecordDict())


def arrays(value=7.):
    # These are analyst-chosen values, not a spec-seeded expected model.
    return [np.full((2, 3), value, dtype=np.float32)]


def test_initial_content_survives_fresh_context_and_preserves_actual_arrays(tmp_path):
    original = arrays()
    expected = seeding.public_array_identity(original)["sha256"]
    assert client_app._initial_model_hash(context(tmp_path), original, 1) == expected
    assert client_app._initial_model_hash(context(tmp_path), arrays(13.), 2) == expected
    assert client_app._initial_model_hash(context(tmp_path), original, 1) == expected
    np.testing.assert_array_equal(original[0], arrays()[0])


def test_different_initial_model_rejects_same_coordinate_but_is_allowed_in_new_run(tmp_path):
    client_app._initial_model_hash(context(tmp_path), arrays(), 1)
    with pytest.raises(RuntimeError, match="initial model changed"):
        client_app._initial_model_hash(context(tmp_path), arrays(13.), 1)
    other = tmp_path / "other-run"
    other.mkdir()
    assert client_app._initial_model_hash(context(other), arrays(13.), 1) == seeding.public_array_identity(arrays(13.))["sha256"]


@pytest.mark.parametrize("corruption", ["missing", "malformed", "extra-field", "oversized", "wrong-mode", "symlink"])
def test_existing_initial_content_pin_fails_closed_and_is_never_replaced(tmp_path, corruption):
    client_app._initial_model_hash(context(tmp_path), arrays(), 1)
    path = tmp_path / "dsflower-initial-model-v3-ordinary.json"
    if corruption == "missing":
        path.unlink()
    elif corruption == "malformed":
        path.write_text("invalid")
    elif corruption == "extra-field":
        value = json.loads(path.read_text()); value["extra"] = 1
        path.write_text(json.dumps(value))
    elif corruption == "oversized":
        path.write_text(path.read_text() + " " * 300)
    elif corruption == "wrong-mode":
        if os.name != "posix": pytest.skip("POSIX mode guard")
        path.chmod(0o644)
    else:
        target = tmp_path / "real-pin"
        path.rename(target)
        try: path.symlink_to(target)
        except OSError: pytest.skip("symlink unavailable")
    before = path.read_bytes() if path.exists() else None
    with pytest.raises(RuntimeError, match="initial model content pin"):
        client_app._initial_model_hash(context(tmp_path), arrays(13.), 2)
    assert (path.read_bytes() if path.exists() else None) == before


def test_fold_pins_are_separate_and_context_cannot_override_durable_initial_hash(tmp_path):
    first = context(tmp_path)
    a = client_app._initial_model_hash(first, arrays(), 1, fold=1)
    b = client_app._initial_model_hash(first, arrays(13.), 1, fold=2)
    assert a != b
    first.state["dsflower-initial-model-v3-1"]["sha256"] = b
    with pytest.raises(RuntimeError, match="initial model changed"):
        client_app._initial_model_hash(first, arrays(), 2, fold=1)
