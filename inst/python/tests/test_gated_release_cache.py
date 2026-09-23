"""Durable replay at the Flower boundary with a real nondeterministic Hook.

Sandbox attestation and sleeping are replaced for this local test; the child,
private CSV loading, v2 identity, DP mechanism, ledger, cache and Flower arrays
are real. These tests do not assert fixed-duration or deployment isolation.
"""

import base64
import hashlib
import io
import json
import multiprocessing
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest
from flwr.common import ArrayRecord, ConfigRecord, Message, RecordDict

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app"))
from dsflower_runner import client_app, release_cache, release_guard, task, tier2_lib


CAPS = {"subprocess": True, "net_lock": True, "fs_isolation": True, "bwrap": None}
HOOK = """import os
import numpy as np
def initial_arrays(cfg, input_dim):
    return [np.zeros(input_dim, dtype=np.float32)]
def local_update(global_arrays, X, y, cfg):
    return [np.asarray(a) + np.frombuffer(os.urandom(a.size * 8), dtype=np.uint64)
            .reshape(a.shape).astype(np.float64) / 2**64 for a in global_arrays]
"""


def _setup_run(root, token="a", *, selection="x", data=1.0, hook=HOOK):
    root = Path(root).resolve()
    package = root / "apps" / "random_hook"
    package.mkdir(parents=True, exist_ok=True)
    (package / "__init__.py").write_text(hook)
    digest = hashlib.sha256(b"__init__.py\n" + hook.encode() + b"\0").hexdigest()
    secret = root / "privacy"
    secret.mkdir(mode=0o700, exist_ok=True)
    (secret / "noise_root").write_text((b"s" * 32).hex())
    (secret / "noise_root").chmod(0o600)
    directory = root / ("run_" + token * 32)
    directory.mkdir(mode=0o700, exist_ok=True)
    (directory / "data.csv").write_text("x,x_alias,y\n%s,%s,0\n2,2,1\n" % (data, data))
    (directory / "pinned_packages.json").write_text(json.dumps({"random_hook": digest}))
    manifest = {
        "run_token": directory.name, "dp-track": "egress", "user-module": "random_hook",
        "privacy-adjacency": "replace_one", "privacy-policy-sha256": "1" * 64,
        "privacy-epsilon": 1.0, "privacy-delta": 1e-5, "privacy-clipping_norm": 1.0,
        "privacy-hook_enabled": 1, "privacy-sample_aggregate": 0,
        "privacy-egress_timeout": 30, "privacy-egress_time_pad": 35,
        "num-server-rounds": 2, "app-params-b64": base64.b64encode(b"{}").decode(),
        "app-params-sha256": hashlib.sha256(b"{}").hexdigest(),
        "task-type": "classification", "num-classes": 2, "num-features": 1,
        "data_file": "data.csv", "data_format": "csv", "data_type": "tabular",
        "feature_columns": [selection], "target_column": "y", "dp-unit": "row",
        "request-source": {"source": "table", "data_symbol": "cohort"},
        "n_samples": 2, "n_units": 2,
    }
    (directory / "manifest.json").write_text(json.dumps(manifest))
    return directory


def _context(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    return SimpleNamespace(node_config={"manifest-dir": str(directory)},
                           run_config=manifest, state=RecordDict())


def _message(round_index=1, values=None):
    values = np.zeros(8, dtype=np.float32) if values is None else values
    return Message(content=RecordDict({
        "arrays": ArrayRecord(numpy_ndarrays=[values]),
        "config": ConfigRecord({"server-round": round_index}),
    }), dst_node_id=1, message_type="train")


def _env(root, directory):
    return {"DSFLOWER_RELEASE_CACHE_DIR": str(root / "cache"),
            "DSFLOWER_RELEASE_CACHE_BYTES": str(1024**3),
            "DSFLOWER_NODE_SECRET_FILE": str(root / "privacy" / "noise_root"),
            "DSFLOWER_PINNED_APP_DIR": str(root / "apps"),
            "DSFLOWER_MANIFEST_DIR": str(directory)}


def _request(root, directory, round_index=1, context=None):
    context = _context(directory) if context is None else context
    outcomes = []
    isolated = tier2_lib._run_isolated

    def capture_child(*args, **kwargs):
        value = isolated(*args, **kwargs)
        outcomes.append(value)
        return value

    with (mock.patch.dict(os.environ, _env(root, directory)),
          mock.patch.object(tier2_lib, "hook_execution_caps", return_value=CAPS),
          mock.patch.object(tier2_lib, "pad_hook_release") as pad,
          mock.patch.object(tier2_lib, "_run_isolated", side_effect=capture_child) as child):
        reply = client_app.train(_message(round_index), context)
    if (root / "apps" / "random_hook" / "__init__.py").read_text() == HOOK:
        # A failed child also produces a valid noised-zero release, so assert
        # this fixture really exercised fresh application randomness.
        for value in outcomes:
            assert value is not None and len(value) == 1
            assert np.any(value[0] != 0)
    arrays = [(a.dtype, tuple(a.shape), a.data) for a in reply.content["arrays"].values()]
    return arrays, dict(reply.content["metrics"]), child.call_count, pad.call_count


def _process_request(root, directory, round_index, start, results):
    try:
        if start is not None:
            start.wait(30)
        results.put(("ok", _request(Path(root), Path(directory), round_index)))
    except BaseException as exc:
        results.put(("error", repr(exc)))


def _spawn_requests(root, requests):
    process_context = multiprocessing.get_context("spawn")
    start = process_context.Event()
    results = process_context.Queue()
    processes = [process_context.Process(target=_process_request, args=(
        str(root), str(directory), round_index, start, results))
        for directory, round_index in requests]
    try:
        for process in processes:
            process.start()
        start.set()
        values = [results.get(timeout=90) for _ in processes]
        for process in processes:
            process.join(30)
            assert process.exitcode == 0
        assert all(status == "ok" for status, _ in values), values
        return [result for _, result in values]
    finally:
        for process in processes:
            if process.is_alive():
                process.kill()
                process.join(10)
        results.close()


@pytest.fixture
def run(tmp_path):
    root = tmp_path.resolve()
    return root, _setup_run(root)


def test_nondeterministic_hook_replays_earlier_round_after_process_restart(run):
    root, directory = run
    first, = _spawn_requests(root, [(directory, 1)])
    assert first[1] == {"num-examples": 1, "hook-executed": 1}
    assert first[2:] == (1, 1)
    second = _request(root, directory, 2)
    assert second[2] == 1
    replay, = _spawn_requests(root, [(directory, 1)])
    assert replay[:2] == first[:2]
    assert replay[2:] == (0, 1)


def test_retained_release_replays_across_new_run_tokens_and_paths(run):
    root, directory = run
    first = _request(root, directory)
    other = _setup_run(root, "b")
    replay = _request(root, other)
    assert first[:2] == replay[:2]
    assert (first[2], replay[2]) == (1, 0)


def test_first_release_and_retry_encode_identically_for_fortran_arrays(run):
    root, directory = run
    released = np.asfortranarray(np.arange(8, dtype=np.float32).reshape(2, 4))
    with (mock.patch.dict(os.environ, _env(root, directory)),
          mock.patch.object(tier2_lib, "hook_execution_caps", return_value=CAPS),
          mock.patch.object(tier2_lib, "pad_hook_release"),
          mock.patch.object(tier2_lib, "gated_local_update", return_value=[released]) as hook):
        replies = [client_app.train(
            _message(values=np.zeros((2, 4), dtype=np.float32)), _context(directory))
            for _ in range(2)]
    hook.assert_called_once()
    assert dict(replies[0].content["metrics"]) == dict(replies[1].content["metrics"])
    assert [a.data for a in replies[0].content["arrays"].values()] == [
        a.data for a in replies[1].content["arrays"].values()]
    np.testing.assert_array_equal(replies[0].content["arrays"].to_numpy_ndarrays()[0], released)


@pytest.mark.parametrize("change", ["selection", "data"])
def test_changed_identity_rejects_committed_coordinate_and_misses_in_new_run(run, change):
    root, directory = run
    context = _context(directory)
    first = _request(root, directory, context=context)
    assert first[2] == 1
    # Deliberately supply an old in-memory reply too: it must not short-circuit
    # the private data/selection identity check.
    claim = release_guard.claim_release(context, _message())
    client_app._cache_reply(context, claim, [np.zeros(8, np.float32)], hook_executed=True)
    kwargs = {"selection": "x_alias"} if change == "selection" else {"data": 3.0}
    _setup_run(root, **kwargs)
    rejected = _request(root, directory, context=context)
    assert rejected[1].get("execution-unavailable") == 1
    assert rejected[2] == 0
    changed_run = _setup_run(root, "b", **kwargs)
    changed = _request(root, changed_run)
    assert changed[1] == first[1]
    assert changed[2] == 1
    assert changed[0] != first[0]


@pytest.mark.parametrize("same_run", [True, False])
def test_identical_concurrent_requests_execute_one_hook(run, same_run):
    root, directory = run
    other = directory if same_run else _setup_run(root, "b")
    first, second = _spawn_requests(root, [(directory, 1), (other, 1)])
    assert first[1] == second[1] == {"num-examples": 1, "hook-executed": 1}
    assert first[:2] == second[:2]
    assert first[2] + second[2] == 1
    assert first[3] == second[3] == 1


@pytest.mark.parametrize("body", ["raise RuntimeError('private crash')",
                                 "return [np.zeros(999)]"])
def test_noised_zero_outcome_is_durably_replayed(tmp_path, body):
    root = tmp_path.resolve()
    hook = "import numpy as np\ndef initial_arrays(cfg, d): return [np.zeros(d)]\n" \
           "def local_update(g, X, y, cfg): " + body + "\n"
    directory = _setup_run(root, hook=hook)
    first = _request(root, directory)
    replay = _request(root, directory)
    assert first[1] == {"num-examples": 1, "hook-executed": 1}
    assert first[:2] == replay[:2]
    assert (first[2], replay[2]) == (1, 0)
    assert np.any(np.load(io.BytesIO(first[0][0][2]), allow_pickle=False))


def test_capacity_rejection_precedes_private_data_and_hook(run):
    root, directory = run
    context = _context(directory)
    environment = _env(root, directory)
    environment["DSFLOWER_RELEASE_CACHE_BYTES"] = "4096"
    with (mock.patch.dict(os.environ, environment),
          mock.patch.object(tier2_lib, "hook_execution_caps", return_value=CAPS),
          mock.patch.object(client_app, "load_data") as load,
          mock.patch.object(tier2_lib, "gated_local_update") as hook):
        reply = client_app.train(_message(), context)
    assert reply.content["metrics"].get("public-preflight-unavailable") == 1
    load.assert_not_called()
    hook.assert_not_called()


def test_disabled_hook_retries_remain_public_without_cache_or_private_work(run):
    root, directory = run
    context = _context(directory)
    with (mock.patch.dict(os.environ, _env(root, directory)),
          mock.patch.object(tier2_lib, "hook_execution_caps", return_value=None),
          mock.patch.object(client_app, "load_data") as load,
          mock.patch.object(release_cache.ReleaseCache, "from_env") as cache):
        for _ in range(2):
            reply = client_app.train(_message(), context)
            assert dict(reply.content["metrics"]) == {
                "num-examples": 1, "hook-executed": 0, "public-preflight-unavailable": 1}
            np.testing.assert_array_equal(
                reply.content["arrays"].to_numpy_ndarrays()[0], np.zeros(8, np.float32))
    load.assert_not_called()
    cache.assert_not_called()


@pytest.mark.parametrize("key", ["release-cache-dir", "release_cache_bytes", "releaseCacheSize",
                                 "deadline", "gatedDeadline"])
def test_cache_controls_rejected_in_flower_config_manifest_and_nested_params(run, key):
    _, directory = run
    context = _context(directory)
    message = _message()
    message.content["config"][key] = "analyst"
    with pytest.raises(RuntimeError, match="administrator-only"):
        release_guard.claim_release(context, message)
    context.run_config[key] = "analyst"
    with pytest.raises(ValueError, match="administrator-only"):
        task.load_pinned_run_config(context)
    context.run_config.pop(key)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest[key] = "analyst"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="administrator-only"):
        task.load_pinned_run_config(context)
    with pytest.raises(ValueError):
        tier2_lib._sanitize_cfg({"app_params": {"nested": {key: "analyst"}},
                                 "round_index": 1, "num_rounds": 2,
                                 "task": "classification", "num_classes": 2})
