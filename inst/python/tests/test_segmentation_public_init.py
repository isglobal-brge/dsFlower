"""Public checkpoint admission, exact loading and provenance at the DP boundary."""

import base64
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest import mock
import zipfile

import numpy as np
import pytest
import torch
from flwr.common import ArrayRecord, Message, RecordDict

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "flower_app"))
from dsflower_runner import (client_app, dp_harness, params, seeding, segmentation as seg,
                             segmentation_checkpoints as checkpoints,
                             server_app, task)

torch.set_num_threads(1)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def public_config():
    return {"task-type": "segmentation", "loss-name": "segmentation_bce_dice",
            "data-kind": "image", "backbone": seg.BACKBONE,
            "vision-extractor-profile": seg.PROFILE, "num-features": seg.FEATURE_DIM,
            "num-classes": 2, "image-size": 128, "segmentation-alpha": .5,
            "segmentation-smooth": 1.0, "mask-vocabulary": "0,255",
            "segmentation-selection": seg.SELECTION,
            "segmentation-preprocessing": seg.PREPROCESSING,
            "segmentation-checkpoint-sha256": seg.CHECKPOINT_SHA256,
            "segmentation-output-shape": "1,128,128",
            "model-spec-b64": base64.b64encode(json.dumps(
                seg.decoder_spec("narrow")).encode()).decode()}


@pytest.fixture
def registry(tmp_path, monkeypatch):
    state = tmp_path / "protected"
    state.mkdir(mode=0o700)
    root = state / "checkpoint-cache"
    directory = tmp_path / "synthetic-v1"
    directory.mkdir(parents=True, mode=0o700)
    shapes = [(8, 128, 3, 3), (8,), (4, 8, 3, 3), (4,), (1, 4, 1, 1), (1,)]
    arrays = [np.full(shape, (i + 1) / 100., np.float32)
              for i, shape in enumerate(shapes)]
    stream = io.BytesIO()
    np.savez(stream, **{str(i): a for i, a in enumerate(arrays)})

    def artifact(name, data):
        (directory / name).write_bytes(data)
        return {"file": name, "size_bytes": len(data), "sha256": sha(data)}

    checkpoint = artifact("checkpoint.npz", stream.getvalue())
    encoder_bytes = b"synthetic frozen encoder"
    monkeypatch.setattr(checkpoints, "_ENCODER_SIZE", len(encoder_bytes))
    monkeypatch.setattr(seg, "CHECKPOINT_SHA256", sha(encoder_bytes))
    encoder = artifact("encoder.pth", encoder_bytes)
    evidence = {key: artifact(key + ".txt", ("public fixture " + key).encode())
                for key in ("protocol", "audit", "licence", "mirror_metadata")}
    dataset = {"dataset": "synthetic", "release": "v1", "sha256": "a" * 64,
               "dataset_url": "https://example.invalid/synthetic", "attribution": "dsFlower tests",
               "licence": "synthetic test fixture", "licence_sha256": evidence["licence"]["sha256"],
               "metadata_sha256": evidence["mirror_metadata"]["sha256"]}
    evidence["provenance"] = artifact("provenance.json", json.dumps(dataset).encode())
    original = {"dataset": "synthetic", "dataset_sha256": dataset["sha256"],
                "decoder": "narrow", "privacy": "public_nonprivate",
                "checkpoint_sha256": checkpoint["sha256"],
                "tensor_sha256": [sha(a.tobytes()) for a in arrays],
                "encoder_sha256": seg.CHECKPOINT_SHA256,
                **{key + "_sha256": evidence[key]["sha256"]
                   for key in ("protocol", "provenance", "audit")}}
    evidence["original_manifest"] = artifact("original.json", json.dumps(original).encode())
    manifest = {"schema_version": checkpoints.SCHEMA,
                "checkpoint_id": directory.name,
                "model_id": "pytorch_resnet18_segmentation", "decoder": "narrow",
                "feature_contract": seg.PROFILE, "encoder_sha256": seg.CHECKPOINT_SHA256,
                "dataset": dataset, "licence": {"declaration": dataset["licence"],
                                                 "scope": "synthetic tests only"},
                "checkpoint": checkpoint, "evidence": evidence, "encoder": encoder,
                "role": "segmentation_decoder",
                "decoder_spec_sha256": sha(checkpoints._canonical(seg.decoder_spec("narrow"))),
                "pretraining_protocol_sha256": evidence["protocol"]["sha256"],
                "creation": {"creator": "dsFlower synthetic tests", "created_at": "2026-09-23"},
                "tensors": [{"name": str(i), "shape": list(a.shape), "dtype": "float32",
                             "sha256": sha(a.tobytes())} for i, a in enumerate(arrays)]}
    wire = json.dumps(manifest).encode()
    (directory / "manifest.json").write_bytes(wire)
    admitted = checkpoints.admit_bundle(directory, root)
    local_directory = directory
    directory = Path(admitted.pop("snapshot_directory"))
    admitted.pop("bundle_sha256")
    summary = admitted
    provenance = summary["provenance"]
    cfg = dict(public_config(), **{checkpoints.INIT_KEY: "client"})
    node = dict(cfg, data_type="image", **{
        "run_token": "run_" + "a" * 32, "dp-unit": "patient", "dp-track": "neural",
        checkpoints.MANIFEST_KEY: provenance["manifest_sha256"],
        checkpoints.CHECKPOINT_KEY: checkpoint["sha256"],
        checkpoints.PROVENANCE_KEY: summary,
        checkpoints.ORIGIN_KEY: "analyst-declared", checkpoints.POLICY_KEY: "analyst_or_resource",
        checkpoints.DIRECTORY_KEY: str(directory),
        checkpoints.ENCODER_KEY: manifest["encoder_sha256"],
        checkpoints.VERSION_KEY: checkpoints.IDENTITY_VERSION})
    run = tmp_path / "run"
    run.mkdir()
    (run / "manifest.json").write_text(json.dumps(node))
    context = SimpleNamespace(run_config=cfg, node_config={"manifest-dir": str(run)},
                              state=RecordDict())
    monkeypatch.setenv("DSFLOWER_NODE_SECRET_FILE", str(state / "noise_root"))
    monkeypatch.setattr(seeding, "_node_secret", lambda: b"s" * 32)
    monkeypatch.setattr(seg, "verified_encoder_bytes", lambda cfg=None: b"synthetic encoder")
    return SimpleNamespace(root=root, directory=directory, manifest=manifest,
                           provenance=provenance, summary=summary, local_directory=local_directory, arrays=arrays, cfg=cfg,
                           node=node, context=context, run=run)


def verify(fixture):
    result = checkpoints.verify_snapshot(fixture.directory, fixture.provenance["manifest_sha256"],
                                         seg.decoder_spec("narrow"))
    result.pop("snapshot_directory")
    return checkpoints._decode_arrays((fixture.directory / "checkpoint.npz").read_bytes(),
                                     result["provenance"]["manifest"]), result


def message(arrays):
    return Message(content=RecordDict({"arrays": ArrayRecord(numpy_ndarrays=arrays)}),
                   dst_node_id=1, message_type="train")


def pinned(fixture):
    return task.load_pinned_run_config(fixture.context)


def pins(round_index=1):
    return {"loss_name": "segmentation_bce_dice", "round_index": round_index,
            "num_rounds": 2}


def test_verified_public_payload_initializes_server_and_node_exactly(registry):
    arrays, provenance = verify(registry)
    assert provenance == registry.summary
    payload = checkpoints.client_payload(registry.local_directory)
    cfg = dict(registry.cfg, **{checkpoints.TRANSPORT_KEY:
               base64.b64encode(json.dumps(payload).encode()).decode()})
    model = server_app._build_initial_model(cfg)
    initial = params.get_torch_params(model)
    registry.context.run_config = cfg
    effective = pinned(registry)
    assert checkpoints.TRANSPORT_KEY not in effective
    node_model, dim, image = client_app._prepare_neural_model(
        message(initial), registry.context, effective, {}, pins())
    assert (dim, image) == (seg.FEATURE_DIM, True)
    for expected, server_value, node_value in zip(arrays, initial, params.get_torch_params(node_model)):
        assert expected.tobytes() == server_value.tobytes() == node_value.tobytes()


def test_later_round_uses_federated_arrays_and_first_round_refuses_substitution(registry):
    changed = [a + np.float32(.25) for a in registry.arrays]
    cfg = pinned(registry)
    with pytest.raises(ValueError, match="first global decoder"):
        client_app._prepare_neural_model(message(changed), registry.context, cfg, {}, pins())
    model, _, _ = client_app._prepare_neural_model(
        message(changed), registry.context, cfg, {}, pins(2))
    for expected, value in zip(changed, params.get_torch_params(model)):
        np.testing.assert_array_equal(expected, value)


def test_encoder_mismatch_refused_before_decoder_weights_are_loaded(registry):
    with mock.patch.object(seg, "verified_encoder_bytes", side_effect=ValueError("encoder digest")), \
            mock.patch.object(client_app, "load_user_model") as load:
        with pytest.raises(ValueError, match="encoder digest"):
            client_app._prepare_neural_model(message(registry.arrays), registry.context,
                                             pinned(registry), {}, pins())
    load.assert_not_called()


@pytest.mark.parametrize("name", ["manifest.json", "checkpoint.npz", "original.json",
                                   "protocol.txt", "provenance.json", "audit.txt",
                                   "licence.txt", "mirror_metadata.txt", "encoder.pth"])
def test_every_snapshot_digest_fails_before_private_training(registry, name):
    path = registry.directory / name
    path.write_bytes(path.read_bytes() + b"corrupt")
    claim = {"status": "new", "message_id": "test", "release_index": 1,
             "num_rounds": 2, "epsilon": 1., "delta": 1e-6, "request_id": "1" * 64}
    with mock.patch.object(client_app.release_guard, "claim_release", return_value=claim), \
            mock.patch.object(client_app, "load_run_pins", return_value=pins()), \
            mock.patch.object(client_app, "load_privacy_config", return_value={}), \
            mock.patch.object(client_app, "_train_neural") as train, \
            mock.patch.object(seg, "load_subject_tensors") as read:
        reply = client_app.train(message(registry.arrays), registry.context)
    train.assert_not_called()
    read.assert_not_called()
    assert reply.content["metrics"]["public-preflight-unavailable"] == 1
    assert not list(registry.run.glob("segmentation-public-init-release-*"))


def repin(fixture):
    wire = json.dumps(fixture.manifest).encode()
    (fixture.directory / "manifest.json").write_bytes(wire)
    fixture.provenance["manifest_sha256"] = checkpoints.canonical_manifest_sha256(fixture.manifest)


@pytest.mark.parametrize("index", range(6))
def test_tensor_digest_is_independently_verified(registry, index):
    registry.manifest["tensors"][index]["sha256"] = "0" * 64
    original_path = registry.directory / "original.json"
    original = json.loads(original_path.read_text())
    original["tensor_sha256"][index] = "0" * 64
    data = json.dumps(original).encode()
    original_path.write_bytes(data)
    registry.manifest["evidence"]["original_manifest"].update(sha256=sha(data), size_bytes=len(data))
    repin(registry)
    with pytest.raises(ValueError, match="tensor digest mismatch"):
        verify(registry)


def test_huge_npy_header_refused_before_numpy_load(registry):
    original_payload = (registry.directory / "checkpoint.npz").read_bytes()
    result = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(original_payload)) as source, \
            zipfile.ZipFile(result, "w") as target:
        for item in source.infolist():
            data = source.read(item)
            if item.filename == "0.npy":
                bad = io.BytesIO()
                np.lib.format.write_array_header_1_0(bad, {
                    "descr": "<f4", "fortran_order": False, "shape": (2**40,)})
                data = bad.getvalue()
            target.writestr(item.filename, data)
    data = result.getvalue()
    registry.manifest["checkpoint"].update(sha256=sha(data), size_bytes=len(data))
    with mock.patch.object(np, "load", side_effect=AssertionError("allocation reached")):
        with pytest.raises(ValueError, match="NPY tensor contract"):
            checkpoints._decode_arrays(data, registry.manifest)


@pytest.mark.parametrize("mutation", ["shape", "dtype", "nonfinite", "object", "roster"])
def test_bad_public_tensor_envelopes_are_rejected(registry, mutation):
    arrays = {str(i): a.copy() for i, a in enumerate(registry.arrays)}
    if mutation == "shape":
        arrays["0"] = arrays["0"].flatten()
    elif mutation == "dtype":
        arrays["0"] = arrays["0"].astype(np.float64)
    elif mutation == "nonfinite":
        arrays["0"].flat[0] = np.nan
    elif mutation == "object":
        arrays["0"] = np.asarray([{"not": "weights"}], dtype=object)
    else:
        arrays["extra"] = np.ones(1, np.float32)
    stream = io.BytesIO()
    np.savez(stream, **arrays)
    data = stream.getvalue()
    registry.manifest["checkpoint"].update(sha256=sha(data), size_bytes=len(data))
    with pytest.raises(ValueError, match="tensor|size bound"):
        checkpoints._decode_arrays(data, registry.manifest)


@pytest.mark.parametrize("key,value", [
    (checkpoints.INIT_KEY, "resource"),
    (checkpoints.MANIFEST_KEY, "a" * 64),
    (checkpoints.CHECKPOINT_KEY, "b" * 64),
    (checkpoints.PROVENANCE_KEY, {})])
def test_untrusted_config_cannot_override_node_admission(registry, key, value):
    registry.context.run_config = dict(registry.cfg, **{key: value})
    with pytest.raises(ValueError, match="selection differs|node-owned"):
        pinned(registry)


@pytest.mark.parametrize("source", ["config", "manifest"])
@pytest.mark.parametrize("key", ["release-cache-dir", "release_cache_bytes", "releaseCacheSize",
                                 "deadline", "gatedDeadline"])
def test_public_segmentation_rejects_administrator_only_cache_controls(registry, source, key):
    assert pinned(registry)[checkpoints.INIT_KEY] == "client"
    node = dict(registry.node)
    if source == "config":
        registry.context.run_config = dict(registry.cfg, **{key: "analyst"})
    else:
        node[key] = "analyst"
    with mock.patch.object(task, "_load_manifest", return_value=node):
        with pytest.raises(ValueError, match="administrator-only"):
            pinned(registry)


def test_public_selection_requires_node_authorization_and_matching_decoder(registry):
    for key in checkpoints.NODE_KEYS:
        node = dict(registry.node)
        del node[key]
        with mock.patch.object(task, "_load_manifest", return_value=node):
            with pytest.raises(ValueError, match="node-owned"):
                pinned(registry)
    with pytest.raises(ValueError, match="requested decoder"):
        checkpoints.verify_snapshot(registry.directory,
                                      registry.provenance["manifest_sha256"], seg.decoder_spec())


@pytest.mark.parametrize("key", (*checkpoints.IDENTITY_KEYS, checkpoints.PROVENANCE_KEY,
                                 checkpoints.TRANSPORT_KEY))
def test_checkpoint_fields_cannot_select_another_contract(key):
    context = SimpleNamespace(run_config={key: "public:unapproved"})
    with mock.patch.object(task, "_load_manifest", return_value={}):
        with pytest.raises(ValueError, match="require the segmentation contract"):
            task.load_pinned_run_config(context)


@pytest.mark.parametrize("value", ["public:", "PUBLIC:id", "public:../outside", "public:/tmp/x",
                                    "public:a/b", "public:" + "x" * 65, True, 1, None])
def test_invalid_selection_rejected(value):
    with pytest.raises(ValueError, match="decoder_init"):
        seg.validate_config(dict(public_config(), **{checkpoints.INIT_KEY: value}))


@pytest.mark.parametrize("key", checkpoints.IDENTITY_KEYS)
def test_every_public_identity_changes_semantic_seed_and_replays(registry, key):
    cfg = pinned(registry)
    def derive(config, node):
        selected, _ = client_app._neural_seed_contract(config, pins(), {}, manifest=node)
        return seeding.master_seed("neural-dpsgd/v1", selected,
                                   {"sigma": 1., "policy_hash": "1" * 64}, 1,
                                   public_arrays=registry.arrays,
                                   private_arrays=(np.zeros((1, 2), np.float32),),
                                   execution_fingerprint={})
    before = derive(cfg, registry.node)
    value = "resource" if key == checkpoints.ORIGIN_KEY else "f" * 64
    assert before != derive(dict(cfg, **{key: value}), dict(registry.node, **{key: value}))
    assert before == derive(copy.deepcopy(cfg), copy.deepcopy(registry.node))


def test_random_default_has_unchanged_selection_seed_and_no_snapshot_reads():
    cfg = public_config()
    selected, _ = client_app._neural_seed_contract(cfg, pins(), {}, manifest=cfg)
    explicit = dict(cfg, **{checkpoints.INIT_KEY: "random"})
    assert selected == client_app._neural_seed_contract(explicit, pins(), {}, manifest=explicit)[0]
    with mock.patch.object(checkpoints, "verify_snapshot", side_effect=AssertionError("snapshot read")):
        assert checkpoints.verify_node_checkpoint(cfg, cfg) == (None, None)
        assert checkpoints.server_initialization(cfg) is None
    torch.manual_seed(193)
    before = params.get_torch_params(server_app._build_initial_model(cfg))
    torch.manual_seed(193)
    after = params.get_torch_params(server_app._build_initial_model(explicit))
    assert all(a.tobytes() == b.tobytes() for a, b in zip(before, after))


def test_successful_release_records_exact_provenance_and_released_tensors(registry):
    claim = {"status": "new", "message_id": "test", "release_index": 1,
             "num_rounds": 2, "epsilon": 1., "delta": 1e-6, "request_id": "1" * 64}
    released = [a + np.float32(.001) for a in registry.arrays]
    with mock.patch.object(client_app.release_guard, "claim_release", return_value=claim), \
            mock.patch.object(client_app, "load_run_pins", return_value=pins()), \
            mock.patch.object(client_app, "load_privacy_config", return_value={}), \
            mock.patch.object(client_app, "_train_neural", return_value=(released, 3)):
        reply = client_app.train(message(registry.arrays), registry.context)
    assert not reply.content["metrics"].get("public-preflight-unavailable", 0)
    record = json.loads((registry.run / "segmentation-public-init-release-000001.json").read_text())
    assert record["public_initialisation"] == registry.node[checkpoints.PROVENANCE_KEY]
    assert record["initialisation"] == "analyst-declared"
    assert record["public_initialisation_policy"] == "analyst_or_resource"
    assert record["round"] == 1
    assert [item["sha256"] for item in record["released_tensors"]] == [sha(a.tobytes()) for a in released]


@pytest.mark.parametrize("route", ["client", "resource"])
def test_public_decoder_runs_two_real_dp_rounds_with_unchanged_budget(registry, route):
    registry.cfg[checkpoints.INIT_KEY] = route
    registry.node[checkpoints.INIT_KEY] = route
    registry.node[checkpoints.ORIGIN_KEY] = "analyst-declared" if route == "client" else "resource"
    registry.node[checkpoints.POLICY_KEY] = "analyst_or_resource" if route == "client" else "resource_only"
    registry.node.update({"batch-size": 2, "local-epochs": 1, "num-server-rounds": 2,
                          "learning-rate": .01, "n_samples": 2})
    (registry.run / "manifest.json").write_text(json.dumps(registry.node))
    X = np.zeros((2, seg.FEATURE_DIM), np.float32)
    y = np.zeros((2, 2, 128, 128), np.float32)
    y[:, 1] = 1
    policy = {"epsilon": 4., "delta": 1e-5, "clipping_norm": 1., "n_samples": 2}
    initial = [a.copy() for a in registry.arrays]
    arrays = initial
    with mock.patch.object(seg, "prepare_encoder", return_value=(object(), "cpu")), \
            mock.patch.object(seg, "load_subject_tensors", return_value=(X, y, ["a", "b"], 2)), \
            mock.patch.object(client_app, "load_privacy_config", return_value=policy), \
            mock.patch.object(client_app.release_cache.ReleaseCache, "from_env",
                              side_effect=AssertionError("segmentation must not open the Hook cache")) as cache, \
            mock.patch.object(dp_harness, "effective_dpsgd_mechanism",
                              wraps=dp_harness.effective_dpsgd_mechanism) as mechanism:
        for rnd in (1, 2):
            claim = {"status": "new", "message_id": "round-%d" % rnd,
                     "release_index": rnd, "num_rounds": 2, "epsilon": policy["epsilon"],
                     "delta": policy["delta"], "request_id": str(rnd) * 64}
            with mock.patch.object(client_app.release_guard, "claim_release", return_value=claim):
                reply = client_app.train(message(arrays), registry.context)
            assert not reply.content["metrics"].get("public-preflight-unavailable", 0)
            assert not reply.content["metrics"].get("execution-unavailable", 0)
            arrays = reply.content["arrays"].to_numpy_ndarrays()
            record = json.loads((registry.run / (
                "segmentation-public-init-release-%06d.json" % rnd)).read_text())
            assert record["public_initialisation"] == registry.summary
    cache.assert_not_called()
    assert all(not np.array_equal(a, b) for a, b in zip(initial, arrays))
    assert mechanism.call_count == 2
    for call in mechanism.call_args_list:
        assert call.kwargs == {"epsilon": 4., "delta": 1e-5, "clipping_norm": 1.,
                               "n_samples": 2, "batch_size": 2, "local_epochs": 1,
                               "num_rounds": 2}


@pytest.mark.parametrize("kind", ["file", "directory", "writable"])
def test_snapshot_refuses_replaceable_paths(registry, kind):
    if kind == "writable":
        if os.name == "nt":
            pytest.skip("POSIX mode check")
        path = registry.directory / "checkpoint.npz"
        path.chmod(0o666)
    else:
        path = registry.directory if kind == "directory" else registry.directory / "checkpoint.npz"
        target = path.with_name(path.name + ".original")
        path.rename(target)
        path.symlink_to(target, target_is_directory=(kind == "directory"))
    with pytest.raises(ValueError, match="protected|roster"):
        verify(registry)


@pytest.mark.parametrize("value", [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}'])
def test_manifest_rejects_ambiguous_or_nonfinite_json(value):
    with pytest.raises(ValueError):
        checkpoints._json(value)


def test_archive_snapshot_has_no_bytes_in_public_summary_and_private_modes(registry, tmp_path):
    archive = tmp_path / "public.zip"
    packed = checkpoints.pack_bundle(registry.local_directory, archive)
    admitted = checkpoints.admit_bundle(archive, tmp_path / "resource-cache", sha(archive.read_bytes()))
    snapshot = Path(admitted.pop("snapshot_directory"))
    admitted.pop("bundle_sha256")
    assert admitted == packed == registry.summary
    assert not any("base64" in key or "b64" in key for key in admitted)
    assert "local_arrays_b64" not in json.dumps(admitted)
    if os.name == "posix":
        assert snapshot.stat().st_mode & 0o777 == 0o700
        assert all(p.stat().st_mode & 0o777 == 0o600 for p in snapshot.iterdir())
    # The original local file is no authority after resource snapshotting.
    archive.write_bytes(b"changed source")
    verified = checkpoints.verify_snapshot(snapshot, registry.provenance["manifest_sha256"])
    assert verified["provenance"] == registry.provenance


def test_archive_pin_checked_before_manifest_or_tensor_parsing(registry, tmp_path):
    archive = tmp_path / "bad.zip"
    archive.write_bytes(b"not even a zip")
    with mock.patch.object(checkpoints, "_json", side_effect=AssertionError("parsed")), \
            mock.patch.object(np, "load", side_effect=AssertionError("parsed tensors")):
        with pytest.raises(ValueError, match="registered bundle digest"):
            checkpoints.admit_bundle(archive, tmp_path / "cache", "0" * 64)


@pytest.mark.parametrize("attack", ["traversal", "absolute", "symlink", "directory", "duplicate", "extra", "oversize", "size", "compression"])
def test_archive_attacks_are_rejected_before_numpy(registry, tmp_path, attack):
    archive = tmp_path / "bad.zip"
    with zipfile.ZipFile(archive, "w") as target:
        for path in registry.local_directory.iterdir():
            if attack == "size" and path.name == "encoder.pth":
                target.writestr(path.name, path.read_bytes() + b"changed")
            else:
                target.writestr(path.name, path.read_bytes())
        if attack == "traversal":
            target.writestr("../outside", b"bad")
        elif attack == "absolute":
            target.writestr("/outside", b"bad")
        elif attack == "symlink":
            info = zipfile.ZipInfo("link")
            info.external_attr = (0o120777 << 16)
            target.writestr(info, b"encoder.pth")
        elif attack == "directory":
            target.writestr("nested/", b"")
        elif attack == "duplicate":
            target.writestr("manifest.json", b"{}")
        elif attack == "extra":
            target.writestr("unlisted.txt", b"extra")
        elif attack == "oversize":
            target.writestr("manifest.json", b"x" * 65537)
        elif attack == "compression":
            target.writestr("bomb.txt", b"0" * 1000000, compress_type=zipfile.ZIP_DEFLATED)
    with mock.patch.object(np, "load", side_effect=AssertionError("parsed tensors")):
        with pytest.raises(ValueError, match="archive|manifest|roster"):
            checkpoints.inspect_bundle(archive)


def test_repack_alias_and_administrative_metadata_do_not_change_noise_identity(registry, tmp_path):
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    checkpoints.pack_bundle(registry.local_directory, first)
    manifest = json.loads((registry.local_directory / "manifest.json").read_bytes())
    manifest["checkpoint_id"] = "custodian-alias"
    manifest["creation"] = {"creator": "another packager", "created_at": "2027-01-01"}
    (registry.local_directory / "manifest.json").write_text(json.dumps(manifest, indent=4))
    with zipfile.ZipFile(second, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in reversed(list(registry.local_directory.iterdir())):
            archive.writestr(path.name, path.read_bytes())
    a = checkpoints.inspect_bundle(first)
    b = checkpoints.inspect_bundle(second)
    assert sha(first.read_bytes()) != sha(second.read_bytes())
    assert a["provenance"]["manifest_sha256"] == b["provenance"]["manifest_sha256"]
    alias = dict(registry.node)
    alias[checkpoints.PROVENANCE_KEY] = b
    alias[checkpoints.DIRECTORY_KEY] = "/another/private/cache"
    alias["resource_name"] = "PublicModels.alias"
    alias["resource_symbol"] = "OTHER_HANDLE"
    alias["bundle_sha256"] = sha(second.read_bytes())
    assert seeding.request_selection(alias) == seeding.request_selection(registry.node)
    changed = dict(registry.node, **{checkpoints.MANIFEST_KEY: "b" * 64})
    assert seeding.request_selection(changed) != seeding.request_selection(registry.node)


@pytest.mark.parametrize("route,policy", [("client", "resource_only"), ("client", "none"), ("resource", "none")])
def test_runner_refuses_disallowed_origin_before_model_or_private_access(registry, route, policy):
    registry.cfg[checkpoints.INIT_KEY] = route
    registry.node.update({checkpoints.INIT_KEY: route, checkpoints.POLICY_KEY: policy,
                          checkpoints.ORIGIN_KEY: "analyst-declared" if route == "client" else "resource"})
    (registry.run / "manifest.json").write_text(json.dumps(registry.node))
    with mock.patch.object(client_app, "load_user_model") as model, \
            mock.patch.object(seg, "load_subject_tensors") as private:
        with pytest.raises(ValueError, match="node policy"):
            client_app._prepare_neural_model(message(registry.arrays), registry.context,
                                             pinned(registry), {}, pins())
    model.assert_not_called()
    private.assert_not_called()


def test_local_coordinator_file_authorizes_nothing_and_must_match_summary(registry, tmp_path):
    payload = checkpoints.coordinator_payload(registry.local_directory / "checkpoint.npz", registry.summary)
    assert payload == checkpoints.client_payload(registry.local_directory)
    wrong = tmp_path / "wrong.npz"
    wrong.write_bytes(b"other public weights")
    with pytest.raises(ValueError, match="size|digest"):
        checkpoints.coordinator_payload(wrong, registry.summary)
    registry.context.run_config[checkpoints.DIRECTORY_KEY] = str(registry.local_directory)
    with pytest.raises(ValueError, match="node-owned"):
        pinned(registry)


@pytest.mark.parametrize("key", ["public-initialisation-policy", "public_initialisation_policy",
                                 "publicInitialisationPolicy", "dsflower.public_initialisation",
                                 "dsflower.public_initialisation.pytorch_resnet18_segmentation"])
def test_untrusted_nonsegmentation_config_cannot_set_policy(key):
    context = SimpleNamespace(run_config={key: "analyst_or_resource"})
    with mock.patch.object(task, "_load_manifest", return_value={checkpoints.POLICY_KEY: "none"}):
        with pytest.raises(ValueError, match="administrator-only"):
            task.load_pinned_run_config(context)


def test_nonsegmentation_manifest_may_report_node_owned_policy():
    context = SimpleNamespace(run_config={})
    with mock.patch.object(task, "_load_manifest", return_value={checkpoints.POLICY_KEY: "none"}):
        assert task.load_pinned_run_config(context)[checkpoints.POLICY_KEY] == "none"


@pytest.mark.parametrize("mutation", ["entry-count", "directory-size", "trailing", "prefix"])
def test_archive_directory_bound_checked_before_zipfile_objects(registry, tmp_path, mutation):
    import struct
    archive = tmp_path / "archive.zip"
    checkpoints.pack_bundle(registry.local_directory, archive)
    payload = bytearray(archive.read_bytes())
    footer = payload.rfind(b"PK\x05\x06")
    if mutation == "entry-count":
        struct.pack_into("<HH", payload, footer + 8, 60000, 60000)
    elif mutation == "directory-size":
        struct.pack_into("<I", payload, footer + 12, 20000000)
    elif mutation == "trailing":
        payload += b"unlisted trailing artifact"
    else:
        payload[:4] = b"evil"
    archive.write_bytes(payload)
    with mock.patch.object(zipfile, "ZipFile", side_effect=AssertionError("central directory allocated")):
        with pytest.raises(ValueError, match="archive"):
            checkpoints.inspect_bundle(archive)
