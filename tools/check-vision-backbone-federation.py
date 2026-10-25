#!/usr/bin/env python3
"""One short local two-site Flower federation for all vision training modes.

The real client admission, image loader, patient pooling, secure DP randomness,
durable release ledger, Flower protobuf arrays, and FedAvg are exercised. Sites
run serially in one process to bound CPU memory. This does not start an Opal or
gRPC deployment. Pre-seed the official verified ImageNet checkpoint in TORCH_HOME.

--runner-root can point at a previous inst/flower_app tree for comparison. The
release PRF deliberately binds the runner source digest, so different versions
produce different secure streams even for these fixed public fixture secrets.
The targeted unit tests separately compare frozen arithmetic at a fixed seed.
"""

import argparse
import base64
import gc
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--runner-root", type=Path,
                    default=Path(__file__).resolve().parents[1] / "inst" / "flower_app")
parser.add_argument("--backbone", choices=("resnet18", "densenet121"), default="resnet18")
parser.add_argument("--modes", nargs="+", choices=("frozen", "finetune", "scratch"),
                    default=("frozen", "finetune", "scratch"))
parser.add_argument("--output-dir", type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.runner_root.resolve()))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "inst" / "python" / "tests"))

import numpy as np
import pandas as pd
from PIL import Image
import torch
from flwr.common import ArrayRecord, ConfigRecord, Message, RecordDict
from flwr.common.serde import recorddict_from_proto, recorddict_to_proto
from dsflower_runner import client_app, initialisation, params, server_app, vision
from v3_test_support import source_sidecar

torch.set_num_threads(1)
args.output_dir.mkdir(parents=True, exist_ok=True)


def config(mode):
    spec = {"layers": [{"op": "linear"}]}
    cfg = {
        "model-spec-b64": base64.b64encode(json.dumps(spec).encode()).decode(),
        "data-kind": "image", "backbone": args.backbone,
        "vision-extractor-profile": vision.extractor_profile_for(args.backbone),
        "num-features": vision.feature_dim_for(args.backbone), "num-classes": 2,
        "image-size": 32, "loss-name": "bce_logits", "task-type": "classification",
        "batch-size": 2, "local-epochs": 1, "num-server-rounds": 1,
        "learning-rate": .01, "dp-track": "neural", "strategy": "fedavg",
    }
    # Keep the default request byte-identical to the pre-0.7.4 request.
    if mode != "frozen":
        cfg["backbone-mode"] = mode
    return cfg


def node(root, cfg, site):
    root.mkdir(mode=0o700)
    images = root / "images"
    images.mkdir(mode=0o700)
    generator = np.random.default_rng(900 + site)
    for index in range(3):
        pixels = generator.integers(0, 256, (32, 32, 3), dtype=np.uint8)
        Image.fromarray(pixels).save(images / ("image-%d.png" % index))
    frame = pd.DataFrame({"relative_path": ["image-%d.png" % i for i in range(3)],
                          "patient_id": ["patient-a", "patient-a", "patient-b"],
                          "label": [0, 0, 1]})
    frame.to_csv(root / "samples.csv", index=False)
    manifest = dict(cfg, **{
        "run_token": "run_" + str(site) * 32,
        "data_type": "image", "samples_file": "samples.csv", "data_format": "csv",
        "target_column": "label", "target-preencoded": True,
        "patient_column": "patient_id", "dp-unit": "patient",
        "patient-id-canonicalization": "trim-utf8-v2",
        "assets": {"images": {"root": str(images), "path_col": "relative_path"}},
        "n_samples": 3, "n_units": 2,
        "request-source": {"source": "image", "data_symbol": "public_test_images"},
        "privacy-adjacency": "replace_one", "privacy-policy-sha256": "1" * 64,
        "privacy-epsilon": 4., "privacy-delta": 1e-5, "privacy-clipping_norm": 1.,
    })
    if cfg.get("backbone-mode", "frozen") != "frozen":
        manifest["allow-trainable-backbone"] = True
    source_sidecar(root, manifest, frame, columns=["label"])
    (root / "manifest.json").write_text(json.dumps(manifest))
    secret = root.parent / ("secret-%d" % site)
    secret.write_text((bytes([site]) * 32).hex())
    secret.chmod(0o600)
    return SimpleNamespace(node_config={"manifest-dir": str(root)},
                           run_config=dict(cfg), state=RecordDict()), secret


def wire_roundtrip(records):
    proto = recorddict_to_proto(records)
    encoded = proto.SerializeToString()
    decoded = type(proto)()
    decoded.ParseFromString(encoded)
    return recorddict_from_proto(decoded), len(encoded)


summaries = []
for mode in args.modes:
    started = time.monotonic()
    cfg = config(mode)
    model = server_app._build_initial_model(cfg)
    initial = [value.copy() for value in params.get_torch_params(model)]
    del model
    replies = []
    wire_bytes = []
    with tempfile.TemporaryDirectory(prefix="dsflower-vision-%s-" % mode) as temporary:
        root = Path(temporary)
        for site in (1, 2):
            context, secret = node(root / ("site-%d" % site), cfg, site)
            os.environ["DSFLOWER_NODE_SECRET_FILE"] = str(secret)
            os.environ["DSFLOWER_NEIGHBOURHOOD_DIR"] = str(root / ("history-%d" % site))
            for name in ("STORE_ID", "K", "MAX_ANCHORS", "STORE_BYTES"):
                os.environ.pop("DSFLOWER_NEIGHBOURHOOD_" + name, None)
            incoming, _ = wire_roundtrip(RecordDict({
                "arrays": ArrayRecord(numpy_ndarrays=initial),
                "config": ConfigRecord({"server-round": 1}),
            }))
            request = Message(content=incoming, dst_node_id=site, message_type="train")
            reply = client_app.train(request, context)
            assert not reply.has_error(), reply
            assert dict(reply.content["metrics"]) == {"num-examples": 1}, dict(reply.content["metrics"])
            outgoing, size = wire_roundtrip(reply.content)
            wire_bytes.append(size)
            site_arrays = outgoing["arrays"].to_numpy_ndarrays()
            assert len(site_arrays) == len(initial)
            assert all(np.isfinite(value).all() for value in site_arrays)
            assert all(np.any(a != b) for a, b in zip(site_arrays, initial)), "untrained/unnoised parameter"
            replies.append(Message(content=outgoing, reply_to=request))
            del site_arrays, reply, incoming, context
            gc.collect()
        strategy = server_app._build_strategy(cfg, min_nodes=2)
        aggregated, _ = strategy.aggregate_train(1, replies)
        assert aggregated is not None
        arrays = aggregated.to_numpy_ndarrays()
        restored = server_app._build_initial_model(cfg)
        params.set_torch_params(restored, arrays)
        artifact = args.output_dir / (args.backbone + "-" + mode + ".pt")
        torch.save(restored.state_dict(), artifact)
        summary = {
            "backbone": args.backbone, "mode": mode, "sites": 2, "rounds": 1,
            "patients_per_site": 2, "images_per_site": 3,
            "parameters": sum(value.size for value in arrays),
            "array_bytes": sum(value.nbytes for value in arrays),
            "protobuf_reply_bytes": max(wire_bytes),
            "model_sha256": initialisation.hash_public_arrays(arrays),
            "seconds": round(time.monotonic() - started, 3), "artifact": str(artifact),
        }
        summaries.append(summary)
        print(json.dumps(summary), flush=True)
        del restored, arrays, aggregated, replies, initial, strategy
        gc.collect()
(args.output_dir / "results.json").write_text(json.dumps(summaries, indent=2) + "\n")
