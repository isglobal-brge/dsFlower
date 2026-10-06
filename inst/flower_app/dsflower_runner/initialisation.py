"""Public, spec-seeded coordinator initialisation with isolated PRNG state.

This is a reproducible default, not a node-side restriction on the analyst's
initial weights. Admitted incoming arrays always remain semantic release inputs.
"""

from contextlib import contextmanager, nullcontext
import hashlib
import json
import random

import numpy as np

VERSION = "dsflower-public-init-v1"


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode("utf-8")


@contextmanager
def isolated_public_rng(seed):
    """Seed Python/NumPy/Torch CPU construction, restoring every caller state.

    Torch is optional for Hook initializers. fork_rng intentionally includes no
    CUDA devices: the public initializer is a CPU construction profile.
    """
    python_state, numpy_state = random.getstate(), np.random.get_state()
    try:
        import torch
    except ImportError:
        torch = None
    try:
        with (torch.random.fork_rng(devices=[]) if torch is not None else nullcontext()):
            random.seed(int(seed))
            np.random.seed(int(seed) % (1 << 32))
            if torch is not None:
                # manual_seed also touches CUDA generators; seed only the CPU
                # generator whose state is covered by fork_rng(devices=[]).
                torch.random.default_generator.manual_seed(int(seed))
            yield
    finally:
        random.setstate(python_state)
        np.random.set_state(numpy_state)


def canonical_initialisation_spec(cfg, *, application_init_contract=None):
    """Only inputs consumed by model construction; no fold/data/session/policy."""
    model, extractor, encoder, checkpoint = None, None, None, None
    if application_init_contract is None:
        from . import model_spec
        loss = str(cfg.get("loss-name", "bce_logits"))
        spatial = None
        if loss == "segmentation_bce_dice":
            from . import segmentation
            spatial = list(segmentation.OUTPUT_SHAPE)
        model = {"spec": model_spec.canonical_spec(cfg),
                 "input_shape": [int(cfg["num-features"])],
                 "output_shape": spatial or [model_spec.output_width(loss, cfg)],
                 "output_limit": model_spec.output_limit_for_loss(loss)}
        if str(cfg.get("data-kind", cfg.get("data_type", ""))).lower() == "image":
            from . import vision
            backbone, _, _ = vision.require_extractor_config(
                cfg.get("backbone", cfg.get("model", "resnet18")),
                cfg.get("vision-extractor-profile"), cfg.get("num-features"), cfg.get("image-size"))
            extractor = cfg.get("vision-extractor-profile")
            if backbone == "resnet18_layer2":
                from .segmentation import CHECKPOINT_SHA256
                encoder = CHECKPOINT_SHA256
            elif backbone in vision._PRESEEDED_ENCODERS:
                encoder = vision._PRESEEDED_ENCODERS[backbone][2]
        from . import segmentation_checkpoints as checkpoints
        if checkpoints.checkpoint_id(cfg) is not None:
            # The admitted manifest/checkpoint identities describe scientific
            # content, not a local path, resource alias or archive packaging.
            checkpoint = {
                "manifest_sha256": cfg.get(checkpoints.MANIFEST_KEY),
                "checkpoint_sha256": cfg.get(checkpoints.CHECKPOINT_KEY),
                "encoder_sha256": cfg.get(checkpoints.ENCODER_KEY),
                "origin": cfg.get(checkpoints.ORIGIN_KEY),
            }
    return {"version": VERSION, "model": model, "extractor_profile": extractor,
            "encoder_sha256": encoder, "public_checkpoint_semantics": checkpoint,
            "application_init_contract": application_init_contract}


def initialisation_spec_sha256(cfg, *, application_init_contract=None):
    return hashlib.sha256(_json(canonical_initialisation_spec(
        cfg, application_init_contract=application_init_contract))).hexdigest()


def public_initialisation_seed(cfg, *, application_init_contract=None):
    digest = initialisation_spec_sha256(cfg, application_init_contract=application_init_contract)
    return int.from_bytes(hashlib.sha256(
        b"dsflower/public-init/v1\x00" + bytes.fromhex(digest)).digest()[:8], "big") & ((1 << 63) - 1)


def hash_public_arrays(arrays):
    """Ordered admitted tensor contents, with the semantic canonical encoding."""
    from .seeding import _update_arrays
    digest = hashlib.sha256()
    _update_arrays(digest, "public-arrays", arrays)
    return digest.hexdigest()


def build_initial_model(cfg):
    """Build every trusted neural family on CPU under the public spec seed."""
    from . import model_spec, segmentation_checkpoints
    from .params import set_torch_params
    loss = str(cfg.get("loss-name", "bce_logits"))
    spatial = {}
    if loss == "segmentation_bce_dice":
        from . import segmentation
        segmentation.validate_config(cfg)
        segmentation.configure_runtime()
        spatial["output_shape"] = segmentation.OUTPUT_SHAPE
    seed = public_initialisation_seed(cfg)
    with isolated_public_rng(seed):
        model = model_spec.build_from_spec(
            model_spec.read_spec(cfg), in_dim=int(cfg["num-features"]),
            out_dim=model_spec.output_width(loss, cfg),
            num_labels=int(cfg["num-labels"]) if cfg.get("num-labels") is not None else None,
            output_limit=model_spec.output_limit_for_loss(loss), **spatial)
        if segmentation_checkpoints.checkpoint_id(cfg) is not None:
            arrays = segmentation_checkpoints.server_initialization(cfg)
            if arrays is not None:
                set_torch_params(model, arrays)
    return model


def hook_initialisation_contract(cfg, public_cfg, user_module):
    """Server-side logical package hash plus the initializer's observable inputs."""
    import os
    path = getattr(user_module, "__file__", None)
    package_hash = None
    if path:
        digest = hashlib.sha256()
        root = os.path.dirname(os.path.realpath(path))
        entries = []
        if os.path.basename(path).startswith("__init__."):
            for current, dirs, files in os.walk(root):
                dirs[:] = sorted(d for d in dirs if d != "__pycache__")
                entries.extend((os.path.relpath(os.path.join(current, name), root).replace(os.sep, "/"),
                                os.path.join(current, name)) for name in files
                               if not name.endswith((".pyc", ".pyo")))
        else:
            entries = [("__init__.py", path)]
        for relative, filename in sorted(entries):
            digest.update(relative.encode("utf-8") + b"\n")
            with open(filename, "rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            digest.update(b"\x00")
        package_hash = digest.hexdigest()
    return {"version": VERSION, "entrypoint": str(cfg["user-module"]),
            "package_sha256": package_hash, "input_shape": [int(cfg.get("num-features", 0))],
            "config": public_cfg}
