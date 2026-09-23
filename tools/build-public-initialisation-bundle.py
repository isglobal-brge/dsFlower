#!/usr/bin/env python3
"""Wrap recovered public checkpoint bytes and their evidence; never install/admit.

The source directory holds a preserved 0.6.0 manifest, its exact evidence files,
and recovered checkpoint.npz. Encoder bytes must match the contract's full pin.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "inst" / "flower_app"))
from dsflower_runner import segmentation, segmentation_checkpoints as checkpoints


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference_directory", type=Path)
    parser.add_argument("encoder", type=Path)
    parser.add_argument("output_zip", type=Path)
    parser.add_argument("--creator", required=True,
                        help="public attribution for the bundle envelope")
    args = parser.parse_args()
    if args.output_zip.exists():
        parser.error("output already exists; choose a new filename")
    if not args.creator.strip():
        parser.error("creator must be nonempty")
    source = args.reference_directory.resolve(strict=True)
    manifest = checkpoints._json((source / "manifest.json").read_bytes())
    if manifest.get("schema_version") != "dsflower-segmentation-public-checkpoint/v1":
        parser.error("source must be a preserved 0.6.0 reference manifest")
    with tempfile.TemporaryDirectory(prefix="dsflower-public-bundle-") as temporary:
        directory = Path(temporary)
        records = [manifest["checkpoint"], *manifest["evidence"].values()]
        names = [record["file"] for record in records]
        if len(names) != len(set(names)):
            parser.error("duplicate artifact names")
        for name in names:
            if Path(name).name != name or name in {"manifest.json", "encoder.pth"}:
                parser.error("artifact name must be a distinct root filename")
            original = source / name
            if original.is_symlink() or not original.is_file():
                parser.error("artifact must be an existing regular file")
            shutil.copyfile(original, directory / name)
        if args.encoder.is_symlink() or not args.encoder.is_file():
            parser.error("encoder must be an existing regular file")
        shutil.copyfile(args.encoder, directory / "encoder.pth")
        manifest.update(
            schema_version=checkpoints.SCHEMA,
            role="segmentation_decoder",
            encoder={"file": "encoder.pth", "size_bytes": 46830571,
                     "sha256": segmentation.CHECKPOINT_SHA256},
            decoder_spec_sha256=hashlib.sha256(json.dumps(
                segmentation.decoder_spec(manifest["decoder"]), sort_keys=True,
                separators=(",", ":"), ensure_ascii=False).encode()).hexdigest(),
            pretraining_protocol_sha256=manifest["evidence"]["protocol"]["sha256"],
            creation={"created_at": datetime.now(timezone.utc).isoformat(),
                      "creator": args.creator})
        (directory / "manifest.json").write_text(
            json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False) + "\n")
        checkpoints.pack_bundle(directory, args.output_zip)
    print("Verified public bundle:", args.output_zip)
    print("Descriptor format: dsflower-checkpoint-v1:" +
          hashlib.sha256(args.output_zip.read_bytes()).hexdigest())
    print("Register this bundle using platform resource administration, or declare it from the client.")


if __name__ == "__main__":
    main()
