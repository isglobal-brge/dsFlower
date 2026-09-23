"""Integrity of shipped BUSI metadata; no original NPZ bytes are available here."""

import hashlib
import io
import json
from pathlib import Path
import unittest
import zipfile

import numpy as np


BUNDLE = Path(__file__).resolve().parents[2] / "extdata" / "segmentation-public-checkpoints"
HISTORICAL = {
    20260919: (
        "b6db994dbb3a922b7b62e60a97a6dac5ce56757015d8e3756b1788e08673d311",
        "7d4152fab1052a7696ccfa722a5661836f455a648a96e6f99654b8e734f5c55d",
    ),
    20260920: (
        "b49b39a40ac00287ba5292f053aafff4c8ef042d09ac9c3a51d88ef2d94e07ed",
        "3f3e9d6ceca5dda443c8e0f52d882f5d23b041685eff4b954bbd95449a9523db",
    ),
    20260921: (
        "54a83fc0751d6a3fba2f461db5a9b1e06ef7b828beed30bc0fc2fc7530452abb",
        "d7bfadb69b4c539669783e566a6185424f748102b612b48710d1a03cb46f7b1d",
    ),
}
EVIDENCE_PINS = {
    "protocol": "28a713f8ffe6f7bb7b74e7d59d13d1be265d5738b43503c60e6581a67e0c5820",
    "provenance": "0e3dd39468fdfe562b54b412284eb7315e612ba2f579b566ec49495bfe3bc51d",
    "audit": "d59334151e074de56afaaec81e92930ddaad8371f7f2665577096ce4ff7d9d57",
    "licence": "a2010f343487d3f7618affe54f789f5487602331c0a8d03f49e9a7c547cf0499",
    "mirror_metadata": "be8bd03602e74ccd95bd86b87f607362e41f6778f4c9d92063d3497e76b82468",
}
ENCODER = "f37072fd47e89c5e827621c5baffa7500819f7896bbacec160b1a16c560e07ec"
DATASET = "7fffc86a517934da55021f66641d81383058d26a30adb0ffab3780ca7e52d57d"
SHAPES = [(8, 128, 3, 3), (8,), (4, 8, 3, 3), (4,), (1, 4, 1, 1), (1,)]


class SegmentationCheckpointBundleTests(unittest.TestCase):
    def setUp(self):
        self.pins = json.loads((BUNDLE / "allowlist.json").read_bytes())

    def records(self):
        for seed in HISTORICAL:
            checkpoint_id = "busi-v5-epochs60-seed%d" % seed
            directory = BUNDLE / checkpoint_id
            manifest = json.loads((directory / "manifest.json").read_bytes())
            original = json.loads((directory / "original-manifest.json").read_bytes())
            yield seed, checkpoint_id, directory, manifest, original

    def test_three_registry_manifests_match_allowlist_and_historical_pins(self):
        self.assertEqual(set(self.pins), {
            "busi-v5-epochs60-seed%d" % seed for seed in HISTORICAL})
        for seed, checkpoint_id, directory, manifest, original in self.records():
            with self.subTest(seed=seed):
                self.assertEqual(hashlib.sha256(
                    (directory / "manifest.json").read_bytes()).hexdigest(),
                    self.pins[checkpoint_id])
                self.assertEqual(manifest["schema_version"],
                                 "dsflower-segmentation-public-checkpoint/v1")
                self.assertEqual(manifest["checkpoint_id"], checkpoint_id)
                self.assertEqual(manifest["model_id"], "pytorch_resnet18_segmentation")
                self.assertEqual(manifest["decoder"], "narrow")
                self.assertEqual(manifest["feature_contract"], "resnet18_layer2_128_v1")
                self.assertEqual(manifest["encoder_sha256"], ENCODER)
                self.assertEqual(manifest["checkpoint"], {
                    "file": "checkpoint.npz", "sha256": HISTORICAL[seed][0],
                    "size_bytes": 39510,
                })
                self.assertEqual(original["checkpoint_sha256"], HISTORICAL[seed][0])
                self.assertEqual(original["seed"], seed)
                self.assertEqual(original["epochs"], 60)
                self.assertEqual(original["privacy"], "public_nonprivate")

    def test_evidence_bytes_preserve_original_digests_and_provenance_links(self):
        for seed, _, directory, manifest, original in self.records():
            expected = dict(EVIDENCE_PINS, original_manifest=HISTORICAL[seed][1])
            self.assertEqual(set(manifest["evidence"]), set(expected))
            for name, digest in expected.items():
                with self.subTest(seed=seed, evidence=name):
                    record = manifest["evidence"][name]
                    payload = (directory / record["file"]).read_bytes()
                    self.assertEqual(record["sha256"], digest)
                    self.assertEqual(hashlib.sha256(payload).hexdigest(), digest)
                    self.assertEqual(record["size_bytes"], len(payload))
            with self.subTest(seed=seed, evidence="links"):
                provenance = json.loads((directory / "provenance.json").read_bytes())
                audit = json.loads((directory / "audit.json").read_bytes())
                self.assertEqual(manifest["dataset"], provenance)
                self.assertEqual(provenance["sha256"], DATASET)
                self.assertEqual(original["dataset_sha256"], DATASET)
                self.assertEqual(original["encoder_sha256"], ENCODER)
                for name in ("protocol", "provenance", "audit"):
                    self.assertEqual(original[name + "_sha256"], EVIDENCE_PINS[name])
                self.assertEqual(provenance["licence_sha256"], EVIDENCE_PINS["licence"])
                self.assertEqual(provenance["metadata_sha256"],
                                 EVIDENCE_PINS["mirror_metadata"])
                self.assertEqual(manifest["licence"]["declaration"], provenance["licence"])
                self.assertIn("not an assertion about original rights",
                              manifest["licence"]["scope"])
                self.assertEqual(audit["source"], provenance)
                self.assertEqual(audit["protocol_sha256"], EVIDENCE_PINS["protocol"])
                self.assertEqual(audit["encoder_sha256"], ENCODER)

    def test_tensor_roster_matches_the_six_original_hashes_and_fixed_shapes(self):
        for seed, _, _, manifest, original in self.records():
            with self.subTest(seed=seed):
                self.assertEqual(len(manifest["tensors"]), 6)
                for i, (tensor, shape, digest) in enumerate(zip(
                        manifest["tensors"], SHAPES, original["tensor_sha256"])):
                    self.assertEqual(tensor, {
                        "name": str(i), "shape": list(shape),
                        "dtype": "float32", "sha256": digest,
                    })

    def test_source_derived_expected_size_uses_uncompressed_synthetic_layout(self):
        # This checks the source's container layout only. These zero arrays are
        # never saved as checkpoints and do not verify the unavailable originals.
        payload = io.BytesIO()
        np.savez(payload, **{
            str(i): np.zeros(shape, dtype=np.float32)
            for i, shape in enumerate(SHAPES)
        })
        with zipfile.ZipFile(payload) as archive:
            self.assertEqual(archive.namelist(), ["%d.npy" % i for i in range(6)])
            self.assertTrue(all(item.compress_type == zipfile.ZIP_STORED
                                for item in archive.infolist()))
            self.assertEqual(sum(item.file_size for item in archive.infolist()),
                             38084 + 6 * 128)
        self.assertEqual(len(payload.getvalue()), 38084 + 768 + 330 + 306 + 22)
        self.assertEqual(len(payload.getvalue()), 39510)


if __name__ == "__main__":
    unittest.main()
