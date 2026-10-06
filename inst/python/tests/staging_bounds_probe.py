"""Check the actual R-staged singleton schema at both neural consumers."""

import argparse
import json
from pathlib import Path
import sys
from types import SimpleNamespace


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app-dir", required=True)
    parser.add_argument("--stage-dir", required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(Path(args.app_dir).resolve()))
    import numpy as np
    from dsflower_runner import client_app, task, validation

    stage = Path(args.stage_dir).resolve()
    with (stage / "manifest.json").open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    assert manifest["feature_columns"] == ["x"]
    assert manifest["feature-bounds"] == {"lower": [0], "upper": [4]}
    context = SimpleNamespace(node_config={"manifest-dir": str(stage)})
    features, _ = task.load_data(context)
    trained = client_app._apply_feature_bounds(features, manifest)
    validated = validation._apply_feature_bounds(features, manifest)
    np.testing.assert_array_equal(trained, validated)
    np.testing.assert_array_equal(np.sort(trained[:, 0]), [-1, 0, 0, 1])
    print("CHECK singleton bounds train-validation PASS")


if __name__ == "__main__":
    main()
