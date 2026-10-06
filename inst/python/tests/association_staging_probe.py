"""Exercise the trusted association app against synthetic R-staged bundles.

Called by the R staging regression. No Flower federation or network is started;
the real manifest checks, source sidecar loader, v3 identity, and DP release run.
"""

import argparse
import json
from pathlib import Path
import sys
from types import SimpleNamespace


def check_stage(stage_dir):
    from flwr.common import ConfigRecord, Message, RecordDict
    from dsflower_runner import (
        association_client_app, epi_association, seeding, task,
    )

    with (Path(stage_dir) / "manifest.json").open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    config = {key: manifest[key] for key in association_client_app._PIN_FIELDS}
    config.update({
        "dp-track": "association", "num-server-rounds": 1,
        "min-train-nodes": manifest["association-n-nodes"],
    })
    context = SimpleNamespace(
        node_config={"manifest-dir": str(stage_dir)}, run_config=config)

    def request(node_id):
        return Message(content=RecordDict({"config": ConfigRecord({
            **{key: config[key] for key in association_client_app._PIN_FIELDS},
            "server-round": 1,
        })}), message_type="train", dst_node_id=node_id)

    message = request(1)
    node_manifest, cfg = association_client_app._pinned_contract(message, context)
    privacy = task.load_privacy_config(context)
    layout = epi_association.association_layout(cfg["association-privacy-unit"])
    identity = seeding.request_identity(
        "association-vector", {
            "layout": layout, "mechanism-profile": layout,
            "request-selection": seeding.request_selection(node_manifest),
        }, privacy, execution_fingerprint=epi_association.EXECUTION_PROFILE,
        manifest=node_manifest)
    outcome, exposure, unit_ids, units = task.load_association_data(
        context, manifest=node_manifest, include_canonical_units=True)
    sufficient = epi_association.association_sufficient_vector(
        outcome, exposure, outcome_levels=(0, 1), exposure_levels=(0, 1),
        privacy_unit=cfg["association-privacy-unit"], unit_ids=unit_ids)
    released, sigma = epi_association.private_association_vector(
        sufficient, privacy_unit=cfg["association-privacy-unit"],
        epsilon=privacy["epsilon"], delta=privacy["delta"],
        request_selection=seeding.request_selection(node_manifest),
        request_identity=identity, source_units=units)

    # The direct steps above retain a useful traceback if a new boundary fails;
    # these calls then exercise the production catch/unavailable path itself.
    for node_id in (1, 9876):
        fresh_context = SimpleNamespace(
            node_config={"manifest-dir": str(stage_dir)}, run_config=dict(config))
        reply = association_client_app.train(request(node_id), fresh_context)
        if reply.content["metrics"]["available"] != 1:
            raise AssertionError("actual association ClientApp release is unavailable")
        if reply.content["metrics"]["noise-sd"] != sigma:
            raise AssertionError("actual association noise scale changed")
        arrays = reply.content["arrays"].to_numpy_ndarrays()
        if len(arrays) != 1 or arrays[0].tobytes() != released.tobytes():
            raise AssertionError("actual association release did not replay exactly")
    print("CHECK association %s available-replay PASS" % manifest["dp-unit"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app-dir", required=True)
    parser.add_argument("--stage-dir", required=True, nargs="+")
    args = parser.parse_args()
    sys.path.insert(0, str(Path(args.app_dir).resolve()))
    for stage_dir in args.stage_dir:
        check_stage(Path(stage_dir).resolve())


if __name__ == "__main__":
    main()
