"""Fail on unclassified configuration reads in the trusted release adapters.

The inventory is a reviewed contract, not generated during tests. Dynamic reads
also pin their enclosing functions and module declarations: adding a field to an
allowlist or helper requires explicitly reviewing its identity classification.
"""
import ast
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "flower_app" / "dsflower_runner"
INVENTORY = Path(__file__).parent / "fixtures" / "semantic_field_inventory_v3.json"
MODULES = (
    "client_app", "task", "tier2_lib", "dp_harness", "model_spec", "survival",
    "segmentation", "forest_adapter", "random_forest_adapter", "boosting_adapter",
    "xgboost_adapter", "native_tree_client_app", "native_tree_validation_client_app",
    "epi_association", "association_client_app", "validation", "resampling",
    "vision", "seeding", "source_projection", "strategy", "initialisation",
)
CONTAINERS = {"cfg", "config", "run_config", "manifest", "node_manifest", "m"}
FIELD_HELPERS = {"required", "bounded_float", "bounded_int", "pinned_bool"}


def _fingerprint(node):
    return hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()


def scan_source():
    fields, dynamic, declarations = {}, {}, {}
    for module in MODULES:
        tree = ast.parse((ROOT / (module + ".py")).read_text())
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        module_has_dynamic = False
        for node in ast.walk(tree):
            target = key = None
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute) and node.func.attr == "get" and node.args:
                    target, key = node.func.value, node.args[0]
                elif isinstance(node.func, ast.Name) and node.func.id in FIELD_HELPERS and node.args:
                    target, key = ast.Name(id="manifest"), node.args[0]
            elif isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load):
                target, key = node.value, node.slice
            if target is None or not (
                    isinstance(target, ast.Name) and target.id in CONTAINERS or
                    isinstance(target, ast.Attribute) and target.attr in CONTAINERS):
                continue
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                fields.setdefault(key.value, set()).add(module)
            elif not isinstance(key, ast.Constant):
                owner = node
                while not isinstance(owner, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)):
                    owner = parents[owner]
                name = getattr(owner, "name", "<module>")
                site = module + ":" + name + ":" + ast.unparse(node)
                dynamic[site] = _fingerprint(owner)
                module_has_dynamic = True
        if module_has_dynamic:
            declarations[module] = _fingerprint(ast.Module(body=[node for node in tree.body
                if isinstance(node, (ast.Assign, ast.AnnAssign))], type_ignores=[]))
    return fields, dynamic, declarations


def test_every_literal_configuration_read_has_an_explicit_identity_classification():
    inventory = json.loads(INVENTORY.read_text())
    observed, _, _ = scan_source()
    classified = inventory["fields"]
    assert set(observed) == set(classified), (
        "Review and classify new/retired trusted configuration reads; never silently "
        "regenerate the inventory: new=%r retired=%r" %
        (sorted(set(observed) - set(classified)), sorted(set(classified) - set(observed))))
    # Every dynamically declared key has the same explicit classification as a
    # literal read; none may receive an automatic operational exemption.
    for field, classification in {**classified, **inventory["dynamic_only_expansions"]}.items():
        assert classification["category"] in ("public_semantic", "private_binding", "operational")
        assert classification["reason"] and len(classification["reason"]) >= 20, field
        paths = classification["identity_paths"]
        assert bool(paths) == (classification["category"] != "operational"), field
        prefix = "R." if classification["category"] == "public_semantic" else "B."
        assert all(path.startswith(prefix) for path in paths), field


def test_dynamic_key_families_require_reviewed_declarations_and_expansions():
    inventory = json.loads(INVENTORY.read_text())
    _, observed, declarations = scan_source()
    reviewed = inventory["dynamic_reads"]
    assert set(observed) == set(reviewed), "New dynamic configuration read requires explicit schema review"
    for site, fingerprint in observed.items():
        record = reviewed[site]
        assert record["function_sha256"] == fingerprint, (
            "Review changed dynamic-read implementation and its finite field expansions: " + site)
        assert record["authority"] and record["declared_field_families"], site
        assert all(family in inventory["field_families"] for family in record["declared_field_families"]), site
        assert all(field in inventory["fields"] for family in record["declared_field_families"]
                   for field in inventory["field_families"][family]), site
    assert declarations == inventory["dynamic_module_declarations"], (
        "A dynamic-read allowlist changed: classify its new fields before updating its reviewed declaration")


def test_field_scanner_detects_unknown_configuration_keys(tmp_path):
    # Independent oracle for the scanner: a new read cannot hide merely because
    # its key does not resemble an existing dsFlower wire prefix.
    import sys
    module = sys.modules[__name__]
    original_root, original_modules = module.ROOT, module.MODULES
    try:
        module.ROOT, module.MODULES = tmp_path, ("example",)
        (tmp_path / "example.py").write_text("def run(cfg):\n    return cfg.get('fresh_nonce')\n")
        fields, dynamic, _ = scan_source()
        assert fields == {"fresh_nonce": {"example"}}
        assert dynamic == {}
    finally:
        module.ROOT, module.MODULES = original_root, original_modules


def test_classified_semantic_paths_name_actual_closed_schema_slots():
    inventory = json.loads(INVENTORY.read_text())
    tree = ast.parse((ROOT / "seeding.py").read_text())
    declaration = next(node for node in tree.body if isinstance(node, ast.Assign)
                       and any(isinstance(target, ast.Name) and target.id == "_SCHEMAS" for target in node.targets))
    schemas = {name: set(fields.split()) for name, fields in ast.literal_eval(declaration.value).items()}
    schemas.update(mechanism={"name", "version", "profile"}, coordinate={"round", "fold"})
    roots = set(schemas) | {"runtime", "operation"}
    for record in {**inventory["fields"], **inventory["dynamic_only_expansions"]}.values():
        for path in record["identity_paths"]:
            parts = path.split(".")
            if parts[0] == "R":
                assert parts[1] in roots, path
                if len(parts) > 2 and parts[1] in schemas:
                    assert parts[2] in schemas[parts[1]], path
            else:
                assert parts[0] == "B" and parts[1] in {"geometry", "units_sha256", "effective_tensors_sha256", "subset"}, path
