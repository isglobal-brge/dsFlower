"""Explicit-path sibling loading for the standalone trusted Hook entry point."""
import hashlib
import importlib.util
import os
import sys


def load_sibling(name, caller_file):
    """Provide relative imports without using an upload-controlled sys.path.

    The private package has exactly one, co-located search directory. Validate
    cached entries too: an existing module never overrides this origin rule.
    """
    if not isinstance(name, str) or not name.isidentifier():
        raise ValueError("trusted sibling module name is invalid")
    root = os.path.realpath(os.path.dirname(caller_file))
    package = "_dsflower_node_" + hashlib.sha256(root.encode()).hexdigest()
    prefix = package + "."
    for key, module in tuple(sys.modules.items()):
        if key == package:
            expected = os.path.join(root, "__init__.py")
            if list(getattr(module, "__path__", ())) != [root]:
                raise RuntimeError("trusted package search path has an invalid origin")
        elif key.startswith(prefix):
            relative = key[len(prefix):].split(".")
            expected = os.path.join(root, *relative) + ".py"
        else:
            continue
        if os.path.realpath(getattr(module, "__file__", "")) != expected:
            raise RuntimeError("trusted module name is already bound to another path")

    def execute(qualified, path, *, package_path=None):
        spec = importlib.util.spec_from_file_location(
            qualified, path, submodule_search_locations=package_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[qualified] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(qualified, None)
            raise
        return module

    if package not in sys.modules:
        execute(package, os.path.join(root, "__init__.py"), package_path=[root])
    qualified = prefix + name
    if qualified in sys.modules:
        return sys.modules[qualified]
    return execute(qualified, os.path.join(root, name + ".py"))
