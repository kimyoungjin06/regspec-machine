from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import types


def test_root_init_imports_in_standalone_mode() -> None:
    root_init = Path(__file__).resolve().parents[1] / "__init__.py"
    assert root_init.is_file()

    module_name = "regspec_machine_root_wrapper_ut"
    spec = importlib.util.spec_from_file_location(module_name, root_init)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    try:
        spec.loader.exec_module(mod)
        assert isinstance(mod, types.ModuleType)
        assert hasattr(mod, "RunRequestContract")
        assert hasattr(mod, "run_key_factor_scan")
    finally:
        sys.modules.pop(module_name, None)

