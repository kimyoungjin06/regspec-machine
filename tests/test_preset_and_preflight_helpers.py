from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest


def _load_module(module_path: Path, name: str):
    if not module_path.exists():
        pytest.skip(f"module not found: {module_path}")
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load spec for {module_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _root() -> Path:
    return Path(__file__).resolve().parents[4]


def test_disk_preflight_helper_pass_and_fail() -> None:
    root = _root()
    runner = _load_module(
        root / "scripts" / "modeling" / "run_phase_b_bikard_machine_scientist_scan.py",
        "phase_b_runner_module_preflight",
    )

    ok = runner._check_disk_space_or_raise(
        probe_paths=[str(root / "data")],
        min_free_space_mb=1,
        stage="unit_test_ok",
    )
    assert ok["stage"] == "unit_test_ok"
    assert int(ok["min_free_space_mb_observed"]) >= 1
    assert isinstance(ok["checks"], list) and len(ok["checks"]) >= 1

    with pytest.raises(RuntimeError, match="insufficient free disk space"):
        runner._check_disk_space_or_raise(
            probe_paths=[str(root / "data")],
            min_free_space_mb=10**12,
            stage="unit_test_fail",
        )


def test_paired_summary_helpers(tmp_path: Path) -> None:
    root = _root()
    preset = _load_module(
        root / "scripts" / "modeling" / "run_phase_b_regspec_preset.py",
        "phase_b_preset_module_summary",
    )

    path = preset._resolve_paired_summary_path("example run id", "")
    assert path.name.startswith("phase_b_bikard_machine_scientist_paired_preset_summary_")
    assert path.suffix == ".json"

    explicit = tmp_path / "paired_summary_unit_test.json"
    payload = {"mode": "paired_nooption_singlex", "run_id": "ut", "status": "ok"}
    preset._write_paired_summary(explicit, payload)
    assert explicit.exists()
    text = explicit.read_text(encoding="utf-8")
    assert '"status": "ok"' in text
