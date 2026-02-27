from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


def _load_scan_script_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "modeling"
        / "run_phase_b_bikard_machine_scientist_scan.py"
    )
    spec = importlib.util.spec_from_file_location("phase_b_scan_script", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_expression_augmentation_smoke_generates_features():
    mod = _load_scan_script_module()

    data = pd.DataFrame(
        {
            "f1": [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
            "f2": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        }
    )
    build_scope_df = pd.DataFrame({"event_id": [0, 0, 1, 1, 2, 2, 3, 3]}, index=data.index)
    registry = [
        {"feature_name": "f1", "allowed_in_scan": 1},
        {"feature_name": "f2", "allowed_in_scan": 1},
    ]

    out_data, out_registry, meta = mod._augment_registry_with_expressions(
        data=data,
        registry=registry,
        build_scope_df=build_scope_df,
        mode="ms_benchmark_lite",
        max_new_features=10,
        max_base_features=2,
        max_pairs=10,
        min_nonmissing_count=1,
        min_variation_share=0.0,
        min_nonmissing_share=0.0,
    )

    expr_rows = [r for r in out_registry if str(r.get("data_source")) == "derived_expression"]
    expr_names = [str(r.get("feature_name", "")).strip() for r in expr_rows]
    expr_names = [n for n in expr_names if n]

    assert isinstance(meta, dict)
    assert int(meta.get("n_generated_features", 0)) >= 1
    assert len(expr_names) >= 1
    assert all(name.startswith("expr__") for name in expr_names)
    assert all(name in out_data.columns for name in expr_names)

