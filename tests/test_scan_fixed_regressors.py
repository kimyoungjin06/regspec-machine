import re

import pandas as pd
import pytest

from regspec_machine.search_engine import ScanConfig, run_key_factor_scan


def _mock_df_with_anchor() -> pd.DataFrame:
    rows = []
    for split_role, prefix in (("discovery", "d"), ("validation", "v")):
        for event_num, policy_doc, pair_id in (
            (1, f"{prefix}doc1", f"{prefix}p1"),
            (2, f"{prefix}doc2", f"{prefix}p2"),
        ):
            event_id = f"{prefix}e{event_num}"
            # Chosen alt is x_feat=0 (y=1) to keep within-event x-diff constant.
            # Anchor differs across events to avoid collinearity with x_feat.
            anchor_chosen = 0.0 if event_num == 1 else 1.0
            rows.append(
                {
                    "track": "primary",
                    "split_id": "s1",
                    "split_role": split_role,
                    "event_id": event_id,
                    "pair_id": pair_id,
                    "policy_document_id": policy_doc,
                    "x_feat": 0.0,
                    "anchor": anchor_chosen,
                    "y_all": 1,
                }
            )
            rows.append(
                {
                    "track": "primary",
                    "split_id": "s1",
                    "split_role": split_role,
                    "event_id": event_id,
                    "pair_id": pair_id,
                    "policy_document_id": policy_doc,
                    "x_feat": 1.0,
                    "anchor": 0.0,
                    "y_all": 0,
                }
            )
    return pd.DataFrame(rows)


def test_fixed_regressors_anchor_updates_spec_id_and_control_set() -> None:
    df = _mock_df_with_anchor()
    feature_registry = [{"feature_name": "x_feat", "allowed_in_scan": 1}]
    config = ScanConfig(
        run_id="t_anchor",
        include_base_controls=False,
        min_informative_events_estimable=99,  # force precheck skip (avoid bootstrap work)
        min_policy_docs_informative_estimable=1,
        min_informative_events_validated=99,
        min_policy_docs_informative_validated=1,
        contexts=(("ctx_all", "y_all"),),
        fixed_regressors=("anchor",),
    )

    scan_rows, _, _ = run_key_factor_scan(df=df, feature_registry=feature_registry, config=config)
    assert scan_rows
    for row in scan_rows:
        assert str(row.get("control_set")) == "x_feat+anchor"
        spec_id = str(row.get("spec_id"))
        assert spec_id.startswith("clogit_key_only__anchored_anchor_h")
        assert re.match(r"^clogit_key_only__anchored_anchor_h[0-9a-f]{8}$", spec_id)


def test_fixed_regressors_missing_raises() -> None:
    df = _mock_df_with_anchor()
    feature_registry = [{"feature_name": "x_feat", "allowed_in_scan": 1}]
    config = ScanConfig(
        run_id="t_anchor_missing",
        include_base_controls=False,
        min_informative_events_estimable=1,
        min_policy_docs_informative_estimable=1,
        min_informative_events_validated=1,
        min_policy_docs_informative_validated=1,
        contexts=(("ctx_all", "y_all"),),
        fixed_regressors=("does_not_exist",),
    )
    with pytest.raises(ValueError, match="fixed_regressors missing"):
        run_key_factor_scan(df=df, feature_registry=feature_registry, config=config)

