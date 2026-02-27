from __future__ import annotations

import pandas as pd

from regspec_machine.feature_registry import build_feature_registry


def test_build_feature_registry_keeps_suggested_controls_scannable():
    rows = []
    for event_id in range(10):  # 10 events * 2 alts = 20 rows (min nonmissing count gate)
        rows.append(
            {
                "event_id": str(event_id),
                "pa__log1p_author_count": float(event_id),
            }
        )
        rows.append(
            {
                "event_id": str(event_id),
                "pa__log1p_author_count": float(event_id + 1),
            }
        )
    df = pd.DataFrame(rows)

    registry = build_feature_registry(df, min_variation_share=0.10, min_nonmissing_share=0.80)
    by_name = {str(r.get("feature_name")): r for r in registry}

    row = by_name.get("pa__log1p_author_count")
    assert row is not None
    assert int(row.get("suggested_as_control", 0)) == 1
    assert int(row.get("allowed_in_scan", 0)) == 1

