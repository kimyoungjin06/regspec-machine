from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from regspec_machine.module_input import load_and_prepare_data


def test_load_and_prepare_data_computes_recency_years_alt(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "track": ["primary_strict", "primary_strict"],
            "pair_id": ["p1", "p1"],
            "policy_document_id": ["d1", "d2"],
            "openalex_work_id": ["w1", "w2"],
            "affiliation_label": ["academia", "industry"],
            "reference_dik": [1, 0],
            "reference_dik_evidence_use": [0, 0],
            "reference_count_dik_all_contexts": [2, 0],
            "reference_count_dik_evidence_use": [1, 0],
            "pub_year": [2010, 2010],
            "pub_date": ["2010-01-01", ""],
            "policy_published_on": ["2012-01-01", ""],
        }
    )
    dyad_path = tmp_path / "dyad.csv"
    df.to_csv(dyad_path, index=False)

    out, meta = load_and_prepare_data(dyad_base_csv=dyad_path)
    assert meta["inputs"]["dyad_base_sha256"]
    assert "recency_years_alt" in out.columns
    assert out["recency_years_alt"].dtype.kind == "f"
    assert float(out["recency_years_alt"].iloc[0]) == pytest.approx(730.0 / 365.25)
    assert pd.isna(out["recency_years_alt"].iloc[1])
