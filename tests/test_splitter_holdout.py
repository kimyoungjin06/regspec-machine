import pandas as pd

from regspec_machine.splitter import assign_policy_document_holdout


def test_policy_document_split_is_disjoint() -> None:
    df = pd.DataFrame(
        {
            "policy_document_id": ["d1", "d1", "d2", "d2", "d3", "d3"],
            "pair_id": ["p1", "p2", "p1", "p2", "p1", "p2"],
        }
    )

    out, meta = assign_policy_document_holdout(df, seed=20260220, discovery_ratio=0.67)

    assert set(out["split_role"].unique()) == {"discovery", "validation"}
    assert out.groupby("policy_document_id")["split_role"].nunique().max() == 1
    assert meta["n_policy_docs_total"] == 3
