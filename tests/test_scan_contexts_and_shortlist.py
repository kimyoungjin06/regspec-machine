import pandas as pd

from regspec_machine.search_engine import ScanConfig, run_key_factor_scan
from regspec_machine.shortlist import select_shortlist_features_from_top_models


def _mock_scan_df(include_y_evidence: bool = True) -> pd.DataFrame:
    rows = []
    for split_role, prefix in (("discovery", "d"), ("validation", "v")):
        for event_num, policy_doc, pair_id in ((1, f"{prefix}doc1", f"{prefix}p1"), (2, f"{prefix}doc2", f"{prefix}p2")):
            event_id = f"{prefix}e{event_num}"
            rows.append(
                {
                    "track": "primary",
                    "split_id": "s1",
                    "split_role": split_role,
                    "event_id": event_id,
                    "pair_id": pair_id,
                    "policy_document_id": policy_doc,
                    "x_feat": 0.0,
                    "y_all": 1,
                    "y_evidence": 1,
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
                    "y_all": 0,
                    "y_evidence": 0,
                }
            )
    df = pd.DataFrame(rows)
    if not include_y_evidence:
        df = df.drop(columns=["y_evidence"])
    return df


def test_run_scan_logs_skipped_contexts_and_y_col() -> None:
    df = _mock_scan_df(include_y_evidence=False)
    feature_registry = [{"feature_name": "x_feat", "allowed_in_scan": 1}]
    config = ScanConfig(
        run_id="t_contexts",
        include_base_controls=False,
        min_informative_events_estimable=99,
        min_policy_docs_informative_estimable=1,
        min_informative_events_validated=99,
        min_policy_docs_informative_validated=1,
        contexts=(
            ("ctx_all", "y_all"),
            ("ctx_all", "y_all"),
            ("ctx_missing", "y_evidence"),
            ("", "y_all"),
            ("ctx_empty", ""),
            ("only_one",),
        ),
    )

    scan_rows, top_rows, search_log = run_key_factor_scan(df=df, feature_registry=feature_registry, config=config)

    assert scan_rows
    assert all(str(r.get("y_col")) == "y_all" for r in scan_rows)
    assert top_rows
    assert all(str(r.get("y_col")) == "y_all" for r in top_rows)

    skipped = [r for r in search_log if r.get("status") == "skipped_context"]
    reason_codes = {str(r.get("reason_code")) for r in skipped}
    assert "duplicate_context_definition" in reason_codes
    assert "missing_context_y_column" in reason_codes
    assert "invalid_context_definition" in reason_codes


def test_validation_gate_map_by_y_changes_gate_behavior() -> None:
    df = _mock_scan_df(include_y_evidence=True)
    feature_registry = [{"feature_name": "x_feat", "allowed_in_scan": 1}]
    config = ScanConfig(
        run_id="t_y_gate",
        include_base_controls=False,
        min_informative_events_estimable=99,
        min_policy_docs_informative_estimable=1,
        min_informative_events_validated=99,
        min_policy_docs_informative_validated=5,
        validated_min_policy_docs_by_y={"y_evidence": 1},
        max_top1_policy_doc_share=1.0,
        contexts=(("ctx_all", "y_all"), ("ctx_evidence", "y_evidence")),
    )

    scan_rows, _, _ = run_key_factor_scan(df=df, feature_registry=feature_registry, config=config)
    validation_rows = {str(r.get("y_col")): r for r in scan_rows if str(r.get("split_role")) == "validation"}

    assert validation_rows["y_all"]["reason_code"] == "validation_gate_infeasible_policy_docs"
    assert "gate=5" in str(validation_rows["y_all"]["reason_detail"])
    assert validation_rows["y_evidence"]["reason_code"] == "validation_low_informative_events"


def test_shortlist_atom_dedupe_keeps_best_candidate() -> None:
    top_rows = [
        {
            "candidate_id": "c1",
            "key_factor": "ext__impact_index",
            "candidate_tier": "validated_candidate",
            "q_value_validation": 0.01,
            "p_boot_validation": 0.01,
            "p_boot_discovery": 0.01,
            "beta_validation": 0.2,
        },
        {
            "candidate_id": "c2",
            "key_factor": "ext__impact_index__sq",
            "candidate_tier": "validated_candidate",
            "q_value_validation": 0.02,
            "p_boot_validation": 0.02,
            "p_boot_discovery": 0.02,
            "beta_validation": 0.5,
        },
        {
            "candidate_id": "c3",
            "key_factor": "pa__log1p_author_count",
            "candidate_tier": "support_candidate",
            "q_value_validation": None,
            "p_boot_validation": None,
            "p_boot_discovery": 0.04,
            "beta_validation": None,
            "beta_discovery": 0.1,
        },
    ]

    selected, meta = select_shortlist_features_from_top_models(
        top_rows,
        tier_mode="validated_or_support",
        max_features=3,
        dedupe_mode="atom",
    )
    selected_features = [str(r.get("feature_name")) for r in selected]

    assert "ext__impact_index" in selected_features
    assert "ext__impact_index__sq" not in selected_features
    assert "pa__log1p_author_count" in selected_features
    assert int(meta["n_selected_features"]) == 2
    assert int(meta["n_dropped_duplicate_signature"]) >= 1
