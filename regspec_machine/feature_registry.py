from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

OUTCOME_NAME_PATTERNS = (
    "reference_",
    "policy_cited",
    "policy_cite_count",
    "time_to_first_policy_citation",
    "ctx_n_evidence_use",
    "ctx_share_evidence_use",
    "first_policy_lag",
    "y_all",
    "y_evidence",
    "count_all",
    "count_evidence",
)

IDENTIFIER_COLUMNS = {
    "track",
    "pair_id",
    "policy_document_id",
    "event_id",
    "openalex_work_id",
    "paper_title",
    "affiliation_label",
    "corresponding_institutions_actual",
    "first_author_institutions_actual",
    "pub_date",
    "policy_published_on",
    "primary_domain",
}

CONTROL_ONLY_FEATURES = {
    "pub_year_alt",
    "recency_years_alt",
    "pa__author_count",
    "pa__log1p_author_count",
}

PHASE_A_PRETREATMENT_COLUMNS = {
    "pub_year",
    "author_count",
    "log1p_author_count",
    "countries_distinct_count",
    "institutions_distinct_count",
    "corresponding_authors_count",
    "is_pair_complete_openalex",
    "is_pair_discordant_openalex",
    "pair_match_count_openalex",
    "include_in_primary",
    "include_in_sensitivity",
}

# Conservative whitelist for extension-table columns that are safe pre-treatment.
# Unknown extension columns are blocked by default in generated registries.
EXTENSION_PRETREATMENT_COLUMNS = {
    # Intentionally empty by default; add columns only after timing audit.
}

DYAD_BASE_PRETREATMENT_COLUMNS = {
    "pub_year_alt",
    "recency_years_alt",
    "is_academia_origin",
}


def _is_outcome_like(name: str) -> bool:
    lower = name.lower()
    return any(token in lower for token in OUTCOME_NAME_PATTERNS)


def is_outcome_like(name: str) -> bool:
    """Public wrapper for outcome-like column detection."""
    return _is_outcome_like(name)


def _classify_timing(name: str) -> str:
    if _is_outcome_like(name):
        return "post_treatment_or_outcome_proxy"
    if name.startswith("ext__"):
        base_name = name.replace("ext__", "", 1)
        if base_name in EXTENSION_PRETREATMENT_COLUMNS:
            return "pre_treatment"
        return "unknown"
    if name.startswith("pa__"):
        base_name = name.replace("pa__", "", 1)
        if base_name in PHASE_A_PRETREATMENT_COLUMNS:
            return "pre_treatment"
        return "unknown"
    if name in DYAD_BASE_PRETREATMENT_COLUMNS:
        return "pre_treatment"
    return "unknown"


def classify_timing(name: str) -> str:
    """Public wrapper for pre/post/unknown timing classification."""
    return _classify_timing(name)


def _variation_metrics(
    df: pd.DataFrame, *, feature_col: str, event_col: str = "event_id"
) -> Tuple[float, float, int, int]:
    view = df[[event_col, feature_col]].copy()
    view[feature_col] = pd.to_numeric(view[feature_col], errors="coerce")

    n_two_alt_events = 0
    n_nonmissing_events = 0
    n_variation_events = 0
    for _, g in view.groupby(event_col, dropna=False):
        if len(g) != 2:
            continue
        n_two_alt_events += 1
        vals = g[feature_col]
        if vals.isna().any():
            continue
        n_nonmissing_events += 1
        if float(vals.iloc[0]) != float(vals.iloc[1]):
            n_variation_events += 1

    share_nonmissing = (
        float(n_nonmissing_events) / float(n_two_alt_events) if n_two_alt_events > 0 else 0.0
    )
    share_variation = (
        float(n_variation_events) / float(n_nonmissing_events) if n_nonmissing_events > 0 else 0.0
    )
    return share_variation, share_nonmissing, n_variation_events, n_two_alt_events


def within_event_variation_metrics(
    df: pd.DataFrame, *, feature_col: str, event_col: str = "event_id"
) -> Tuple[float, float, int, int]:
    """Compute within-event variation and nonmissing shares for dyad (2-alt) events."""
    return _variation_metrics(df, feature_col=feature_col, event_col=event_col)


def _variation_label(share_variation: float) -> str:
    if share_variation >= 0.60:
        return "high"
    if share_variation >= 0.30:
        return "medium"
    return "low"


def within_event_variation_label(share_variation: float) -> str:
    """Public wrapper for the qualitative within-event variation label."""
    return _variation_label(share_variation)


def _default_transform(series: pd.Series) -> str:
    numeric = pd.to_numeric(series, errors="coerce")
    uniq = int(numeric.dropna().nunique())
    if uniq <= 2:
        return "none"
    return "zscore_within_track"


def build_feature_registry(
    df: pd.DataFrame,
    *,
    min_variation_share: float = 0.10,
    min_nonmissing_share: float = 0.80,
    max_features: int = 0,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for col in sorted(df.columns):
        if col in IDENTIFIER_COLUMNS:
            continue
        if _is_outcome_like(col):
            continue

        numeric = pd.to_numeric(df[col], errors="coerce")
        nonmissing_count = int(numeric.notna().sum())
        if nonmissing_count < 20:
            continue
        if int(numeric.dropna().nunique()) < 2:
            continue

        if col.startswith("ext__"):
            data_source = "metadata_extension"
        elif col.startswith("pa__"):
            data_source = "phase_a_model_input"
        else:
            data_source = "dyad_base"

        timing_label = _classify_timing(col)
        share_variation, share_nonmissing, n_var_events, n_two_alt_events = _variation_metrics(
            df, feature_col=col
        )
        variation_expected = _variation_label(share_variation)
        transform = _default_transform(df[col])

        role = "control_only" if col in CONTROL_ONLY_FEATURES else "key_factor_candidate"
        block_reasons: List[str] = []
        if timing_label != "pre_treatment":
            block_reasons.append("timing_not_pre_treatment")
        if share_variation < min_variation_share:
            block_reasons.append("low_within_event_variation")
        if share_nonmissing < min_nonmissing_share:
            block_reasons.append("low_nonmissing_share")
        if role != "key_factor_candidate":
            block_reasons.append("control_only_feature")
        allowed = 1 if len(block_reasons) == 0 else 0

        rows.append(
            {
                "feature_name": col,
                "data_source": data_source,
                "timing_label": timing_label,
                "role": role,
                "allowed_in_scan": allowed,
                "within_event_variation_expected": variation_expected,
                "share_events_with_variation": round(share_variation, 6),
                "share_events_nonmissing": round(share_nonmissing, 6),
                "n_events_with_variation": n_var_events,
                "n_two_alt_events": n_two_alt_events,
                "transform": transform,
                "missing_policy": "drop",
                "block_reasons": block_reasons,
            }
        )

    if max_features > 0:
        rows = rows[:max_features]
    return rows


def load_feature_registry(path: Path) -> List[Dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows_raw: object = payload
    if isinstance(payload, dict):
        rows_raw = payload.get("feature_registry", [])
    if not isinstance(rows_raw, list):
        raise ValueError(f"feature registry must be a JSON list or dict.feature_registry: {path}")
    out: List[Dict[str, object]] = []
    for row in rows_raw:
        if not isinstance(row, dict):
            continue
        if "feature_name" not in row:
            continue
        out.append(row)
    return out
