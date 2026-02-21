from __future__ import annotations

from math import inf
from typing import Dict, List, Sequence, Tuple


def _as_float(value: object, default: float = inf) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _tier_priority(tier: str) -> int:
    t = str(tier or "").strip()
    if t == "validated_candidate":
        return 0
    if t == "support_candidate":
        return 1
    return 2


def _extract_feature_atoms(feature_name: str) -> List[str]:
    feat = str(feature_name or "").strip()
    if not feat:
        return []
    parts = feat.split("__")
    atoms: List[str] = []
    for i in range(len(parts) - 1):
        prefix = parts[i]
        if prefix in {"pa", "ext"}:
            atom = f"{prefix}__{parts[i + 1]}"
            if atom not in atoms:
                atoms.append(atom)
    if not atoms:
        for token in ("is_academia_origin", "pub_year_alt", "recency_years_alt"):
            if token in feat:
                atoms.append(token)
    if not atoms:
        atoms.append(feat)
    return atoms


def _row_rank(row: Dict[str, object]) -> Tuple[object, ...]:
    tier = str(row.get("candidate_tier", ""))
    q_val = _as_float(row.get("q_value_validation"), default=inf)
    p_val = _as_float(row.get("p_boot_validation"), default=inf)
    p_discovery = _as_float(row.get("p_boot_discovery"), default=inf)
    beta_val = _as_float(row.get("beta_validation"), default=inf)
    if beta_val == inf:
        beta_val = _as_float(row.get("beta_discovery"), default=0.0)
    return (
        _tier_priority(tier),
        q_val,
        p_val,
        p_discovery,
        -abs(beta_val),
        str(row.get("candidate_id", "")),
    )


def select_shortlist_features_from_top_models(
    top_rows: Sequence[Dict[str, object]],
    *,
    tier_mode: str = "validated_or_support",
    max_features: int = 8,
    dedupe_mode: str = "atom",
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    if tier_mode not in {"validated_only", "validated_or_support"}:
        raise ValueError("tier_mode must be one of: validated_only, validated_or_support")
    if dedupe_mode not in {"feature", "atom"}:
        raise ValueError("dedupe_mode must be one of: feature, atom")
    max_features = int(max_features)
    if max_features <= 0:
        return [], {
            "tier_mode": tier_mode,
            "dedupe_mode": dedupe_mode,
            "max_features": max_features,
            "n_input_top_rows": int(len(top_rows)),
            "n_eligible_rows": 0,
            "n_selected_features": 0,
            "n_dropped_duplicate_signature": 0,
            "selected_features": [],
            "selected_signatures": [],
        }

    allowed_tiers = {"validated_candidate"}
    if tier_mode == "validated_or_support":
        allowed_tiers.add("support_candidate")

    eligible: List[Dict[str, object]] = []
    for row in top_rows:
        tier = str(row.get("candidate_tier", "")).strip()
        if tier not in allowed_tiers:
            continue
        feature = str(row.get("key_factor", "")).strip()
        if not feature:
            continue
        eligible.append(dict(row))
    eligible.sort(key=_row_rank)

    seen_signatures: set[str] = set()
    selected: List[Dict[str, object]] = []
    selected_signatures: List[str] = []
    dropped_duplicate_signature = 0
    for rank_idx, row in enumerate(eligible, start=1):
        feature = str(row.get("key_factor", "")).strip()
        atoms = _extract_feature_atoms(feature)
        if dedupe_mode == "feature":
            signature = feature
        else:
            signature = "|".join(sorted(set(atoms)))
        if signature in seen_signatures:
            dropped_duplicate_signature += 1
            continue
        seen_signatures.add(signature)
        selected_signatures.append(signature)
        selected.append(
            {
                "feature_name": feature,
                "allowed_in_scan": 1,
                "shortlist_rank": rank_idx,
                "shortlist_signature": signature,
                "shortlist_atoms": atoms,
                "shortlist_source_tier": str(row.get("candidate_tier", "")),
                "shortlist_source_candidate_id": str(row.get("candidate_id", "")),
            }
        )
        if len(selected) >= max_features:
            break

    meta = {
        "tier_mode": tier_mode,
        "dedupe_mode": dedupe_mode,
        "max_features": max_features,
        "n_input_top_rows": int(len(top_rows)),
        "n_eligible_rows": int(len(eligible)),
        "n_selected_features": int(len(selected)),
        "n_dropped_duplicate_signature": int(dropped_duplicate_signature),
        "selected_features": [str(r.get("feature_name", "")) for r in selected],
        "selected_signatures": selected_signatures,
    }
    return selected, meta

