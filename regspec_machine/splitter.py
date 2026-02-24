from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd


def _policy_doc_hash_score(*, policy_document_id: str, seed: int) -> float:
    token = f"{seed}|{policy_document_id}".encode("utf-8")
    # Keep the same 12-hex slice convention as the registry lock builder script.
    return int(hashlib.sha256(token).hexdigest()[:12], 16) / float(16**12)


def assign_policy_document_holdout(
    df: pd.DataFrame,
    *,
    seed: int,
    discovery_ratio: float = 0.80,
    method: str = "hash",
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    if discovery_ratio <= 0.0 or discovery_ratio >= 1.0:
        raise ValueError("discovery_ratio must be in (0, 1)")

    docs = sorted(df["policy_document_id"].dropna().astype(str).unique())
    if len(docs) < 2:
        raise ValueError("need at least two policy documents to create holdout split")

    if method not in {"hash", "random"}:
        raise ValueError("method must be one of: hash, random")

    if method == "random":
        rng = np.random.default_rng(seed)
        shuffled = np.array(docs, dtype=object)
        rng.shuffle(shuffled)
        n_discovery = int(round(len(docs) * discovery_ratio))
        n_discovery = max(1, min(n_discovery, len(docs) - 1))
        discovery_docs = set(str(x) for x in shuffled[:n_discovery].tolist())
    else:
        discovery_docs = {
            doc for doc in docs if _policy_doc_hash_score(policy_document_id=doc, seed=seed) < discovery_ratio
        }
        # Safety fallback: keep both discovery/validation non-empty.
        if len(discovery_docs) == 0:
            discovery_docs.add(docs[0])
        if len(discovery_docs) == len(docs):
            discovery_docs.remove(docs[-1])
        n_discovery = len(discovery_docs)

    discovery_pct = int(round(discovery_ratio * 100.0))
    validation_pct = int(100 - discovery_pct)
    split_id = f"policy_document_holdout_seed{seed}_{discovery_pct}_{validation_pct}"
    out = df.copy()
    out["split_id"] = split_id
    out["split_role"] = out["policy_document_id"].astype(str).map(
        lambda x: "discovery" if x in discovery_docs else "validation"
    )

    report = {
        "split_method": f"policy_document_holdout_{method}",
        "split_id": split_id,
        "split_seed": seed,
        "discovery_ratio": discovery_ratio,
        "n_policy_docs_total": len(docs),
        "n_policy_docs_discovery": int(n_discovery),
        "n_policy_docs_validation": int(len(docs) - n_discovery),
        "n_rows_discovery": int((out["split_role"] == "discovery").sum()),
        "n_rows_validation": int((out["split_role"] == "validation").sum()),
    }
    return out, report


def apply_policy_split_file(
    df: pd.DataFrame,
    *,
    split_csv: Path,
    split_seed_fallback: int = 20260219,
    discovery_ratio_fallback: float = 0.80,
    strict: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    src = pd.read_csv(split_csv)
    required = {"policy_document_id", "split_role"}
    if not required.issubset(set(src.columns)):
        raise ValueError(f"split file missing required columns: {sorted(required - set(src.columns))}")

    split_map: Dict[str, str] = {}
    invalid_roles = 0
    conflicting_roles = 0
    split_methods: list[str] = []
    split_seeds: list[int] = []
    for _, row in src.iterrows():
        doc = str(row.get("policy_document_id", "")).strip()
        role = str(row.get("split_role", "")).strip()
        if not doc:
            continue
        if role not in {"discovery", "validation"}:
            invalid_roles += 1
            continue
        if doc in split_map and split_map[doc] != role:
            conflicting_roles += 1
            continue
        split_map[doc] = role
        method = str(row.get("split_method", "")).strip()
        if method:
            split_methods.append(method)
        try:
            seed_val = int(float(str(row.get("split_seed", "")).strip()))
            split_seeds.append(seed_val)
        except Exception:
            pass

    out = df.copy()
    roles: list[str] = []
    n_fallback = 0
    for doc in out["policy_document_id"].astype(str).tolist():
        role = split_map.get(doc)
        if role is None:
            n_fallback += 1
            if strict:
                continue
            role = (
                "discovery"
                if _policy_doc_hash_score(policy_document_id=doc, seed=split_seed_fallback)
                < discovery_ratio_fallback
                else "validation"
            )
        roles.append(role)

    if strict:
        errors: list[str] = []
        if n_fallback > 0:
            errors.append(f"missing_docs_in_split_csv={n_fallback}")
        if invalid_roles > 0:
            errors.append(f"invalid_roles_in_split_csv={invalid_roles}")
        if conflicting_roles > 0:
            errors.append(f"conflicting_roles_in_split_csv={conflicting_roles}")
        if errors:
            raise ValueError(
                "strict split lock failed: " + ", ".join(errors)
            )

    split_method = split_methods[0] if split_methods and len(set(split_methods)) == 1 else "policy_document_holdout_external_csv"
    split_seed = split_seeds[0] if split_seeds and len(set(split_seeds)) == 1 else split_seed_fallback
    split_id = f"{split_method}_seed{split_seed}_external"
    out["split_id"] = split_id
    out["split_role"] = roles

    docs = sorted(out["policy_document_id"].dropna().astype(str).unique())
    n_docs_discovery = int((out[["policy_document_id", "split_role"]].drop_duplicates()["split_role"] == "discovery").sum())
    report = {
        "split_method": split_method,
        "split_id": split_id,
        "split_source_csv": str(split_csv),
        "split_seed": split_seed,
        "split_seed_fallback": split_seed_fallback,
        "discovery_ratio_fallback": discovery_ratio_fallback,
        "n_policy_docs_total": len(docs),
        "n_policy_docs_discovery": n_docs_discovery,
        "n_policy_docs_validation": int(len(docs) - n_docs_discovery),
        "n_rows_discovery": int((out["split_role"] == "discovery").sum()),
        "n_rows_validation": int((out["split_role"] == "validation").sum()),
        "n_docs_missing_in_split_csv_fallback": n_fallback,
        "n_invalid_roles_in_split_csv": invalid_roles,
        "n_conflicting_roles_in_split_csv": conflicting_roles,
        "strict_lock_mode": int(strict),
    }
    return out, report
