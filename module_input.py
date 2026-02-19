from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

REQUIRED_COLUMNS = (
    "track",
    "pair_id",
    "policy_document_id",
    "openalex_work_id",
    "affiliation_label",
    "reference_dik",
    "reference_dik_evidence_use",
    "reference_count_dik_all_contexts",
    "reference_count_dik_evidence_use",
    "pub_year",
    "pub_date",
    "policy_published_on",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_json(payload: object) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def get_git_commit(cwd: Optional[Path] = None) -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(cwd) if cwd else None,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out
    except Exception:
        return ""


def parse_date(value: object) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10], fmt)
        except Exception:
            continue
    return None


def _validate_schema(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")


def _prepare_base_columns(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["pair_id"] = d["pair_id"].astype(str)
    d["policy_document_id"] = d["policy_document_id"].astype(str)
    d["openalex_work_id"] = d["openalex_work_id"].astype(str)
    d["event_id"] = d["pair_id"] + "|" + d["policy_document_id"]
    d["is_academia_origin"] = (d["affiliation_label"].astype(str) == "academia").astype(int)

    d["pub_year_alt"] = pd.to_numeric(d["pub_year"], errors="coerce")
    d["y_all"] = pd.to_numeric(d["reference_dik"], errors="coerce").fillna(0).astype(int)
    d["y_evidence"] = pd.to_numeric(d["reference_dik_evidence_use"], errors="coerce").fillna(0).astype(int)
    d["count_all"] = (
        pd.to_numeric(d["reference_count_dik_all_contexts"], errors="coerce").fillna(0).astype(int)
    )
    d["count_evidence"] = (
        pd.to_numeric(d["reference_count_dik_evidence_use"], errors="coerce").fillna(0).astype(int)
    )

    recency_years: List[Optional[float]] = []
    for _, row in d.iterrows():
        pub_d = parse_date(row.get("pub_date", ""))
        policy_d = parse_date(row.get("policy_published_on", ""))
        if pub_d is None or policy_d is None:
            recency_years.append(None)
            continue
        recency_years.append((policy_d - pub_d).days / 365.25)
    d["recency_years_alt"] = recency_years
    return d


def _prefixed_merge(
    base_df: pd.DataFrame,
    *,
    table_path: Path,
    key_col: str,
    prefix: str,
    drop_cols: Sequence[str],
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    src = pd.read_csv(table_path, low_memory=False)
    if key_col not in src.columns:
        raise ValueError(f"{table_path} missing key column: {key_col}")

    src[key_col] = src[key_col].astype(str)
    src = src.drop_duplicates(subset=[key_col], keep="first")
    keep_cols = [c for c in src.columns if c not in set(drop_cols) and c != key_col]
    rename_map = {c: f"{prefix}{c}" for c in keep_cols}
    slim = src[[key_col, *keep_cols]].rename(columns=rename_map)
    merged = base_df.merge(slim, on=key_col, how="left")

    report = {
        "path": str(table_path),
        "n_rows_source": int(len(src)),
        "n_rows_merged": int(len(merged)),
        "n_cols_added": int(len(keep_cols)),
    }
    return merged, report


def load_and_prepare_data(
    *,
    dyad_base_csv: Path,
    extension_feature_csv: Optional[Path] = None,
    phase_a_covariates_csv: Optional[Path] = None,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    raw = pd.read_csv(dyad_base_csv, low_memory=False)
    _validate_schema(raw)
    data = _prepare_base_columns(raw)

    merge_report: Dict[str, object] = {}
    if extension_feature_csv is not None:
        data, rep = _prefixed_merge(
            data,
            table_path=extension_feature_csv,
            key_col="openalex_work_id",
            prefix="ext__",
            drop_cols=(
                "pair_id",
                "paper_key",
                "affiliation_label",
                "policy_cited_5y",
                "policy_cite_count_5y_derived",
            ),
        )
        merge_report["metadata_extension"] = rep

    if phase_a_covariates_csv is not None:
        data, rep = _prefixed_merge(
            data,
            table_path=phase_a_covariates_csv,
            key_col="openalex_work_id",
            prefix="pa__",
            drop_cols=(
                "pair_id",
                "legacy_magid",
                "matched_raw",
                "matched_api",
                "matched_openalex",
                "matched_for_phase",
                "bridge_source",
                "bridge_match_confidence",
                "doi",
                "pmid",
                "pmcid",
                "primary_field",
                "primary_domain",
                "primary_subfield",
                "primary_topic",
                "journal_or_source",
                "work_type_openalex",
                "language",
                "affiliation_label",
                "label_confidence",
                "anchor_rule",
                "policy_cited_3y",
                "policy_cited_5y",
                "policy_cited_10y",
                "policy_cite_count_3y",
                "policy_cite_count_5y",
                "policy_cite_count_10y",
                "time_to_first_policy_citation_days",
                "eligible_3y",
                "eligible_5y",
                "eligible_10y",
            ),
        )
        merge_report["phase_a_covariates"] = rep

    input_report: Dict[str, object] = {
        "dyad_base_csv": str(dyad_base_csv),
        "dyad_base_sha256": sha256_file(dyad_base_csv),
        "n_rows": int(len(data)),
        "n_columns": int(len(data.columns)),
    }
    if extension_feature_csv is not None:
        input_report["extension_feature_csv"] = str(extension_feature_csv)
        input_report["extension_feature_sha256"] = sha256_file(extension_feature_csv)
    if phase_a_covariates_csv is not None:
        input_report["phase_a_covariates_csv"] = str(phase_a_covariates_csv)
        input_report["phase_a_covariates_sha256"] = sha256_file(phase_a_covariates_csv)

    return data, {"inputs": input_report, "merge_report": merge_report}

