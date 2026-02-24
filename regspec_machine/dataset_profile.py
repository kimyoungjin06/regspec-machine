from __future__ import annotations

from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from pandas import CategoricalDtype


def _safe_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


def _is_id_like(*, col_name: str, unique_share: float) -> bool:
    text = str(col_name).strip().lower()
    if not text:
        return False
    if unique_share >= 0.98 and any(token in text for token in ("id", "uuid", "doi", "key")):
        return True
    if text.endswith("_id") or text.startswith("id_"):
        return True
    return False


def _dtype_group(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "bool"
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    nonmissing = int(series.notna().sum())
    if nonmissing > 0 and pd.api.types.is_object_dtype(series):
        sample_texts = [str(v).strip() for v in series.dropna().astype(str).head(50).tolist()]
        looks_datetime = any(
            bool(re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", text))
            or ("T" in text and ":" in text)
            for text in sample_texts
        )
        if looks_datetime:
            parsed = pd.to_datetime(series, errors="coerce", utc=True)
            if int(parsed.notna().sum()) / float(nonmissing) >= 0.95:
                return "datetime_like"
    if isinstance(series.dtype, CategoricalDtype):
        return "categorical"
    return "string"


def _build_column_profile(
    *,
    name: str,
    series: pd.Series,
    n_rows: int,
) -> Dict[str, Any]:
    nonmissing_count = int(series.notna().sum())
    nonmissing_share = (nonmissing_count / float(n_rows)) if n_rows > 0 else 0.0
    missing_share = 1.0 - nonmissing_share
    unique_count = int(series.dropna().nunique())
    unique_share = (unique_count / float(nonmissing_count)) if nonmissing_count > 0 else 0.0
    group = _dtype_group(series)
    samples = [str(v) for v in series.dropna().astype(str).head(3).tolist()]

    out: Dict[str, Any] = {
        "name": str(name),
        "dtype": str(series.dtype),
        "dtype_group": group,
        "nonmissing_count": int(nonmissing_count),
        "nonmissing_share": float(nonmissing_share),
        "missing_share": float(missing_share),
        "unique_count": int(unique_count),
        "unique_share": float(unique_share),
        "sample_values": samples,
    }

    if group in {"numeric", "bool"}:
        numeric = pd.to_numeric(series, errors="coerce")
        valid = numeric.dropna()
        if not valid.empty:
            vmin = _safe_float(valid.min())
            vmax = _safe_float(valid.max())
            vmean = _safe_float(valid.mean())
            vstd = _safe_float(valid.std(ddof=0))
            zero_share = _safe_float((valid == 0).mean())
            is_binary_numeric = bool(valid.isin([0, 1]).all() and valid.nunique() <= 2)
            is_nonnegative_integer = bool((valid >= 0).all() and ((valid.round() - valid).abs() < 1e-12).all())

            out.update(
                {
                    "min": vmin,
                    "max": vmax,
                    "mean": vmean,
                    "std": vstd,
                    "zero_share": zero_share,
                    "is_binary_numeric": is_binary_numeric,
                    "is_nonnegative_integer": is_nonnegative_integer,
                }
            )
            if is_binary_numeric:
                out["positive_share"] = _safe_float(valid.mean())
    return out


def _select_y_candidates(columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for col in columns:
        nonmissing_share = float(col.get("nonmissing_share") or 0.0)
        if nonmissing_share < 0.60:
            continue
        unique_share = float(col.get("unique_share") or 0.0)
        if _is_id_like(col_name=str(col.get("name")), unique_share=unique_share):
            continue

        dtype_group = str(col.get("dtype_group", ""))
        if dtype_group in {"numeric", "bool"} and bool(col.get("is_binary_numeric")):
            rows.append(
                {
                    "name": str(col.get("name")),
                    "y_type": "binary",
                    "nonmissing_share": nonmissing_share,
                    "unique_count": int(col.get("unique_count") or 0),
                    "priority": 3.0 + nonmissing_share,
                }
            )
            continue
        if dtype_group in {"numeric", "bool"} and bool(col.get("is_nonnegative_integer")):
            unique_count = int(col.get("unique_count") or 0)
            if unique_count >= 4:
                rows.append(
                    {
                        "name": str(col.get("name")),
                        "y_type": "count",
                        "nonmissing_share": nonmissing_share,
                        "unique_count": unique_count,
                        "priority": 2.0 + nonmissing_share,
                    }
                )
                continue
        if dtype_group in {"numeric", "bool"}:
            std = _safe_float(col.get("std"))
            if std is not None and std > 0:
                rows.append(
                    {
                        "name": str(col.get("name")),
                        "y_type": "continuous",
                        "nonmissing_share": nonmissing_share,
                        "unique_count": int(col.get("unique_count") or 0),
                        "priority": 1.0 + nonmissing_share,
                    }
                )
    rows_sorted = sorted(rows, key=lambda r: (float(r["priority"]), int(r["unique_count"])), reverse=True)
    return rows_sorted[:12]


def _select_x_candidates(columns: List[Dict[str, Any]], *, n_rows: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for col in columns:
        name = str(col.get("name", ""))
        nonmissing_share = float(col.get("nonmissing_share") or 0.0)
        if nonmissing_share < 0.60:
            continue
        unique_count = int(col.get("unique_count") or 0)
        unique_share = float(col.get("unique_share") or 0.0)
        if _is_id_like(col_name=name, unique_share=unique_share):
            continue

        dtype_group = str(col.get("dtype_group", ""))
        if dtype_group in {"numeric", "bool"}:
            std = _safe_float(col.get("std"))
            if std is None or std <= 0:
                continue
            rows.append(
                {
                    "name": name,
                    "x_type": "numeric",
                    "nonmissing_share": nonmissing_share,
                    "unique_count": unique_count,
                    "priority": 2.0 + nonmissing_share,
                }
            )
            continue

        if dtype_group == "datetime_like":
            rows.append(
                {
                    "name": name,
                    "x_type": "time",
                    "nonmissing_share": nonmissing_share,
                    "unique_count": unique_count,
                    "priority": 1.7 + nonmissing_share,
                }
            )
            continue

        max_unique = max(10, int(min(40, n_rows * 0.20)))
        if 2 <= unique_count <= max_unique:
            rows.append(
                {
                    "name": name,
                    "x_type": "categorical",
                    "nonmissing_share": nonmissing_share,
                    "unique_count": unique_count,
                    "priority": 1.4 + nonmissing_share,
                }
            )

    rows_sorted = sorted(
        rows,
        key=lambda r: (float(r["priority"]), -int(r["unique_count"])),
        reverse=True,
    )
    return rows_sorted[:40]


def _numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _to_time_numeric(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce", utc=True)
    # nanosecond epoch; keep float for correlation usage.
    return pd.Series(ts.view("int64"), index=series.index, dtype="float64").where(ts.notna(), other=float("nan"))


def _pair_score(
    *,
    df: pd.DataFrame,
    y_col: str,
    y_type: str,
    x_col: str,
    x_type: str,
) -> Optional[Dict[str, Any]]:
    if y_col == x_col:
        return None

    y_raw = df[y_col]
    x_raw = df[x_col]
    if x_type == "numeric":
        xv = _numeric_series(x_raw)
    elif x_type == "time":
        xv = _to_time_numeric(x_raw)
    else:
        xv = x_raw.astype(str).where(x_raw.notna(), other=pd.NA)

    if y_type == "binary":
        yv_num = _numeric_series(y_raw)
        yv = yv_num.where(yv_num.isin([0, 1]), other=pd.NA)
    else:
        yv = _numeric_series(y_raw)

    pair = pd.DataFrame({"y": yv, "x": xv}).dropna()
    n = int(len(pair))
    if n < 30:
        return None

    score = None
    signal = ""
    if y_type == "binary" and x_type in {"numeric", "time"}:
        g1 = pair.loc[pair["y"] == 1, "x"]
        g0 = pair.loc[pair["y"] == 0, "x"]
        if g1.empty or g0.empty:
            return None
        mean1 = _safe_float(g1.mean()) or 0.0
        mean0 = _safe_float(g0.mean()) or 0.0
        std = _safe_float(pair["x"].std(ddof=0)) or 0.0
        score = abs(mean1 - mean0) / (std + 1e-12)
        signal = f"group-mean gap (y=1:{mean1:.3g}, y=0:{mean0:.3g})"
    elif y_type == "binary" and x_type == "categorical":
        group_rate = pair.groupby("x")["y"].mean()
        if len(group_rate) < 2:
            return None
        max_rate = _safe_float(group_rate.max()) or 0.0
        min_rate = _safe_float(group_rate.min()) or 0.0
        score = abs(max_rate - min_rate)
        signal = f"group positive-rate spread ({min_rate:.3g}..{max_rate:.3g})"
    elif y_type in {"count", "continuous"} and x_type in {"numeric", "time"}:
        corr = _safe_float(pair["y"].corr(pair["x"]))
        if corr is None:
            return None
        score = abs(corr)
        signal = f"abs corr = {corr:.3g}"
    elif y_type in {"count", "continuous"} and x_type == "categorical":
        group_mean = pair.groupby("x")["y"].mean()
        if len(group_mean) < 2:
            return None
        y_std = _safe_float(pair["y"].std(ddof=0)) or 0.0
        spread = (_safe_float(group_mean.max()) or 0.0) - (_safe_float(group_mean.min()) or 0.0)
        score = abs(spread) / (y_std + 1e-12)
        signal = f"group mean spread/std = {score:.3g}"
    else:
        return None

    if score is None:
        return None
    return {
        "y_col": y_col,
        "y_type": y_type,
        "x_col": x_col,
        "x_type": x_type,
        "score": float(score),
        "support_rows": n,
        "pair_nonmissing_share": float(n / float(len(df))) if len(df) > 0 else 0.0,
        "signal_summary": signal,
        "label": f"{y_col} ~ {x_col}",
    }


def _build_question_seeds(
    *,
    df: pd.DataFrame,
    y_candidates: List[Dict[str, Any]],
    x_candidates: List[Dict[str, Any]],
    top_n: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    y_pool = y_candidates[:8]
    x_pool = x_candidates[:24]
    for y in y_pool:
        y_col = str(y["name"])
        y_type = str(y["y_type"])
        for x in x_pool:
            x_col = str(x["name"])
            x_type = str(x["x_type"])
            score_row = _pair_score(df=df, y_col=y_col, y_type=y_type, x_col=x_col, x_type=x_type)
            if score_row is None:
                continue
            rows.append(score_row)

    rows_sorted = sorted(rows, key=lambda r: (float(r["score"]), float(r["pair_nonmissing_share"])), reverse=True)
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows_sorted[: max(1, int(top_n))], start=1):
        copied = dict(row)
        copied["rank"] = int(idx)
        out.append(copied)
    return out


def profile_dataset_csv(
    *,
    dataset_path: Path,
    sample_rows: int = 20000,
    top_n: int = 20,
) -> Dict[str, Any]:
    nrows = max(1, int(sample_rows))
    df = pd.read_csv(dataset_path, nrows=nrows, low_memory=False)
    if df.empty:
        raise ValueError(f"dataset has no rows: {dataset_path}")

    row_count = int(len(df))
    column_count = int(len(df.columns))
    columns: List[Dict[str, Any]] = []
    for col_name in df.columns:
        columns.append(
            _build_column_profile(
                name=str(col_name),
                series=df[col_name],
                n_rows=row_count,
            )
        )

    columns_sorted = sorted(columns, key=lambda c: float(c.get("missing_share") or 0.0), reverse=True)
    y_candidates = _select_y_candidates(columns)
    x_candidates = _select_x_candidates(columns, n_rows=row_count)
    question_seeds = _build_question_seeds(
        df=df,
        y_candidates=y_candidates,
        x_candidates=x_candidates,
        top_n=max(1, int(top_n)),
    )

    charts = {
        "missing_share_top": [
            {"column": c["name"], "missing_share": float(c.get("missing_share") or 0.0)}
            for c in columns_sorted[:12]
        ],
        "unique_share_top": [
            {"column": c["name"], "unique_share": float(c.get("unique_share") or 0.0)}
            for c in sorted(columns, key=lambda c: float(c.get("unique_share") or 0.0), reverse=True)[:12]
        ],
        "seed_score_top": [
            {"label": s["label"], "score": float(s.get("score") or 0.0)} for s in question_seeds[:12]
        ],
    }

    return {
        "dataset_path": str(dataset_path),
        "row_count": row_count,
        "column_count": column_count,
        "sample_rows_used": row_count,
        "columns": columns,
        "y_candidates": y_candidates,
        "x_candidates": x_candidates,
        "question_seeds": question_seeds,
        "charts": charts,
    }
