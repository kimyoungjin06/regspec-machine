from __future__ import annotations

import math
import warnings
from typing import List, Optional, Sequence, Tuple

import pandas as pd
from statsmodels.discrete.conditional_models import ConditionalLogit


def standardize_inplace(df: pd.DataFrame, cols: Sequence[str]) -> None:
    for col in cols:
        if col not in df.columns:
            continue
        vec = pd.to_numeric(df[col], errors="coerce")
        if vec.dropna().nunique() <= 2:
            df[col] = vec
            continue
        mean = float(vec.mean()) if vec.notna().any() else 0.0
        std = float(vec.std(ddof=0)) if vec.notna().any() else 0.0
        if std <= 0.0 or not math.isfinite(std):
            df[col] = vec.fillna(mean) - mean
        else:
            df[col] = (vec.fillna(mean) - mean) / std


def prepare_informative_df(
    df: pd.DataFrame,
    *,
    y_col: str,
    exog_cols: Sequence[str],
) -> pd.DataFrame:
    use_cols = ["event_id", "pair_id", "policy_document_id", y_col, *exog_cols]
    use = df[use_cols].copy().rename(columns={y_col: "y"})
    for c in exog_cols:
        use[c] = pd.to_numeric(use[c], errors="coerce")
    if exog_cols:
        use = use.dropna(subset=list(exog_cols))

    keep_events: List[str] = []
    for eid, g in use.groupby("event_id", dropna=False):
        if len(g) != 2:
            continue
        if int(g["y"].sum()) != 1:
            continue
        keep_events.append(str(eid))
    return use[use["event_id"].astype(str).isin(keep_events)].copy()


def fit_clogit(
    df: pd.DataFrame,
    *,
    exog_cols: Sequence[str],
    key_factor_col: str,
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    if df.empty:
        return None, None, "empty_data"
    try:
        y = df["y"].astype(int).to_numpy()
        X = df[list(exog_cols)].astype(float).to_numpy()
        groups = df["event_id"].astype(str).to_numpy()
        mdl = ConditionalLogit(y, X, groups=groups)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            res = mdl.fit(disp=False, maxiter=300)

        warning_texts = [f"{w.category.__name__}:{w.message}" for w in caught]
        lower_warning_blob = " | ".join(warning_texts).lower()

        converged = True
        mle_retvals = getattr(res, "mle_retvals", None)
        if isinstance(mle_retvals, dict):
            converged = bool(mle_retvals.get("converged", True))
        if not converged:
            return None, None, "fit_failed:non_converged"
        if "overflow" in lower_warning_blob:
            return None, None, "fit_failed:numerical_overflow"

        idx = list(exog_cols).index(key_factor_col)
        beta = float(res.params[idx])
        llf = float(res.llf)
        if not math.isfinite(beta) or not math.isfinite(llf):
            return None, None, "fit_failed:non_finite_estimate"
        return beta, llf, None
    except Exception as exc:
        return None, None, f"fit_failed:{exc}"
