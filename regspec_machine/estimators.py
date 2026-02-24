from __future__ import annotations

import math
import warnings
from typing import List, Optional, Sequence, Tuple

import numpy as np
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
    optimizer_mode: str = "none",
    adam_max_iter: int = 300,
    adam_learning_rate: float = 0.05,
    adam_beta1: float = 0.9,
    adam_beta2: float = 0.999,
    adam_eps: float = 1e-8,
    adam_l2: float = 1e-4,
    adam_min_iter: int = 25,
    adam_tol: float = 1e-6,
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    if df.empty:
        return None, None, "empty_data"
    mode = str(optimizer_mode or "none").strip().lower()
    if mode in {"none", "statsmodels"}:
        return _fit_clogit_statsmodels(
            df=df,
            exog_cols=exog_cols,
            key_factor_col=key_factor_col,
        )
    if mode == "adam_lite":
        return _fit_clogit_adam_lite(
            df=df,
            exog_cols=exog_cols,
            key_factor_col=key_factor_col,
            adam_max_iter=int(adam_max_iter),
            adam_learning_rate=float(adam_learning_rate),
            adam_beta1=float(adam_beta1),
            adam_beta2=float(adam_beta2),
            adam_eps=float(adam_eps),
            adam_l2=float(adam_l2),
            adam_min_iter=int(adam_min_iter),
            adam_tol=float(adam_tol),
        )
    return None, None, f"fit_failed:unsupported_optimizer_mode:{mode}"


def _fit_clogit_statsmodels(
    *,
    df: pd.DataFrame,
    exog_cols: Sequence[str],
    key_factor_col: str,
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
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


def _fit_clogit_adam_lite(
    *,
    df: pd.DataFrame,
    exog_cols: Sequence[str],
    key_factor_col: str,
    adam_max_iter: int,
    adam_learning_rate: float,
    adam_beta1: float,
    adam_beta2: float,
    adam_eps: float,
    adam_l2: float,
    adam_min_iter: int,
    adam_tol: float,
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    if key_factor_col not in exog_cols:
        return None, None, "fit_failed:key_factor_not_in_exog_cols"
    x_diff = _build_pairwise_differences(df=df, exog_cols=exog_cols)
    n_events, n_features = x_diff.shape
    if n_events <= 0 or n_features <= 0:
        return None, None, "fit_failed:insufficient_pairwise_events"

    max_iter = max(int(adam_max_iter), 1)
    lr = max(float(adam_learning_rate), 1e-6)
    beta1 = min(max(float(adam_beta1), 0.0), 0.9999)
    beta2 = min(max(float(adam_beta2), 0.0), 0.999999)
    eps = max(float(adam_eps), 1e-12)
    l2 = max(float(adam_l2), 0.0)
    min_iter = max(int(adam_min_iter), 0)
    tol = max(float(adam_tol), 0.0)

    beta = np.zeros(n_features, dtype=float)
    m = np.zeros(n_features, dtype=float)
    v = np.zeros(n_features, dtype=float)
    step_norm = float("inf")
    for t in range(1, max_iter + 1):
        z = x_diff @ beta
        p = _sigmoid_stable(z)
        grad_ll = x_diff.T @ (1.0 - p) - (l2 * beta)
        grad = -grad_ll
        m = (beta1 * m) + ((1.0 - beta1) * grad)
        v = (beta2 * v) + ((1.0 - beta2) * (grad * grad))
        m_hat = m / (1.0 - (beta1**t))
        v_hat = v / (1.0 - (beta2**t))
        step = lr * m_hat / (np.sqrt(v_hat) + eps)
        beta = beta - step
        step_norm = float(np.linalg.norm(step, ord=2))
        if t >= min_iter and step_norm <= tol:
            break

    z_final = x_diff @ beta
    llf = float(np.sum(-np.logaddexp(0.0, -z_final)))
    key_idx = list(exog_cols).index(key_factor_col)
    beta_key = float(beta[key_idx])
    if not (math.isfinite(beta_key) and math.isfinite(llf)):
        return None, None, "fit_failed:non_finite_estimate"
    if not math.isfinite(step_norm):
        return None, None, "fit_failed:optimizer_diverged"
    return beta_key, llf, None


def _build_pairwise_differences(
    *,
    df: pd.DataFrame,
    exog_cols: Sequence[str],
) -> np.ndarray:
    diffs: List[np.ndarray] = []
    if not exog_cols:
        return np.zeros((0, 0), dtype=float)
    for _, g in df.groupby("event_id", dropna=False):
        if len(g) != 2:
            continue
        y = pd.to_numeric(g["y"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        if int(np.sum(y)) != 1:
            continue
        pos_idx = int(np.argmax(y))
        neg_idx = 1 - pos_idx
        x = g[list(exog_cols)].astype(float).to_numpy(dtype=float)
        diffs.append(x[pos_idx] - x[neg_idx])
    if not diffs:
        return np.zeros((0, len(exog_cols)), dtype=float)
    return np.vstack(diffs).astype(float, copy=False)


def _sigmoid_stable(z: np.ndarray) -> np.ndarray:
    out = np.empty_like(z, dtype=float)
    pos = z >= 0.0
    out[pos] = 1.0 / (1.0 + np.exp(-z[pos]))
    exp_z = np.exp(z[~pos])
    out[~pos] = exp_z / (1.0 + exp_z)
    return out
