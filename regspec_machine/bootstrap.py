from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from .estimators import fit_clogit


def bootstrap_clogit(
    df: pd.DataFrame,
    *,
    exog_cols: Sequence[str],
    key_factor_col: str,
    cluster_unit: str,
    n_bootstrap: int,
    seed: int,
    optimizer_mode: str = "none",
    adam_max_iter: int = 300,
    adam_learning_rate: float = 0.05,
    adam_beta1: float = 0.9,
    adam_beta2: float = 0.999,
    adam_eps: float = 1e-8,
    adam_l2: float = 1e-4,
    adam_min_iter: int = 25,
    adam_tol: float = 1e-6,
) -> Tuple[List[float], int, int]:
    if df.empty or n_bootstrap <= 0:
        return [], 0, 0
    if cluster_unit not in df.columns:
        raise ValueError(f"cluster unit column not found: {cluster_unit}")

    cluster_ids = sorted(df[cluster_unit].dropna().astype(str).unique())
    if not cluster_ids:
        return [], 0, 0

    cluster_map: Dict[str, pd.DataFrame] = {
        cid: df[df[cluster_unit].astype(str) == cid].copy() for cid in cluster_ids
    }
    rng = np.random.default_rng(seed)
    betas: List[float] = []

    for b in range(n_bootstrap):
        sampled = rng.choice(cluster_ids, size=len(cluster_ids), replace=True)
        parts: List[pd.DataFrame] = []
        for j, cid in enumerate(sampled):
            part = cluster_map[str(cid)].copy()
            if part.empty:
                continue
            part["event_id"] = part["event_id"].astype(str) + f"|bs{b}_{j}"
            parts.append(part)
        if not parts:
            continue
        boot = pd.concat(parts, ignore_index=True)
        beta, _, err = fit_clogit(
            boot,
            exog_cols=exog_cols,
            key_factor_col=key_factor_col,
            optimizer_mode=optimizer_mode,
            adam_max_iter=adam_max_iter,
            adam_learning_rate=adam_learning_rate,
            adam_beta1=adam_beta1,
            adam_beta2=adam_beta2,
            adam_eps=adam_eps,
            adam_l2=adam_l2,
            adam_min_iter=adam_min_iter,
            adam_tol=adam_tol,
        )
        if beta is not None and err is None:
            betas.append(beta)

    return betas, len(cluster_ids), n_bootstrap


def summarize_bootstrap(
    values: List[float],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    if not values:
        return None, None, None, None, None
    arr = np.array(values, dtype=float)
    est = float(np.mean(arr))
    se = float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0
    lo = float(np.quantile(arr, 0.025))
    hi = float(np.quantile(arr, 0.975))
    # Finite-sample correction avoids exact zero p-values when no sign reversals appear.
    n = int(arr.size)
    left = (float(np.sum(arr <= 0.0)) + 1.0) / float(n + 1)
    right = (float(np.sum(arr >= 0.0)) + 1.0) / float(n + 1)
    p_boot = min(1.0, 2.0 * min(left, right))
    return est, lo, hi, se, p_boot
