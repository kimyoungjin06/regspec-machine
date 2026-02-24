from __future__ import annotations

import math

import numpy as np
import pandas as pd

from regspec_machine.bootstrap import bootstrap_clogit
from regspec_machine.estimators import fit_clogit


def _mock_pair_df(*, n_events: int = 40, seed: int = 20260224, signal: float = 0.5) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n_events):
        event_id = f"e{i}"
        pair_id = f"p{i}"
        policy_document_id = f"d{i}"
        x_pos = float(signal + rng.normal(0.0, 0.5))
        x_neg = float(-signal + rng.normal(0.0, 0.5))
        rows.append(
            {
                "event_id": event_id,
                "pair_id": pair_id,
                "policy_document_id": policy_document_id,
                "y": 1,
                "x": x_pos,
            }
        )
        rows.append(
            {
                "event_id": event_id,
                "pair_id": pair_id,
                "policy_document_id": policy_document_id,
                "y": 0,
                "x": x_neg,
            }
        )
    return pd.DataFrame(rows)


def test_fit_clogit_adam_lite_returns_finite_estimate() -> None:
    df = _mock_pair_df(n_events=48, signal=0.8)
    beta, llf, err = fit_clogit(
        df,
        exog_cols=["x"],
        key_factor_col="x",
        optimizer_mode="adam_lite",
        adam_max_iter=400,
        adam_learning_rate=0.05,
    )
    assert err is None
    assert beta is not None and beta > 0.0
    assert llf is not None and math.isfinite(float(llf))


def test_fit_clogit_adam_lite_has_same_sign_as_statsmodels_path() -> None:
    df = _mock_pair_df(n_events=60, signal=0.35)
    beta_none, llf_none, err_none = fit_clogit(
        df,
        exog_cols=["x"],
        key_factor_col="x",
        optimizer_mode="none",
    )
    beta_adam, llf_adam, err_adam = fit_clogit(
        df,
        exog_cols=["x"],
        key_factor_col="x",
        optimizer_mode="adam_lite",
        adam_max_iter=300,
    )
    assert err_none is None
    assert err_adam is None
    assert beta_none is not None and beta_adam is not None
    assert llf_none is not None and llf_adam is not None
    assert beta_none * beta_adam > 0.0


def test_bootstrap_clogit_supports_adam_lite_mode() -> None:
    df = _mock_pair_df(n_events=36, signal=0.6)
    betas, n_clusters, attempted = bootstrap_clogit(
        df,
        exog_cols=["x"],
        key_factor_col="x",
        cluster_unit="policy_document_id",
        n_bootstrap=25,
        seed=20260224,
        optimizer_mode="adam_lite",
        adam_max_iter=150,
    )
    assert attempted == 25
    assert n_clusters == 36
    assert len(betas) >= 10


def test_fit_clogit_rejects_unknown_optimizer_mode() -> None:
    df = _mock_pair_df(n_events=20, signal=0.5)
    beta, llf, err = fit_clogit(
        df,
        exog_cols=["x"],
        key_factor_col="x",
        optimizer_mode="unknown_mode",
    )
    assert beta is None
    assert llf is None
    assert isinstance(err, str) and "unsupported_optimizer_mode" in err
