"""Execution contracts for CLI/API/UI parity.

These contracts are intentionally stdlib-only so they can be shared by
CLI wrappers, future API services, and tests without extra dependencies.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


RUN_MODES: Tuple[str, ...] = (
    "nooption",
    "nooption_baseline",
    "singlex",
    "singlex_baseline",
    "singlex_hypothesis_panel",
    "openexplore",
    "openexplore_autorefine",
    "nooption_hypothesis_panel",
    "paired_nooption_singlex",
    "paired_nooption_singlex_hypothesis",
)

RUN_STATE_VALUES: Tuple[str, ...] = (
    "queued",
    "running",
    "succeeded",
    "failed",
    "cancelled",
)

TIME_SERIES_PRECHECK_MODES: Tuple[str, ...] = (
    "off",
    "warn",
    "fail_redundant_confirmatory",
    "fail_low_support",
    "fail_any",
)

TIME_SERIES_AUTO_POLICY_MODES: Tuple[str, ...] = (
    "off",
    "drop_redundant",
    "drop_redundant_and_low_support",
)

NONCONFIRMATORY_MAX_TIERS: Tuple[str, ...] = (
    "support_candidate",
    "exploratory",
)


def _utc_now_isoz() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _required_str(name: str, value: Any) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise ValueError(f"{name} is required and must be a non-empty string")
    return text


def _int_ge(name: str, value: Any, minimum: int) -> int:
    try:
        out = int(value)
    except Exception as exc:
        raise ValueError(f"{name} must be an integer >= {minimum}") from exc
    if out < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return out


def _float_between(name: str, value: Any, lo: float, hi: float) -> float:
    try:
        out = float(value)
    except Exception as exc:
        raise ValueError(f"{name} must be a float in [{lo}, {hi}]") from exc
    if out < lo or out > hi:
        raise ValueError(f"{name} must be a float in [{lo}, {hi}]")
    return out


def _choice(name: str, value: Any, allowed: Sequence[str]) -> str:
    text = _required_str(name, value)
    if text not in allowed:
        allowed_text = ", ".join(allowed)
        raise ValueError(f"{name} must be one of: {allowed_text}")
    return text


def _string_list(name: str, value: Any) -> Tuple[str, ...]:
    if value is None:
        return tuple()
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{name} must be a list of strings")
    out = []
    for item in value:
        text = str(item).strip()
        if not text:
            continue
        out.append(text)
    return tuple(out)


@dataclass(frozen=True)
class RunErrorContract:
    code: str
    message: str
    retryable: bool = False
    details: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        code: str,
        message: str,
        retryable: bool = False,
        details: Optional[Mapping[str, Any]] = None,
    ) -> "RunErrorContract":
        return cls(
            code=_required_str("code", code),
            message=_required_str("message", message),
            retryable=bool(retryable),
            details=dict(details or {}),
        )

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunRequestContract:
    mode: str
    run_id: str
    scan_n_bootstrap: int = 0
    scan_max_features: int = 0
    refine_n_bootstrap: int = 0
    runner_python: str = ".venv/bin/python"
    cli_summary_top_n: int = 10
    paired_legacy_sync_validation: bool = False
    skip_direction_review: bool = False
    extra_args: Tuple[str, ...] = ()
    hypothesis_window_years: str = "3,5,10"
    hypothesis_confirmatory_window_years: str = "3,5"
    hypothesis_time_series_precheck_mode: str = "fail_redundant_confirmatory"
    hypothesis_auto_confirmatory_policy: str = "drop_redundant_and_low_support"
    hypothesis_nonconfirmatory_max_tier: str = "exploratory"
    hypothesis_time_series_min_positive_events: int = 20
    hypothesis_time_series_min_track_positive_events: int = 0
    hypothesis_time_series_min_positive_share: float = 0.05
    idempotency_key: str = ""

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "RunRequestContract":
        p = dict(payload)
        mode = _choice("mode", p.get("mode"), RUN_MODES)
        run_id = _required_str("run_id", p.get("run_id"))
        idempotency_key = str(p.get("idempotency_key", "")).strip()
        if len(idempotency_key) > 128:
            raise ValueError("idempotency_key must be 128 characters or fewer")
        return cls(
            mode=mode,
            run_id=run_id,
            scan_n_bootstrap=_int_ge("scan_n_bootstrap", p.get("scan_n_bootstrap", 0), 0),
            scan_max_features=_int_ge("scan_max_features", p.get("scan_max_features", 0), 0),
            refine_n_bootstrap=_int_ge("refine_n_bootstrap", p.get("refine_n_bootstrap", 0), 0),
            runner_python=str(p.get("runner_python", ".venv/bin/python")).strip() or ".venv/bin/python",
            cli_summary_top_n=_int_ge("cli_summary_top_n", p.get("cli_summary_top_n", 10), 1),
            paired_legacy_sync_validation=bool(p.get("paired_legacy_sync_validation", False)),
            skip_direction_review=bool(p.get("skip_direction_review", False)),
            extra_args=_string_list("extra_args", p.get("extra_args", ())),
            hypothesis_window_years=str(p.get("hypothesis_window_years", "3,5,10")).strip() or "3,5,10",
            hypothesis_confirmatory_window_years=str(
                p.get("hypothesis_confirmatory_window_years", "3,5")
            ).strip()
            or "3,5",
            hypothesis_time_series_precheck_mode=_choice(
                "hypothesis_time_series_precheck_mode",
                p.get("hypothesis_time_series_precheck_mode", "fail_redundant_confirmatory"),
                TIME_SERIES_PRECHECK_MODES,
            ),
            hypothesis_auto_confirmatory_policy=_choice(
                "hypothesis_auto_confirmatory_policy",
                p.get("hypothesis_auto_confirmatory_policy", "drop_redundant_and_low_support"),
                TIME_SERIES_AUTO_POLICY_MODES,
            ),
            hypothesis_nonconfirmatory_max_tier=_choice(
                "hypothesis_nonconfirmatory_max_tier",
                p.get("hypothesis_nonconfirmatory_max_tier", "exploratory"),
                NONCONFIRMATORY_MAX_TIERS,
            ),
            hypothesis_time_series_min_positive_events=_int_ge(
                "hypothesis_time_series_min_positive_events",
                p.get("hypothesis_time_series_min_positive_events", 20),
                0,
            ),
            hypothesis_time_series_min_track_positive_events=_int_ge(
                "hypothesis_time_series_min_track_positive_events",
                p.get("hypothesis_time_series_min_track_positive_events", 0),
                0,
            ),
            hypothesis_time_series_min_positive_share=_float_between(
                "hypothesis_time_series_min_positive_share",
                p.get("hypothesis_time_series_min_positive_share", 0.05),
                0.0,
                1.0,
            ),
            idempotency_key=idempotency_key,
        )

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunStatusContract:
    run_id: str
    mode: str
    state: str
    created_at_utc: str
    updated_at_utc: str
    progress_stage: str = ""
    progress_message: str = ""
    progress_fraction: Optional[float] = None
    attempt: int = 1
    error: Optional[RunErrorContract] = None

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        mode: str,
        state: str,
        created_at_utc: Optional[str] = None,
        updated_at_utc: Optional[str] = None,
        progress_stage: str = "",
        progress_message: str = "",
        progress_fraction: Optional[float] = None,
        attempt: int = 1,
        error: Optional[RunErrorContract] = None,
    ) -> "RunStatusContract":
        if progress_fraction is not None and (progress_fraction < 0.0 or progress_fraction > 1.0):
            raise ValueError("progress_fraction must be in [0.0, 1.0]")
        created = str(created_at_utc or _utc_now_isoz()).strip()
        updated = str(updated_at_utc or created).strip()
        return cls(
            run_id=_required_str("run_id", run_id),
            mode=_choice("mode", mode, RUN_MODES),
            state=_choice("state", state, RUN_STATE_VALUES),
            created_at_utc=_required_str("created_at_utc", created),
            updated_at_utc=_required_str("updated_at_utc", updated),
            progress_stage=str(progress_stage).strip(),
            progress_message=str(progress_message).strip(),
            progress_fraction=progress_fraction,
            attempt=_int_ge("attempt", attempt, 1),
            error=error,
        )

    def as_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        if self.error is not None:
            payload["error"] = self.error.as_dict()
        return payload


@dataclass(frozen=True)
class RunArtifactsContract:
    scan_runs_csv: str = ""
    top_models_csv: str = ""
    top_models_inference_csv: str = ""
    search_log_jsonl: str = ""
    run_summary_json: str = ""
    feasibility_frontier_json: str = ""
    feature_registry_json: str = ""
    restart_stability_csv: str = ""
    paired_summary_json: str = ""
    direction_review_json: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunResultContract:
    run_id: str
    mode: str
    state: str
    artifacts: RunArtifactsContract = field(default_factory=RunArtifactsContract)
    counts: Dict[str, int] = field(default_factory=dict)
    governance_checks: Dict[str, Any] = field(default_factory=dict)
    audit_hashes: Dict[str, str] = field(default_factory=dict)
    timestamp_utc: str = field(default_factory=_utc_now_isoz)

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        mode: str,
        state: str,
        artifacts: Optional[RunArtifactsContract] = None,
        counts: Optional[Mapping[str, Any]] = None,
        governance_checks: Optional[Mapping[str, Any]] = None,
        audit_hashes: Optional[Mapping[str, Any]] = None,
        timestamp_utc: Optional[str] = None,
    ) -> "RunResultContract":
        return cls(
            run_id=_required_str("run_id", run_id),
            mode=_choice("mode", mode, RUN_MODES),
            state=_choice("state", state, RUN_STATE_VALUES),
            artifacts=artifacts or RunArtifactsContract(),
            counts={str(k): _int_ge(str(k), v, 0) for k, v in dict(counts or {}).items()},
            governance_checks=dict(governance_checks or {}),
            audit_hashes={str(k): str(v) for k, v in dict(audit_hashes or {}).items()},
            timestamp_utc=_required_str("timestamp_utc", timestamp_utc or _utc_now_isoz()),
        )

    def as_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["artifacts"] = self.artifacts.as_dict()
        return payload

