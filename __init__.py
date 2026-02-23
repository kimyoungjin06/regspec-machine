"""Compatibility wrapper for legacy import paths.

Standalone usage should prefer `regspec_machine`.
"""

from .regspec_machine import (  # noqa: F401
    NONCONFIRMATORY_MAX_TIERS,
    REQUIRED_COLUMNS,
    RUN_MODES,
    RUN_STATE_VALUES,
    ScanConfig,
    TIME_SERIES_AUTO_POLICY_MODES,
    TIME_SERIES_PRECHECK_MODES,
    apply_policy_split_file,
    assign_policy_document_holdout,
    build_feature_registry,
    get_git_commit,
    load_and_prepare_data,
    load_feature_registry,
    RunArtifactsContract,
    RunErrorContract,
    RunRequestContract,
    RunResultContract,
    RunStatusContract,
    run_key_factor_scan,
    select_shortlist_features_from_top_models,
    sha256_file,
    sha256_json,
)

__all__ = [
    "NONCONFIRMATORY_MAX_TIERS",
    "REQUIRED_COLUMNS",
    "RUN_MODES",
    "RUN_STATE_VALUES",
    "ScanConfig",
    "TIME_SERIES_AUTO_POLICY_MODES",
    "TIME_SERIES_PRECHECK_MODES",
    "apply_policy_split_file",
    "assign_policy_document_holdout",
    "build_feature_registry",
    "get_git_commit",
    "load_and_prepare_data",
    "load_feature_registry",
    "RunArtifactsContract",
    "RunErrorContract",
    "RunRequestContract",
    "RunResultContract",
    "RunStatusContract",
    "run_key_factor_scan",
    "select_shortlist_features_from_top_models",
    "sha256_file",
    "sha256_json",
]
