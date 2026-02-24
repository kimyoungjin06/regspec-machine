"""Key-factor explorer module package for Phase-B Bikard dyad scans."""

from .module_input import (
    REQUIRED_COLUMNS,
    get_git_commit,
    load_and_prepare_data,
    sha256_file,
    sha256_json,
)
from .contracts import (
    NONCONFIRMATORY_MAX_TIERS,
    RUN_MODES,
    RUN_STATE_VALUES,
    TIME_SERIES_AUTO_POLICY_MODES,
    TIME_SERIES_PRECHECK_MODES,
    RunArtifactsContract,
    RunErrorContract,
    RunRequestContract,
    RunResultContract,
    RunStatusContract,
)
from .api import create_app
from .engine import CommandResult, EngineExecution, PresetEngine
from .feature_registry import build_feature_registry, load_feature_registry
from .launcher import ConsoleLaunchConfig, build_parser, main, parse_args
from .orchestrator import ExecutionEngine, RunOrchestrator, RunSnapshot
from .shortlist import select_shortlist_features_from_top_models
from .splitter import apply_policy_split_file, assign_policy_document_holdout
from .search_engine import ScanConfig, run_key_factor_scan
from .ui_page import build_ui_page_html

__all__ = [
    "CommandResult",
    "EngineExecution",
    "NONCONFIRMATORY_MAX_TIERS",
    "create_app",
    "build_ui_page_html",
    "ConsoleLaunchConfig",
    "build_parser",
    "parse_args",
    "main",
    "PresetEngine",
    "ExecutionEngine",
    "REQUIRED_COLUMNS",
    "RunOrchestrator",
    "RUN_MODES",
    "RUN_STATE_VALUES",
    "RunSnapshot",
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
