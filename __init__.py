"""Key-factor explorer module package for Phase-B Bikard dyad scans."""

from .module_input import (
    REQUIRED_COLUMNS,
    get_git_commit,
    load_and_prepare_data,
    sha256_file,
    sha256_json,
)
from .feature_registry import build_feature_registry, load_feature_registry
from .splitter import apply_policy_split_file, assign_policy_document_holdout
from .search_engine import ScanConfig, run_key_factor_scan

__all__ = [
    "REQUIRED_COLUMNS",
    "ScanConfig",
    "apply_policy_split_file",
    "assign_policy_document_holdout",
    "build_feature_registry",
    "get_git_commit",
    "load_and_prepare_data",
    "load_feature_registry",
    "run_key_factor_scan",
    "sha256_file",
    "sha256_json",
]
