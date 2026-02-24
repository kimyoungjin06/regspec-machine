"""Key-factor explorer module package for Phase-B Bikard dyad scans.

This module exports a flat public API while keeping imports lazy to avoid
module preload side effects (for example `python -m regspec_machine.launcher`).
"""

from importlib import import_module
from typing import Dict, Tuple

__all__ = [
    "CommandResult",
    "EngineExecution",
    "NONCONFIRMATORY_MAX_TIERS",
    "create_app",
    "BundleBuildResult",
    "DesktopBundleConfig",
    "build_bundle_parser",
    "build_pyinstaller_command",
    "parse_bundle_args",
    "resolve_bundle_executable",
    "run_bundle_build",
    "DesktopLaunchConfig",
    "build_desktop_parser",
    "parse_desktop_args",
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

_EXPORT_MAP: Dict[str, Tuple[str, str]] = {
    "CommandResult": (".engine", "CommandResult"),
    "EngineExecution": (".engine", "EngineExecution"),
    "NONCONFIRMATORY_MAX_TIERS": (".contracts", "NONCONFIRMATORY_MAX_TIERS"),
    "create_app": (".api", "create_app"),
    "BundleBuildResult": (".bundle", "BundleBuildResult"),
    "DesktopBundleConfig": (".bundle", "DesktopBundleConfig"),
    "build_bundle_parser": (".bundle", "build_bundle_parser"),
    "build_pyinstaller_command": (".bundle", "build_pyinstaller_command"),
    "parse_bundle_args": (".bundle", "parse_bundle_args"),
    "resolve_bundle_executable": (".bundle", "resolve_bundle_executable"),
    "run_bundle_build": (".bundle", "run_bundle_build"),
    "DesktopLaunchConfig": (".desktop", "DesktopLaunchConfig"),
    "build_desktop_parser": (".desktop", "build_desktop_parser"),
    "parse_desktop_args": (".desktop", "parse_desktop_args"),
    "build_ui_page_html": (".ui_page", "build_ui_page_html"),
    "ConsoleLaunchConfig": (".launcher", "ConsoleLaunchConfig"),
    "build_parser": (".launcher", "build_parser"),
    "parse_args": (".launcher", "parse_args"),
    "main": (".launcher", "main"),
    "PresetEngine": (".engine", "PresetEngine"),
    "ExecutionEngine": (".orchestrator", "ExecutionEngine"),
    "REQUIRED_COLUMNS": (".module_input", "REQUIRED_COLUMNS"),
    "RunOrchestrator": (".orchestrator", "RunOrchestrator"),
    "RUN_MODES": (".contracts", "RUN_MODES"),
    "RUN_STATE_VALUES": (".contracts", "RUN_STATE_VALUES"),
    "RunSnapshot": (".orchestrator", "RunSnapshot"),
    "ScanConfig": (".search_engine", "ScanConfig"),
    "TIME_SERIES_AUTO_POLICY_MODES": (".contracts", "TIME_SERIES_AUTO_POLICY_MODES"),
    "TIME_SERIES_PRECHECK_MODES": (".contracts", "TIME_SERIES_PRECHECK_MODES"),
    "apply_policy_split_file": (".splitter", "apply_policy_split_file"),
    "assign_policy_document_holdout": (".splitter", "assign_policy_document_holdout"),
    "build_feature_registry": (".feature_registry", "build_feature_registry"),
    "get_git_commit": (".module_input", "get_git_commit"),
    "load_and_prepare_data": (".module_input", "load_and_prepare_data"),
    "load_feature_registry": (".feature_registry", "load_feature_registry"),
    "RunArtifactsContract": (".contracts", "RunArtifactsContract"),
    "RunErrorContract": (".contracts", "RunErrorContract"),
    "RunRequestContract": (".contracts", "RunRequestContract"),
    "RunResultContract": (".contracts", "RunResultContract"),
    "RunStatusContract": (".contracts", "RunStatusContract"),
    "run_key_factor_scan": (".search_engine", "run_key_factor_scan"),
    "select_shortlist_features_from_top_models": (
        ".shortlist",
        "select_shortlist_features_from_top_models",
    ),
    "sha256_file": (".module_input", "sha256_file"),
    "sha256_json": (".module_input", "sha256_json"),
}

_SUBMODULES = {
    "api",
    "bundle",
    "contracts",
    "desktop",
    "engine",
    "feature_registry",
    "launcher",
    "module_input",
    "orchestrator",
    "search_engine",
    "shortlist",
    "splitter",
    "ui_page",
}


def __getattr__(name: str):
    if name in _SUBMODULES:
        module = import_module(f".{name}", __name__)
        globals()[name] = module
        return module

    target = _EXPORT_MAP.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals().keys()) | set(__all__))
