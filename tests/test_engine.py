from __future__ import annotations

import json
from pathlib import Path

from regspec_machine.contracts import RunRequestContract
from regspec_machine.engine import CommandResult, PresetEngine


def _mk_workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    preset = root / "scripts" / "modeling" / "run_phase_b_regspec_preset.py"
    preset.parent.mkdir(parents=True, exist_ok=True)
    preset.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
    return root


def _mk_workspace_with_module_preset(tmp_path: Path) -> Path:
    root = tmp_path / "workspace_mod"
    preset = (
        root
        / "modules"
        / "03_regspec_machine"
        / "scripts"
        / "modeling"
        / "run_phase_b_regspec_preset.py"
    )
    preset.parent.mkdir(parents=True, exist_ok=True)
    preset.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
    return root


def test_engine_build_command_includes_contract_options(tmp_path: Path) -> None:
    root = _mk_workspace(tmp_path)
    engine = PresetEngine(
        workspace_root=root,
        command_executor=lambda _cmd, _cwd: CommandResult(returncode=0),
    )
    req = RunRequestContract.from_payload(
        {
            "mode": "singlex_baseline",
            "run_id": "ut_cmd",
            "scan_n_bootstrap": 49,
            "scan_max_features": 120,
            "refine_n_bootstrap": 199,
            "paired_legacy_sync_validation": True,
            "extra_args": ["--foo", "--bar=1"],
        }
    )
    cmd = engine.build_command(req, dry_run=True)
    assert str(cmd[1]).endswith("run_phase_b_regspec_preset.py")
    assert "--mode" in cmd and "singlex_baseline" in cmd
    assert "--scan-n-bootstrap" in cmd and "49" in cmd
    assert "--scan-max-features" in cmd and "120" in cmd
    assert "--refine-n-bootstrap" in cmd and "199" in cmd
    assert "--paired-legacy-sync-validation" in cmd
    assert "--extra-arg" in cmd and "--foo" in cmd and "--bar=1" in cmd
    assert cmd[-1] == "--dry-run"


def test_engine_resolves_module_relative_preset_script(tmp_path: Path) -> None:
    root = _mk_workspace_with_module_preset(tmp_path)
    engine = PresetEngine(
        workspace_root=root,
        command_executor=lambda _cmd, _cwd: CommandResult(returncode=0),
    )
    req = RunRequestContract.from_payload({"mode": "singlex_baseline", "run_id": "ut_script_path"})
    cmd = engine.build_command(req, dry_run=True)
    assert "modules/03_regspec_machine" in str(cmd[1]).replace("\\", "/")


def test_engine_execute_singlex_reads_run_summary_payload(tmp_path: Path) -> None:
    root = _mk_workspace(tmp_path)
    run_id = "ut singlex summary"
    slug = "ut_singlex_summary"
    summary_rel = f"data/metadata/phase_b_bikard_machine_scientist_run_summary_{slug}.json"
    summary_path = root / summary_rel
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(
            {
                "counts": {
                    "scan_rows": 24,
                    "top_rows": 8,
                    "top_rows_inference": 3,
                    "candidate_tier_top_rows_inference": {"validated_candidate": 1},
                },
                "search_governance": {
                    "validation_used_for_search": False,
                    "candidate_pool_locked_pre_validation": True,
                },
                "audit_hashes": {
                    "data_hash": "datahash_ut",
                    "config_hash": "confighash_ut",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    engine = PresetEngine(
        workspace_root=root,
        command_executor=lambda _cmd, _cwd: CommandResult(returncode=0, stdout="ok"),
    )
    req = RunRequestContract.from_payload({"mode": "singlex_baseline", "run_id": run_id})
    out = engine.execute(req)

    assert out.status.state == "succeeded"
    assert out.result.state == "succeeded"
    assert out.result.artifacts.run_summary_json == summary_rel
    assert out.result.counts["scan_rows"] == 24
    assert out.result.counts["top_rows_inference"] == 3
    assert out.result.governance_checks["search_governance"]["validation_used_for_search"] is False
    assert out.result.audit_hashes["data_hash"] == "datahash_ut"


def test_engine_execute_paired_reads_direction_review_checks(tmp_path: Path) -> None:
    root = _mk_workspace(tmp_path)
    run_id = "ut_pair"
    paired_rel = "data/metadata/phase_b_bikard_machine_scientist_paired_preset_summary_ut_pair.json"
    review_rel = "data/metadata/phase_b_bikard_machine_scientist_direction_review_ut_pair.json"
    paired_path = root / paired_rel
    review_path = root / review_rel
    paired_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    paired_path.write_text(
        json.dumps(
            {
                "mode": "paired_nooption_singlex",
                "run_id": run_id,
                "status": "ok",
                "children": [
                    {"mode": "nooption_baseline", "status": "ok"},
                    {"mode": "singlex_baseline", "status": "ok"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    review_path.write_text(
        json.dumps(
            {
                "checks": {
                    "all_children_ok": True,
                    "required_fields_present": True,
                    "singlex_track_consensus_check_pass": True,
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    engine = PresetEngine(
        workspace_root=root,
        command_executor=lambda _cmd, _cwd: CommandResult(returncode=0, stdout="ok"),
    )
    req = RunRequestContract.from_payload({"mode": "paired_nooption_singlex", "run_id": run_id})
    out = engine.execute(req)

    assert out.status.state == "succeeded"
    assert out.result.counts["children_total"] == 2
    assert out.result.counts["children_ok"] == 2
    assert out.result.governance_checks["all_children_ok"] is True
    assert out.result.artifacts.paired_summary_json == paired_rel
    assert out.result.artifacts.direction_review_json == review_rel


def test_engine_execute_overnight_reads_summary_payload(tmp_path: Path) -> None:
    root = _mk_workspace(tmp_path)
    run_id = "ut_overnight"
    overnight_rel = "data/metadata/phase_b_bikard_machine_scientist_overnight_summary_ut_overnight.json"
    overnight_path = root / overnight_rel
    overnight_path.parent.mkdir(parents=True, exist_ok=True)
    overnight_path.write_text(
        json.dumps(
            {
                "mode": "overnight_validation",
                "run_id": run_id,
                "status": "partial_failure",
                "checkpoint_json": "data/metadata/phase_b_bikard_machine_scientist_overnight_checkpoint_ut_overnight.json",
                "aggregate": {
                    "n_attempts_total": 6,
                    "n_attempts_returncode_zero": 5,
                    "n_attempts_returncode_nonzero": 1,
                    "n_primary_objective_gate_pass": 3,
                    "primary_objective_gate_pass_rate": 0.5,
                    "best_metrics": {
                        "nooption_best_q_validation": {
                            "value": 0.08,
                            "run_id": "ut_overnight__b99_s20260219",
                        }
                    },
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    engine = PresetEngine(
        workspace_root=root,
        command_executor=lambda _cmd, _cwd: CommandResult(returncode=0, stdout="ok"),
    )
    req = RunRequestContract.from_payload({"mode": "overnight_validation", "run_id": run_id})
    out = engine.execute(req)

    assert out.status.state == "succeeded"
    assert out.result.artifacts.overnight_summary_json == overnight_rel
    assert out.result.counts["n_attempts_total"] == 6
    assert out.result.counts["n_attempts_returncode_nonzero"] == 1
    assert out.result.governance_checks["overnight_status"] == "partial_failure"
    assert out.result.governance_checks["primary_objective_gate_pass_rate"] == 0.5


def test_engine_execute_failure_maps_to_failed_status(tmp_path: Path) -> None:
    root = _mk_workspace(tmp_path)
    engine = PresetEngine(
        workspace_root=root,
        command_executor=lambda _cmd, _cwd: CommandResult(returncode=2, stderr="boom"),
    )
    req = RunRequestContract.from_payload({"mode": "nooption_baseline", "run_id": "ut_fail"})
    out = engine.execute(req)

    assert out.returncode == 2
    assert out.status.state == "failed"
    assert out.status.error is not None
    assert out.status.error.code == "PRESET_RUNNER_FAILED"
    assert out.result.state == "failed"


def test_engine_run_shortcuts_default_to_baseline_modes(tmp_path: Path) -> None:
    root = _mk_workspace(tmp_path)
    engine = PresetEngine(
        workspace_root=root,
        command_executor=lambda _cmd, _cwd: CommandResult(returncode=0, stdout="ok"),
    )

    singlex = engine.run_singlex(run_id="ut_singlex_shortcut", dry_run=True)
    nooption = engine.run_nooption(run_id="ut_nooption_shortcut", dry_run=True)
    paired = engine.run_paired(run_id="ut_paired_shortcut", dry_run=True)

    assert singlex.request.mode == "singlex_baseline"
    assert nooption.request.mode == "nooption_baseline"
    assert paired.request.mode == "paired_nooption_singlex"
    assert singlex.result.governance_checks["dry_run"] is True
    assert nooption.result.governance_checks["dry_run"] is True
    assert paired.result.governance_checks["dry_run"] is True
