#!/usr/bin/env python3
"""Headless UX journey smoke for RegSpec-Machine Console."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _import_playwright() -> Any:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception as exc:  # pragma: no cover - env dependent
        raise SystemExit(
            "playwright is required for ui journey smoke. "
            "install with: .venv/bin/pip install playwright"
        ) from exc
    return sync_playwright


@dataclass
class SmokeThresholds:
    max_elapsed_sec: float
    max_click_count: int
    max_scroll_viewports: float
    min_actions_completed: int


def _click_if(page: Any, selector: str, *, timeout_ms: int = 12000) -> bool:
    locator = page.locator(selector)
    if locator.count() <= 0:
        return False
    try:
        if not locator.first.is_visible(timeout=800):
            return False
    except Exception:
        return False
    locator.first.click(timeout=timeout_ms)
    return True


def _click_first_row_if(page: Any, row_selector: str, *, timeout_ms: int = 12000) -> bool:
    rows = page.locator(row_selector)
    if rows.count() <= 0:
        return False
    try:
        if not rows.first.is_visible(timeout=800):
            return False
    except Exception:
        return False
    rows.first.click(timeout=timeout_ms)
    return True


def _expand_details_if(page: Any, selector: str) -> bool:
    locator = page.locator(selector)
    if locator.count() <= 0:
        return False
    try:
        return bool(
            locator.first.evaluate(
                """(el) => {
                  if (!(el instanceof HTMLDetailsElement)) return false;
                  if (!el.open) el.open = true;
                  return true;
                }"""
            )
        )
    except Exception:
        return False


def _click_scatter_point_if(page: Any) -> bool:
    selectors = [
        "#explorer_best_qp_scatter .scatterlayer .points path",
        "#explorer_best_qp_scatter .scatterlayer .points circle",
        "#explorer_best_qp_scatter .point",
    ]
    for sel in selectors:
        loc = page.locator(sel)
        n = loc.count()
        if n <= 0:
            continue
        for i in range(min(10, n)):
            try:
                loc.nth(i).click(timeout=3000, force=True)
                return True
            except Exception:
                continue
    return False


def _read_payload(page: Any) -> dict[str, Any]:
    return page.evaluate(
        """() => {
          const exported = (typeof uxPayloadForExport === "function") ? uxPayloadForExport() : {};
          const notice = (document.getElementById("ux_notice") || {}).innerText || "";
          return { exported, notice };
        }"""
    )


def _check_thresholds(payload: dict[str, Any], thresholds: SmokeThresholds) -> list[str]:
    errors: list[str] = []
    elapsed = float(payload.get("elapsed_sec") or 0.0)
    clicks = int(payload.get("click_count") or 0)
    scroll = float(payload.get("scroll_viewports") or 0.0)
    actions = int(payload.get("actions_completed") or 0)
    if elapsed > thresholds.max_elapsed_sec:
        errors.append(f"elapsed_sec {elapsed:.2f} > {thresholds.max_elapsed_sec:.2f}")
    if clicks > thresholds.max_click_count:
        errors.append(f"click_count {clicks} > {thresholds.max_click_count}")
    if scroll > thresholds.max_scroll_viewports:
        errors.append(f"scroll_viewports {scroll:.2f} > {thresholds.max_scroll_viewports:.2f}")
    if actions < thresholds.min_actions_completed:
        errors.append(f"actions_completed {actions} < {thresholds.min_actions_completed}")
    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run headless UX journey smoke for /ui.")
    parser.add_argument(
        "--base-url",
        default=os.getenv("UI_BASE_URL", "http://127.0.0.1:8010/ui"),
        help="UI URL (default: http://127.0.0.1:8010/ui)",
    )
    parser.add_argument(
        "--out-dir",
        default="tmp/ui",
        help="artifact output directory",
    )
    parser.add_argument("--max-elapsed-sec", type=float, default=20.0)
    parser.add_argument("--max-click-count", type=int, default=12)
    parser.add_argument("--max-scroll-viewports", type=float, default=10.0)
    parser.add_argument("--min-actions-completed", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    thresholds = SmokeThresholds(
        max_elapsed_sec=float(args.max_elapsed_sec),
        max_click_count=int(args.max_click_count),
        max_scroll_viewports=float(args.max_scroll_viewports),
        min_actions_completed=int(args.min_actions_completed),
    )
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_json = out_dir / f"regspec_ui_ux_smoke_{ts}.json"
    out_png = out_dir / f"regspec_ui_ux_smoke_{ts}.png"

    sync_playwright = _import_playwright()
    steps: list[str] = []
    warnings: list[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 1200})
        page.goto(args.base_url, wait_until="networkidle", timeout=120000)
        steps.append("open_ui")
        page.locator("#section_explorer_sweep").scroll_into_view_if_needed()
        page.wait_for_timeout(500)

        _expand_details_if(page, "#explorer_ux_benchmark")
        if _click_if(page, "#ux_start_btn"):
            steps.append("ux_start")
        if _click_if(page, "#explorer_refresh_btn"):
            steps.append("refresh_explorer")
        page.wait_for_timeout(2200)

        _expand_details_if(page, "#explorer_tables_fold")
        if _click_first_row_if(page, "#explorer_combo_tbody tr.focus-clickable"):
            steps.append("select_combo")
        else:
            warnings.append("combo_row_missing")
        page.wait_for_timeout(600)

        if _click_first_row_if(page, "#explorer_focus_similar_tbody tr.focus-clickable"):
            steps.append("select_similar")
        else:
            warnings.append("similar_row_missing")
        page.wait_for_timeout(600)

        if _click_if(page, "#explorer_focus_apply_inspect_btn"):
            steps.append("filter_inspect_focus")
        else:
            if _click_if(page, "#explorer_focus_apply_filter_btn"):
                steps.append("filter_focus_only")
            if _click_if(page, "#explorer_focus_inspect_run_btn"):
                steps.append("inspect_focus_only")
        page.wait_for_timeout(800)

        if _click_scatter_point_if(page):
            steps.append("click_scatter")
        else:
            warnings.append("scatter_point_missing")
        page.wait_for_timeout(600)

        if _click_if(page, "#ux_stop_btn"):
            steps.append("ux_stop")
        page.wait_for_timeout(400)

        payload = _read_payload(page)
        page.screenshot(path=str(out_png), full_page=True)
        browser.close()

    exported = payload.get("exported") if isinstance(payload, dict) else {}
    exported = exported if isinstance(exported, dict) else {}
    threshold_errors = _check_thresholds(exported, thresholds)

    result = {
        "base_url": args.base_url,
        "steps": steps,
        "warnings": warnings,
        "threshold_errors": threshold_errors,
        "thresholds": {
            "max_elapsed_sec": thresholds.max_elapsed_sec,
            "max_click_count": thresholds.max_click_count,
            "max_scroll_viewports": thresholds.max_scroll_viewports,
            "min_actions_completed": thresholds.min_actions_completed,
        },
        "ux_payload": exported,
        "ux_notice": payload.get("notice") if isinstance(payload, dict) else "",
        "artifacts": {
            "json": str(out_json),
            "screenshot": str(out_png),
        },
    }
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if threshold_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
