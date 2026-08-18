"""
rotation_monitor.pipeline — full end-to-end orchestrator.

    refresh price history -> build rotation_state.json -> render charts ->
    write rotation_report.md

Mirrors the existing top-level run_pipeline.py's role for the stock
research pipeline, but as an importable function so it can be called from
a CLI entrypoint (run_rotation.py) or from the Flask app the same way
trigger_pipeline.py already calls run_pipeline.py.
"""

import time
from typing import Dict

from rotation_monitor import charts, data_loader, report, state_builder
from rotation_monitor.returns import load_price_df
from rotation_monitor import config


def run(refresh_data: bool = True, render_charts: bool = True) -> Dict:
    timings = {}

    if refresh_data:
        t0 = time.time()
        results = data_loader.refresh_universe()
        timings["data_refresh_seconds"] = round(time.time() - t0, 2)
        errors = [r for r in results if r["status"] not in ("OK", "NO_DATA")]
        if errors:
            print(f"WARNING: {len(errors)} symbol(s) failed to refresh: {[e['symbol'] for e in errors]}")

    t0 = time.time()
    price_dfs = {s: load_price_df(s) for s in config.universe_tickers()}
    state = state_builder.build_rotation_state(price_dfs=price_dfs)
    timings["state_build_seconds"] = round(time.time() - t0, 2)

    state_path = state_builder.save_state(state)

    t0 = time.time()
    report_path = report.save_report(state)
    timings["report_seconds"] = round(time.time() - t0, 2)

    chart_paths = {}
    if render_charts:
        t0 = time.time()
        breadth_result = state.get("breadth")
        chart_paths = charts.render_all_charts(price_dfs=price_dfs, breadth_result=breadth_result)
        timings["charts_seconds"] = round(time.time() - t0, 2)

    return {
        "state_path": state_path,
        "report_path": report_path,
        "chart_paths": chart_paths,
        "timings": timings,
        "regime": state.get("market_regime"),
        "regime_confidence": state.get("regime_confidence"),
    }


if __name__ == "__main__":
    result = run()
    print(f"Rotation state: {result['state_path']}")
    print(f"Rotation report: {result['report_path']}")
    print(f"Charts: {len(result['chart_paths'])} rendered")
    print(f"Regime: {result['regime']} (confidence {result['regime_confidence']})")
    print(f"Timings: {result['timings']}")
