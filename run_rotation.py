"""
run_rotation.py — top-level entrypoint for the Theme & Sector Rotation
Monitor. Mirrors run_pipeline.py's role for the stock research pipeline.

    python run_rotation.py

Refreshes cached ETF price history, computes the full rotation analysis,
and writes:
    outputs/rotation_state.json
    outputs/rotation_report.md
    outputs/*.png (heatmap + relative-strength charts)
"""

from rotation_monitor.pipeline import run

if __name__ == "__main__":
    result = run()
    print(f"\nRotation state: {result['state_path']}")
    print(f"Rotation report: {result['report_path']}")
    print(f"Charts: {len(result['chart_paths'])} rendered")
    print(f"Regime: {result['regime']} (confidence {result['regime_confidence']})")
    print(f"Timings: {result['timings']}")
