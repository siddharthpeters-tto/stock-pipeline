# Stock Research Pipeline

Automated stock screening pipeline that:

1. Builds a stock universe
2. Filters financially viable companies
3. Detects business inflections
4. Classifies structural business quality
5. Evaluates durability
6. Identifies valuation mispricing

## Pipeline

buildUniverse → Stage1 → Stage2 → Stage3 → Stage4 → Stage5-1 → Stage5-2

## Running

python run_pipeline.py

## Rotation Monitor

A separate, read-only module (`rotation_monitor/`) tracks capital rotation
across a configurable ETF universe (`rotation_config/rotation_universe.json`)
— relative strength, persistence, breadth, correlation, wave/oscillation
detection, and market-regime classification. It never picks stocks or
issues buy/sell signals.

Run it with:

    python run_rotation.py

which refreshes cached ETF price history (`data/rotation/rotation.db`) and
writes `outputs/rotation_state.json`, `outputs/rotation_report.md`, and
the heatmap/relative-strength charts. View it at `/rotation` when
`trigger_pipeline.py` is running, or trigger it remotely via
`POST /run-rotation`. See `verify_phase1.py` for a standalone diagnostic
of the underlying return/correlation calculations.