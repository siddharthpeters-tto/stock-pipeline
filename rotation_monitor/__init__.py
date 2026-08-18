"""
rotation_monitor — Theme & Sector Rotation Monitor.

A read-only analytics module layered on top of the existing FMP-based
research pipeline. It tracks relative strength, persistence, breadth,
correlation, and regime behavior across a configurable ETF universe.

It never recommends individual stocks and never produces BUY/SELL signals.

Phase 1 (data + analytics) is implemented first:
    config, fmp_client, storage, data_loader, returns, relative_strength,
    correlations, rankings.

Phase 2+ (persistence, breadth, regime, wave detection, charts, report,
dashboard integration) build on top of Phase 1 once its calculations are
confirmed against real data.
"""
