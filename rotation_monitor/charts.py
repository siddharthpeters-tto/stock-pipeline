"""
rotation_monitor.charts — PNG chart generation (§17, §18, §25).

Renders the monthly heatmap and relative-strength ratio charts to
outputs/*.png, using matplotlib with the non-interactive Agg backend (no
display needed — this runs from a scheduled/CLI pipeline). Kept as static
images rather than a JS charting library so the dashboard stays dependency-
free, matching the existing index.html's vanilla-JS-only style.
"""

import os
from typing import Dict, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from rotation_monitor import config, heatmap as heatmap_mod  # noqa: E402
from rotation_monitor.relative_strength import normalized_ratio_series  # noqa: E402
from rotation_monitor.returns import load_price_df  # noqa: E402

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(_REPO_ROOT, "outputs")

# period label -> approximate trading days
RS_CHART_PERIODS = {"1m": 21, "3m": 63, "6m": 126, "1y": 252}

PRIORITY_PAIR = ("IGV", "SOXX")


def _ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def render_heatmap_png(heatmap_data: Dict, out_path: Optional[str] = None) -> str:
    _ensure_output_dir()
    out_path = out_path or os.path.join(OUTPUT_DIR, "rotation_heatmap.png")

    themes = heatmap_data["themes"]
    dates = heatmap_data["dates"]
    values = heatmap_data["values_pct"]

    if not themes or not dates:
        # nothing to plot (e.g. brand-new month with no sessions yet) --
        # still produce a small placeholder image rather than erroring
        fig, ax = plt.subplots(figsize=(6, 2))
        ax.text(0.5, 0.5, "No data available for this period", ha="center", va="center")
        ax.axis("off")
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return out_path

    data = [[v if v is not None else float("nan") for v in row] for row in values]

    fig_width = max(6, 0.5 * len(dates) + 2)
    fig_height = max(4, 0.35 * len(themes) + 1.5)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    vmax = max(1.0, max((abs(v) for row in data for v in row if v == v), default=1.0))
    im = ax.imshow(data, cmap="RdYlGn", vmin=-vmax, vmax=vmax, aspect="auto")

    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels([d[5:] for d in dates], rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(themes)))
    ax.set_yticklabels(themes, fontsize=9)

    for i, row in enumerate(data):
        for j, v in enumerate(row):
            if v == v:  # not NaN
                ax.text(j, i, f"{v:.1f}", ha="center", va="center", fontsize=6, color="black")

    mode_label = {"relative_spy": "Relative to SPY", "relative_qqq": "Relative to QQQ", "absolute": "Absolute"}.get(
        heatmap_data.get("mode"), heatmap_data.get("mode")
    )
    ax.set_title(f"Rotation Heatmap — {mode_label} (%) — {heatmap_data.get('year')}-{heatmap_data.get('month'):02d}")
    fig.colorbar(im, ax=ax, shrink=0.7, label="% return")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def render_rs_ratio_png(
    symbol_a: str, symbol_b: str, price_dfs: Dict[str, pd.DataFrame],
    period_days: int, period_label: str, out_path: Optional[str] = None,
) -> str:
    _ensure_output_dir()
    out_path = out_path or os.path.join(OUTPUT_DIR, f"rs_{symbol_a}_{symbol_b}_{period_label}.png")

    df_a = price_dfs.get(symbol_a)
    if df_a is None:
        df_a = load_price_df(symbol_a)
    df_b = price_dfs.get(symbol_b)
    if df_b is None:
        df_b = load_price_df(symbol_b)
    series = normalized_ratio_series(df_a, df_b, window=period_days)

    fig, ax = plt.subplots(figsize=(7, 3.5))
    if series.empty:
        ax.text(0.5, 0.5, "Not enough history for this window", ha="center", va="center")
        ax.axis("off")
    else:
        ax.plot(series.index, series.values, color="#2b6cb0", linewidth=1.6)
        ax.axhline(100, color="#888888", linewidth=0.8, linestyle="--")
        ax.set_title(f"{symbol_a}/{symbol_b} Relative Strength (normalized to 100, {period_label})")
        ax.set_ylabel("Ratio (start = 100)")
        fig.autofmt_xdate()

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def render_breadth_bar_png(breadth_result: Dict, out_path: Optional[str] = None) -> str:
    _ensure_output_dir()
    out_path = out_path or os.path.join(OUTPUT_DIR, "category_breadth.png")

    categories = list(breadth_result.get("by_category", {}).keys())
    values = [breadth_result["by_category"][c].get("outperforming_spy_5d_pct") for c in categories]
    values = [v if v is not None else 0 for v in values]

    fig, ax = plt.subplots(figsize=(7, 3.5))
    colors = ["#2f855a" if v >= 50 else "#c53030" for v in values]
    ax.bar(categories, values, color=colors)
    ax.axhline(50, color="#888888", linewidth=0.8, linestyle="--")
    ax.set_ylabel("% of category outperforming SPY (5D)")
    ax.set_title("Category Breadth (5D)")
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


def render_all_charts(
    price_dfs: Optional[Dict[str, pd.DataFrame]] = None,
    breadth_result: Optional[Dict] = None,
    heatmap_mode: str = "relative_spy",
) -> Dict[str, str]:
    if price_dfs is None:
        price_dfs = {s: load_price_df(s) for s in config.universe_tickers()}

    paths: Dict[str, str] = {}

    hm_data = heatmap_mod.compute_monthly_heatmap(mode=heatmap_mode, price_dfs=price_dfs)
    paths["heatmap"] = render_heatmap_png(hm_data)

    a, b = PRIORITY_PAIR
    for label, days in RS_CHART_PERIODS.items():
        paths[f"rs_{a}_{b}_{label}"] = render_rs_ratio_png(a, b, price_dfs, days, label)

    for pair_a, pair_b in config.RS_RATIO_PAIRS:
        if (pair_a, pair_b) == PRIORITY_PAIR:
            continue
        paths[f"rs_{pair_a}_{pair_b}_3m"] = render_rs_ratio_png(pair_a, pair_b, price_dfs, RS_CHART_PERIODS["3m"], "3m")

    if breadth_result is not None:
        paths["category_breadth"] = render_breadth_bar_png(breadth_result)

    return paths
