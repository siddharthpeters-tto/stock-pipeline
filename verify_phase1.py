"""
verify_phase1.py — Phase 1 sanity-check runner for the Rotation Monitor.

Refreshes the cached ETF price history, then computes and prints:
    - absolute returns for the full universe
    - relative returns vs SPY / vs QQQ
    - 1D/5D/10D/20D leadership rankings (absolute and relative-to-SPY)
    - the priority IGV/SOXX relative-strength ratio + rolling correlation
    - all configured RS ratio pairs and correlation pairs

This is a read-only diagnostic script, not part of the rotation_monitor
package — its only job is to make Phase 1's calculations inspectable
against real market data before Phase 2 (persistence/breadth/regime/wave
detection) is built on top of it.
"""

from rotation_monitor import config, correlations, data_loader, rankings, relative_strength, returns


def section(title: str):
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def fmt_pct(v):
    return "DATA_UNAVAILABLE" if v is None else f"{v * 100:+.2f}%"


def main():
    section("1. REFRESHING PRICE HISTORY")
    results = data_loader.refresh_universe()
    data_loader._print_summary(results)

    universe = config.load_universe()
    tickers = list(universe.keys())

    section("2. ABSOLUTE RETURNS")
    universe_returns = {}
    price_dfs = {}
    for t in tickers:
        df = returns.load_price_df(t)
        price_dfs[t] = df
        universe_returns[t] = returns.compute_returns(t, df=df)

    header = f"{'Ticker':<6}{'AsOf':<12}" + "".join(f"{h:>10}" for h in ["1D", "3D", "5D", "10D", "20D", "MTD", "QTD", "YTD"])
    print(header)
    for t in tickers:
        as_of = returns.latest_close_date(t, df=price_dfs[t]) or "DATA_UNAVAILABLE"
        r = universe_returns[t]
        row = f"{t:<6}{as_of:<12}" + "".join(f"{fmt_pct(r[h]):>10}" for h in ["1D", "3D", "5D", "10D", "20D", "MTD", "QTD", "YTD"])
        print(row)

    section("3. RELATIVE RETURNS (vs SPY / vs QQQ)")
    rel_returns = relative_strength.compute_relative_returns_for_universe(universe_returns)
    header = f"{'Ticker':<6}" + "".join(f"{h:>10}" for h in ["1D", "5D", "10D", "20D"]) + "  |  " + \
             "".join(f"{h:>10}" for h in ["1D", "5D", "10D", "20D"])
    print(f"{'':<6}{'vs SPY':^42}{'vs QQQ':^42}")
    print(header)
    for t in tickers:
        spy_row = "".join(f"{fmt_pct(rel_returns[t]['vs_SPY'][h]):>10}" for h in ["1D", "5D", "10D", "20D"])
        qqq_row = "".join(f"{fmt_pct(rel_returns[t]['vs_QQQ'][h]):>10}" for h in ["1D", "5D", "10D", "20D"])
        print(f"{t:<6}{spy_row}  |  {qqq_row}")

    section("4. LEADERSHIP RANKINGS (themes only, excl. SPY/QQQ)")
    abs_rank = rankings.rank_by_absolute_return(universe_returns)
    rel_rank_spy = rankings.rank_by_relative_return(rel_returns, vs="vs_SPY")

    for h in ["1D", "5D", "10D", "20D"]:
        print(f"\n-- {h} absolute return leaders --")
        for row in abs_rank[h][:5]:
            print(f"   {row['rank']}. {row['symbol']:<6} {fmt_pct(row['value'])}")
        print(f"-- {h} absolute return laggards --")
        for row in abs_rank[h][-5:][::-1]:
            print(f"   {row['rank']}. {row['symbol']:<6} {fmt_pct(row['value'])}")

        print(f"-- {h} relative-to-SPY leaders --")
        for row in rel_rank_spy[h][:5]:
            print(f"   {row['rank']}. {row['symbol']:<6} {fmt_pct(row['value'])}")

    section("5. PRIORITY PAIR: IGV / SOXX")
    for window in [20, 60]:
        summary = relative_strength.rs_ratio_summary("IGV", "SOXX", window=window)
        print(f"  RS ratio ({window}D window): {summary}")

    corr = correlations.correlation_summary("SOXX", "IGV")
    print(f"  Correlation SOXX vs IGV: {corr}")

    section("6. ALL CONFIGURED RS RATIO PAIRS (20D window)")
    for pair_key, summary in relative_strength.all_rs_ratio_summaries(window=20).items():
        print(f"  {pair_key:<12} {summary}")

    section("7. ALL CONFIGURED CORRELATION PAIRS")
    for pair_key, summary in correlations.all_correlation_summaries().items():
        print(f"  {pair_key:<12} 20D={summary['20D']}  60D={summary['60D']}")

    section("DONE — Phase 1 output above. Review before Phase 2.")


if __name__ == "__main__":
    main()
