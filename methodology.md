How Your Stock-Analysis System Actually Works
I read every .py file in the repo, traced every import and function call by hand (not by filename assumption), and reconstructed one real worked example (AAPL) using actual cached API data found in api_cache/. No files were modified. Below is the evidence-based trace.

STEP 1 — System Map
All Python files (12 total)
File	Role	Materially affects final result?
buildUniverse.py	Pulls NASDAQ+NYSE screener universe → tickers.txt	✅ Yes — defines the starting universe
Stage1.py	TTM fundamentals + hard survival filters	✅ Yes
Stage2.py	Inflection scoring engine + ranking	✅ Yes
Stage3.py	Sector exclusions + bucket classification	✅ Yes
Stage4.py	Top-N shortlisting per bucket + templated "thesis" text	✅ Yes (as a gate), but its "thesis" text is cosmetic, not calculated
Stage5_1.py	5-year structural quality scoring + (dead) GPT layer	✅ Yes (quant part); GPT part is dead code
Stage5_2.py	Live valuation rebuild + quality/value quadrant + QAV score + GPT nuance	✅ Yes — this is the core valuation/recommendation engine
generate_report.py	Adds "persistence bonus", writes email_summary.txt	✅ Yes — computes the actual "final_score"
run_pipeline.py	Orchestrator — runs all stages in sequence	✅ Yes (control flow only, no calculation)
trigger_pipeline.py	Flask web server behind index.html — alternate entry point	✅ Yes — but bypasses Stages 1–4 entirely (see Step 9)
category_config.py	Defines CATEGORY_RULES / DEFAULT_CATEGORY	❌ Dead code — confirmed unused (see Step 7)
test.py	One-off script hitting /income-statement for AAPL and printing raw JSON	❌ Not part of the pipeline — a scratch/debug file
Non-Python files that matter: index.html (front-end for trigger_pipeline.py), .env/.env.example (API keys), requirements.txt, api_cache/ (15-minute TTL cache used only by Stage5_2), and the gitignored runtime JSON files (tickers.txt, stage1_output.json … level2_results.json, pipeline_history.json) which are the actual intermediate datasets — none of these exist locally right now (confirmed: all missing from disk), so this analysis is based on code logic plus the one real artifact that does exist: api_cache/ and a historical email_summary.txt.

Two independent entry points (confirmed from code, not assumption)
Batch pipeline — run_pipeline.py → subprocess-runs buildUniverse.py → Stage1 → Stage2 → Stage3 → Stage4 → Stage5_1 → Stage5_2 → generate_report.py in order.
Single-stock web app — trigger_pipeline.py (Flask) serves index.html; its /analyze-stock route calls analyze_single_stock_stage5_1() and analyze_single_stock_stage5_2() directly, for whatever ticker a user types. This path never touches Stage1–Stage4. Any ticker can be analyzed this way, including ones that would have been filtered out by the inflection/exclusion logic in Stages 1–3.
Data sources
Financial Modeling Prep (FMP) stable API — income statement, balance sheet, cash flow, ratios, key-metrics, quote, stock-peers, company-screener. This is the only fundamental/market data source.
OpenAI (gpt-4o-mini by default) — used for a qualitative "nuance" layer in Stage5_2 (and defined-but-unused in Stage5_1).
No other data sources (no local datasets, no technical/price-history feed beyond the single live /quote call, no macro data).
Configuration
.env → FMP_API_KEY, OPENAI_API_KEY, PIPELINE_SECRET, optional OPENAI_MODEL/FMP_BASE_URL overrides.
Numeric thresholds are hard-coded as module-level constants inside each stage file (not centralized) — e.g., MIN_REVENUE, DELTA_CAP_FOR_SCORE, MAX_NET_DEBT_EBITDA, TOP_HC/TOP_TA/TOP_SPEC, MAX_PEERS. There is no single config file that governs the whole pipeline.
category_config.py looks like it's meant to be that central config, but it is never imported anywhere.
STEP 2 & 3 — The Calculation Trace and the True Formula (combined, since they're the same evidence)
Important finding stated up front: there is no single final formula. The system is a sequential filter → independently-rescored funnel, not a weighted blend. Three separate scoring systems exist (Stage2's inflection score, Stage5-1's structural score, Stage5-2's QAV score), each computed from raw data, each used only to rank/gate within its own stage. A stock's Stage2 score does not get algebraically folded into its Stage5-2 score. I'll show each layer's actual math, then show how they chain.

Layer 0 — Universe (buildUniverse.py)
US-listed NASDAQ+NYSE, market cap $10B–$10T, not ETF/fund, actively trading, ticker has no -/.. No calculation, pure filter.

Layer 1 — Stage1.py: TTM fundamentals + survival filter
compute_metrics() (Stage1.py:66-150), per ticker, from 20 quarters of income/balance/cashflow:


rev_now      = sum(last 4 quarters revenue)         [sum_quarters(income,"revenue",0)]
rev_1y       = sum(quarters 4-7 revenue)             [start=4]
rev_2y       = sum(quarters 8-11 revenue)            [start=8]
revenue_1y_growth = rev_now/rev_1y - 1
revenue_cagr_2y    = (rev_now/rev_2y)^(1/2) - 1

operating_margin     = TTM operatingIncome / TTM revenue
operating_margin_2y  = operatingIncome(2y ago TTM) / revenue(2y ago TTM)
gross_margin         = TTM grossProfit / TTM revenue
fcf_now = TTM operatingCashFlow + TTM capitalExpenditure   (capex is already negative)
fcf_margin    = fcf_now / rev_now
fcf_margin_2y = fcf_2y / rev_2y
net_debt_to_ebitda = (totalDebt - cash) / TTM ebitda
Hard kill filters (drop, return None) — Stage1.py:68,79,94,130,134:

Fewer than 12 quarters of history → dropped
rev_now/rev_2y missing or ≤0 → dropped
operating_margin < 0 → dropped (loss-making TTM excluded at the very first gate)
net_debt_to_ebitda > 4 → dropped
revenue_cagr_2y < -0.10 → dropped ("structural revenue collapse filter")
Output → stage1_output.json.

Layer 2 — Stage2.py: Inflection scoring engine (score_company, Stage2.py:54-273)
This is the first point-based score. Its full component breakdown, and this is where I found the clearest double-counting (see Step 7):


raw_accel        = growth_1y - cagr_2y
raw_margin_delta = op_margin - op_margin_2y
raw_fcf_delta    = fcf_margin - fcf_margin_2y
Hard filters (Stage2.py:71-76): revenue < $150M → drop; gross_margin < 25% → drop; net_debt/EBITDA > 4 → drop (redundant with Stage1's identical check).

Multi-signal gate (Stage2.py:88-101): needs ≥2 of {accel>5pts, margin_delta>3pts, fcf_delta>3pts} — else dropped entirely, regardless of score. This is a hard cliff, not a scoring adjustment.

Caps for scoring (raw values are preserved for reporting, capped values feed the score): accel capped to ±40pts, margin_delta/fcf_delta capped to ±35pts.

Sub-scores (all additive, unnormalized point totals — not a weighted percentage system):

Sub-score	Range	Driven by
s_growth	-2 to +3	growth_1y thresholds (20%/10%/5%/-5%)
s_accel	-1 to +2	capped accel thresholds
s_cagr	-2 to +2	cagr_2y thresholds
s_margin	-2 to +3	capped margin_delta thresholds
s_margin_flip	0 or +3	op-margin flipped neg→pos, only if revenue ≥ $300M
s_op_quality	0 to +2	absolute op_margin level (>20%/>12%)
s_fcf	-2 to +3	capped fcf_delta thresholds
s_fcf_flip	0 or +2	FCF margin flipped neg→pos, only if revenue ≥ $300M
s_fcf_quality	-1 to +2	absolute fcf_margin level
s_balance	-1 to +2	net_debt_to_ebitda thresholds
s_scale	0 to +2	max(0, min(2, log10(revenue) - 8)) — log-scale size bonus
s_saturation	0 to -4.5	-1.5 per metric that hit its cap (accel/margin/fcf)
total_score = sum of all twelve sub-scores (Stage2.py:237-242). Ranking sorts by (total_score, margin_delta, fcf_delta, acceleration, revenue) descending — the last three are tie-breakers only. Output → stage2_output.json.

Layer 3 — Stage3.py: sector exclusion + bucket classification
No new scoring math. Pure filter (REITs, miners, crypto miners, BDCs, MLPs, named excluded industries via FMP /profile; plus score < 12, revenue < $200M, saturation ≤ -3, margin_delta ≤0 AND fcf_delta ≤0). Survivors are bucketed by a second, independent, hard-coded rule set (not the same thresholds as Stage2's scoring bands):


if revenue ≥ $1B and margin_delta > 5% and fcf_delta > 5%:  "high_conviction_growth"
elif margin_delta > 8% and accel ≥ 0:                        "operational_turnaround"
else:                                                          "speculative_asymmetric"
Layer 4 — Stage4.py: shortlist
Ranks each bucket by Stage2's total_score, keeps top 18 / 12 / 30 respectively (TOP_HC/TOP_TA/TOP_SPEC). Attaches a templated string thesis (generate_thesis, Stage4.py:33-48) chosen by simple if/elif on bucket name and accel — this is not a calculated or LLM-generated thesis; it's one of four fixed sentences. Output → prime_candidates.json.

Layer 5 — Stage5_1.py: Structural quality score (independent of everything above)
compute_quant_features() (Stage5_1.py:252-431) re-pulls 5 years of annual (FY) data — a totally separate API pull from Stage1's TTM quarterly data:


rev_cagr_5y      = (revenue[newest]/revenue[oldest, 5y back])^(1/4) - 1
gross_margin_delta = grossProfitMargin[newest] - grossProfitMargin[oldest]
op_margin_delta     = operatingProfitMargin[newest] - operatingProfitMargin[oldest]
roic_latest      = key_metrics[newest].returnOnInvestedCapital
fcf_margin_latest = freeCashFlow[newest] / revenue[newest]
dilution_5y      = (shares[newest] - shares[oldest]) / shares[oldest]
fcf_margin_median_5y, fcf_margin_std_5y = median/pstdev of 5y FCF-margin series
roic_std_5y      = pstdev of 5y ROIC series
cycle_distortion_flag = True if (fcf_margin_latest > 2 × fcf_margin_median_5y) OR (roic_std_5y > 0.15)
Kill flags (compute_kill_flags, Stage5_1.py:586-608) — informational strings only (low CAGR, low ROIC, margin deterioration, negative FCF, high dilution). Confirmed: these flags never gate, filter, or numerically penalize anything downstream — they're just carried through as metadata (see Step 7).

level1_score (score_company, Stage5_1.py:610-653) — a second independent additive score:


Growth (rev_cagr_5y):  ≥20%→+5, ≥12%→+3, ≥7%→+1, else -2
ROIC (roic_latest):    ≥18%→+5, ≥12%→+3, ≥8%→+1, else -3
FCF margin:             ≥15%→+4, ≥8%→+2, ≥0%→0, else -3
GPT signals block:      +0 always — gpt is hardcoded to {} (see Step 7, dead code)
cycle_distortion_flag:  -2 if True
Output → level1_results.json, sorted by level1_score descending. This score is never reused as a numeric input to Stage5_2's final score — Stage5_2 only reuses the raw metrics (rev_cagr_5y, fcf_margin_latest, dilution_5y) that Stage5-1 computed, plus the level1_score value itself only as a display field and as the sort key for choosing which candidates to run through Stage5_2 first (TOP_N, unused by default — None).

Layer 6 — Stage5_2.py: Live valuation + Quality-Adjusted Value (QAV) score — this is the layer that produces the final buy/hold/avoid recommendation
Rebuilds enterprise value from live data (not the stale FY-snapshot EV):


EV = live_market_cap + total_debt - cash
ev_to_fcf     = EV / FCF
fcf_yield     = FCF / EV
ev_to_ebitda  = EV / EBITDA
ev_to_sales   = EV / revenue
earnings_yield = EPS / live_price
quality_bucket() (Stage5_2.py:392-428) — a third independent 0–5-ish point score:


ROIC:      ≥18%→+2, ≥12%→+1, else -1
FCF margin: ≥15%→+2, ≥8%→+1, else -1
Dilution:   ≤5%→+1, ≥25%→-2, (else 0 — a dead zone between 5% and 25%)
score ≥4 → "high_quality"; score ≤0 → "low_quality"; else "mid_quality"
valuation_bucket() (Stage5_2.py:431-467) — EV/FCF banded (≤15 cheap, 15–30 fair [two redundant overlapping branches], ≥30 expensive), with FCF-yield as fallback if EV/FCF is unavailable/negative.

quadrant() = cross of quality_bucket × valuation_bucket → one of HQ_Cheap / HQ_FairValue / HQ_Expensive / MQ_Cheap / MQ_FairValue / MQ_Expensive / LQ_Cheap / LQ_FairValue / LQ_Expensive / Unclassified.

score_quality_adjusted_value() (Stage5_2.py:498-562) — the actual continuous "final score" used for ranking within the app:


QAV =
    clamp((min(ROIC, 0.50) - 0.10) × 40,  -5, 8)      # capped ROIC quality
  + clamp((FCF_margin - 0.05) × 40,        -4, 8)      # FCF margin quality
  + clamp((rev_cagr_5y - 0.04) × 30,       -3, 6)      # growth
  + clamp(-(dilution_5y) × 10,             -8, 2)      # capital discipline
  + clamp(-(SBC/revenue) × 50,             -6, 1)      # SBC penalty (ceiling of 1 is unreachable — see Step 7)
  + clamp((25 - EV/FCF) / 5,               -6, 6)      # absolute valuation, anchored at 25x
  + clamp(((peer_median_EV/FCF - EV/FCF) / peer_median_EV/FCF) × 6,  -3, 3)   # peer-relative valuation
  + clamp(((ROIC_uncapped - peer_median_ROIC) / peer_median_ROIC) × 3, -2, 2) # peer-relative quality (uses UNCAPPED roic — see Step 7)
investment_view() (Stage5_2.py:688-719) — a fixed rule table on quality_bucket × valuation_bucket:

Cheap	Fair	Expensive
High Quality	Strong Buy	Accumulate	Watch for Pullback
Mid Quality	Speculative Buy	Neutral	Avoid
Low Quality	Value Trap Risk	Avoid	Avoid
GPT nuance layer (call_gpt_nuance) then produces a narrative decision_tilt (strong_buy/buy/hold/avoid) from the same computed numbers (no new facts allowed by the prompt). A durability overlay (Stage5_2.py:948-990, 1124-1168) can silently downgrade strong_buy → buy in the narrative if roic_std_5y > 15%, fcf_margin_std_5y > 10%, cycle_distortion_flag, or dilution_5y > 50% — but this overlay only edits the text field, never the quality_adjusted_value_score or quadrant themselves.

Layer 7 — generate_report.py: the actual "final_score"

persistence_bonus = (# of days this ticker has appeared in pipeline_history.json across all past runs) × 0.5
final_score = quality_adjusted_value_score + persistence_bonus
"Top Research Candidates" = stocks where quadrant == "HQ_Cheap" AND gpt_nuance.decision_tilt ∈ {buy, strong_buy}, sorted by final_score descending, top 5 shown.

Full pipeline formula, stated honestly
There is no single weighted equation producing "the final score." The true structure is:


Universe
 → Stage1 hard filters (survive/die)
 → Stage2 additive inflection score (12 correlated sub-terms) → hard multi-signal gate → rank
 → Stage3 sector exclusion filters → rule-based bucket (3 buckets)
 → Stage4 top-N-per-bucket cut (score used only as sort key, then discarded)
 → Stage5-1 additive structural score (3 terms + dead GPT block + cycle penalty) → rank
     [raw metrics carried forward; level1_score itself mostly discarded]
 → Stage5-2 quality_bucket (rule score) × valuation_bucket (rule bands) → quadrant
     → QAV continuous score (8 clamped additive terms, live-market-driven)
     → investment_view (fixed lookup table on the two buckets)
     → GPT decision_tilt narrative (same numbers, restated in English; can be downgraded by a durability overlay that never touches the number)
 → generate_report.py: final_score = QAV + persistence_bonus
     → "Top Research Candidate" = HQ_Cheap AND GPT says buy/strong_buy
STEP 4 — File Reference Table
File	Key functions	Inputs	Calculations	Outputs	Used by
buildUniverse.py	fetch_universe()	FMP /company-screener	None (pure filter)	tickers.txt	Stage1
Stage1.py	compute_metrics(), run_scan()	FMP income/balance/cashflow (20 quarterly)	TTM growth, margins, FCF margin, net debt/EBITDA + hard kill filters	stage1_output.json	Stage2
Stage2.py	score_company()	stage1_output.json	12-term additive inflection score + multi-signal gate	stage2_output.json	Stage3, Stage4
Stage3.py	fetch_profile(), filter loop	stage2_output.json + FMP /profile	Sector/industry exclusion + 3-way bucket rule	stage3_output.json	Stage4
Stage4.py	rank_bucket(), build_output(), generate_thesis()	stage3_output.json	Top-N cut per bucket + templated thesis text (no calc)	prime_candidates.json	Stage5_1
Stage5_1.py	compute_quant_features(), compute_kill_flags(), score_company(), analyze_single_stock_stage5_1()	prime_candidates.json + FMP 5Y income/cashflow/balance/ratios/key-metrics	5Y CAGR, margin deltas, ROIC, FCF margin, dilution, volatility, cycle flag + additive structural score	level1_results.json	Stage5_2, trigger_pipeline.py
Stage5_2.py	build_value_quality_inputs(), quality_bucket(), valuation_bucket(), quadrant(), score_quality_adjusted_value(), investment_view(), call_gpt_nuance(), analyze_single_stock_stage5_2()	level1_results.json + live FMP quote/key-metrics/ratios/income/cashflow/balance/peers	Live EV rebuild, valuation multiples, quality bucket, valuation bucket, quadrant, QAV score, investment view, GPT nuance + durability overlay	level2_results.json, api_cache/*.json	generate_report.py, trigger_pipeline.py, index.html (via API)
generate_report.py	persistence loop, final-score loop	level2_results.json + pipeline_history.json	final_score = QAV + persistence_bonus; HQ_Cheap+buy filter	email_summary.txt, updated pipeline_history.json	End of pipeline (email/report)
run_pipeline.py	orchestration only	—	none	subprocess chain	Cron/manual trigger
trigger_pipeline.py	Flask routes /run, /report, /analyze-stock	ticker string from HTTP request	Calls Stage5_1/Stage5_2 directly, no Stage1-4	JSON response to index.html	index.html
index.html	analyze(), badgeClass(), friendlyLabel(), renderGPT()	user-typed ticker	client-side formatting only	rendered UI	End user browser
category_config.py	—	—	—	—	Nothing imports this file — dead code
test.py	—	AAPL income statement	none (debug print)	console only	Nothing — standalone scratch script
STEP 5 — Dependency Map

buildUniverse.py  ──(tickers.txt)──▶  Stage1.py
                                          │  compute_metrics()
                                          │  [TTM growth/margins/FCF/debt filters]
                                          ▼
                                   stage1_output.json
                                          │
                                          ▼
                                       Stage2.py
                                          │  score_company()
                                          │  [12-term inflection score, multi-signal gate]
                                          ▼
                                   stage2_output.json
                                          │
                                          ▼
                                       Stage3.py
                                          │  sector/industry exclusion + bucket rule
                                          ▼
                                   stage3_output.json
                                          │
                                          ▼
                                       Stage4.py
                                          │  top-N per bucket, templated thesis
                                          ▼
                                  prime_candidates.json
                                          │
                                          ▼
                                      Stage5_1.py
                                          │  compute_quant_features() [5Y CAGR/margins/ROIC/FCF/dilution/volatility]
                                          │  compute_kill_flags()  (informational only, never gates)
                                          │  score_company()       (GPT sub-block is DEAD — gpt={} always)
                                          ▼
                                   level1_results.json
                                          │
                                          ▼
                                      Stage5_2.py
                                          │  build_value_quality_inputs()
                                          │  live EV rebuild ── ev_to_fcf / ev_to_ebitda / ev_to_sales / fcf_yield / earnings_yield
                                          │        │
                                          │        ▼
                                          │  quality_bucket()  ───┐
                                          │  valuation_bucket() ──┼──▶ quadrant()
                                          │        │              │
                                          │        ▼              ▼
                                          │  score_quality_adjusted_value() (QAV — the continuous score)
                                          │        │
                                          │        ▼
                                          │  investment_view()  (rule lookup on the two buckets)
                                          │        │
                                          │        ▼
                                          │  call_gpt_nuance()  → decision_tilt
                                          │        │
                                          │        ▼
                                          │  durability overlay (can downgrade strong_buy→buy in TEXT ONLY,
                                          │                        never touches QAV score or quadrant)
                                          ▼
                                   level2_results.json
                                          │
                                          ▼
                                  generate_report.py
                                          │  final_score = QAV + persistence_bonus (from pipeline_history.json)
                                          │  Top candidates = quadrant==HQ_Cheap AND decision_tilt∈{buy,strong_buy}
                                          ▼
                                   email_summary.txt   ◀── FINAL OUTPUT (batch path)


                     ── ALTERNATE, PARALLEL ENTRY POINT (bypasses everything above Stage5) ──

     index.html (user types ticker)
            │  POST /analyze-stock
            ▼
    trigger_pipeline.py
            │  analyze_single_stock_stage5_1(ticker)  ── direct call, no Stage1-4 filters ever applied
            │  analyze_single_stock_stage5_2(ticker, stage1_result)
            ▼
    JSON → rendered in browser  ◀── FINAL OUTPUT (interactive path)
The two entry points recombine only in shared code (Stage5_1.py/Stage5_2.py functions), not in shared data — a ticker analyzed via the web UI never passes through the Stage2 inflection gate or the Stage3 sector exclusions.

STEP 6 — Worked Example: AAPL, using real cached data
api_cache/AAPL_*.json contains an actual, previously-fetched FMP response set (FY2025 income/balance/cashflow/ratios/key-metrics, plus its real peer list). This is genuine data the pipeline produced, not a fabrication. I reconstructed Stage5_2's math exactly from these files. Note: level1_results.json (Stage5-1's output for AAPL) is not present locally, so the 5-year-trend-only inputs (rev_cagr_5y, dilution_5y, margin deltas, volatility, cycle_distortion_flag) are not available from local data — I flag every such value explicitly below rather than inventing one.

Confirmed from cached data

Revenue (FY25)         = $416,161M
FCF (FY25)              = $98,767M       → fcf_margin_latest = 98,767/416,161 = 23.74%
EBITDA                  = $144,427M
Total debt              = $112,377M
Cash                     = $35,934M
FY market cap (proxy)    = $3,818,744M   (live /quote wasn't cached — this is the FY snapshot, used as the closest real stand-in)
EV = 3,818,744 + 112,377 − 35,934        = $3,895,187M   (matches FMP's own stored enterpriseValue exactly — validates the reconstruction)
ROIC (returnOnInvestedCapital)           = 51.97%
Gross margin                              = 46.91%
Operating margin                          = 31.97%
SBC / revenue                             = 3.09%
Net debt / EBITDA                         = 0.53
Valuation multiples (Stage5_2 formulas, live EV rebuild):


EV/FCF     = 3,895,187 / 98,767   = 39.45x
FCF yield  =            = 2.54%
EV/EBITDA  = 3,895,187 / 144,427  = 26.97x
EV/Sales   = 3,895,187 / 416,161  = 9.36x
Earnings yield ≈ EPS(7.49)/implied price(~$255) = 2.93%   (matches FMP's own earningsYield field almost exactly)
Peer set — confirmed real: AAPL's cached /stock-peers returned 9 symbols; MAX_PEERS=8 keeps the first 8 (GOOGL, META, MSFT, NVDA, NXT, RIME, SONY, TBCH), dropping TSM — exactly matching which peer files exist in the cache.


Peer EV/FCF values: GOOGL 52.32, META 37.13, MSFT 52.05, NVDA 46.89, NXT 8.49, RIME -18.18, SONY 14.48, TBCH 518.04
→ peer_median_EV/FCF = 42.01

Peer ROIC values: GOOGL 21.8%, META 18.0%, MSFT 22.0%, NVDA 62.9%, NXT 23.6%, RIME 141.1%, SONY 4.1%, TBCH 8.8%
→ peer_median_ROIC = 21.92%
Stage5_2 QAV score, computed term by term

ROIC term:        adj_roic = min(0.5197, 0.50) = 0.50 (CAPPED — real ROIC of 51.97% is clipped)
                   clamp((0.50-0.10)×40, -5, 8) = clamp(16, -5, 8) = 8.00   [hits ceiling]

FCF margin term:   clamp((0.2374-0.05)×40, -4, 8) = clamp(7.49, -4, 8) = 7.49

Growth term:       NOT COMPUTABLE from local data (needs Stage5-1's 5Y series) — omitted, flagged

Dilution term:     NOT COMPUTABLE from local data — omitted, flagged

SBC term:          clamp(-(0.0309)×50, -6, 1) = clamp(-1.545, -6, 1) = -1.55

Valuation term:    clamp((25-39.45)/5, -6, 6) = clamp(-2.89, -6, 6) = -2.89

Peer EV/FCF term:  rel = (42.01-39.45)/42.01 = 0.061 → clamp(0.061×6, -3, 3) = +0.37

Peer ROIC term:    relq = (0.5197-0.2192)/0.2192 = 1.371 → clamp(1.371×3, -2, 2) = 2.00  [hits ceiling, uses UNCAPPED roic]

Partial QAV (5 of 8 terms computable) = 8.00 + 7.49 - 1.55 - 2.89 + 0.37 + 2.00 = 13.42
Classification

quality_bucket: ROIC≥18%→+2, FCF≥15%→+2, dilution unknown→+0  ⇒ score=4 ⇒ "high_quality"
valuation_bucket: EV/FCF=39.45 ≥ 30 ⇒ "expensive"
quadrant: HQ_Expensive
investment_view: "Watch for Pullback"
Result

AAPL ⇒ Quality: high_quality | Valuation: expensive | Quadrant: HQ_Expensive
QAV Score ≈ 13.4 (partial — growth & dilution terms unavailable locally; full run would add roughly
             +0.8 to +2.3 more given AAPL's low/negative historical dilution and modest revenue growth,
             but that is an INFERENCE, not from local data)
Investment View: Watch for Pullback
This traces cleanly to code and matches intuition: AAPL is unambiguously high-quality (extreme ROIC, strong FCF margin) but trades at 39x EV/FCF against a peer median of 42x — so it is not penalized much on a relative basis, yet the absolute-valuation term (anchored at a fixed 25x) still drags the score down. Note also: because AAPL's ROIC is capped at 50% for the direct-ROIC term but used uncapped (51.97%) for the peer-relative term, the same underlying number is treated two different ways within one score — see Step 7.

STEP 7 — Audit Findings
Confirmed issue — Correlated growth sub-metrics stacked in Stage2's score. s_growth (from growth_1y), s_accel (from accel = growth_1y − cagr_2y), and s_cagr (from cagr_2y) are three separate additive terms (up to +3, +2, +2 = +7 combined) all derived from the same three numbers, two of which (accel and cagr_2y) are algebraically related to the third (growth_1y). A company with strong recent revenue growth gets rewarded for that single underlying fact up to three times.
Stage2.py:138-157

Confirmed issue — Same pattern for margin quality. s_margin (delta), s_margin_flip (neg→pos flip), and s_op_quality (absolute level) are three separate terms (up to +3, +3, +2 = +8) all measuring the same underlying signal — improving/high operating margin — from overlapping data.
Stage2.py:162-180

Confirmed issue — Same pattern for FCF quality. s_fcf, s_fcf_flip, s_fcf_quality (up to +3, +2, +2 = +7) — same triple-counting structure as above, applied to FCF margin.
Stage2.py:184-203

Confirmed issue — Inconsistent ROIC capping within one scoring function. In score_quality_adjusted_value(), the direct ROIC term uses adj_roic = min(roic, 0.50) (explicitly capped "to avoid denominator distortions"), but the peer-relative ROIC term two blocks later reuses the raw, uncapped roic variable. The same distortion risk the code explicitly guards against in one term is left unguarded in the other, four lines away. Demonstrated concretely in the AAPL example above (52% used raw in the peer term, 50% used in the direct term).
Stage5_2.py:520-523 vs Stage5_2.py:552-559

Confirmed issue — generate_report.py's quadrant list cannot match the current quadrant() function's possible outputs. Stage5_2.py's quadrant() (lines 470-496) can only return HQ_Cheap / HQ_FairValue / HQ_Expensive / MQ_Cheap / MQ_FairValue / MQ_Expensive / LQ_Cheap / LQ_FairValue / LQ_Expensive / Unclassified. But generate_report.py's order list (line 146-151) is ["HQ_Cheap", "Mixed_or_Unclear", "HQ_Expensive", "LQ_Expensive"]. "Mixed_or_Unclear" is a string the current code can never produce, so that report section is permanently empty — and, more materially, every MQ_*, LQ_Cheap, LQ_FairValue, and Unclassified group is silently dropped from the emailed report, even though those stocks exist in groups and were fully scored. As direct evidence this is a real regression (not a hypothetical): the checked-in email_summary.txt on disk shows an actual, populated "Mixed_or_Unclear" section from a prior version of the code — proving quadrant()'s return values changed at some point and generate_report.py was never updated to match.
generate_report.py:146-170 vs Stage5_2.py:470-496, cross-checked against email_summary.txt

Confirmed issue — Stage5-1's entire GPT scoring block is dead code. analyze_single_stock_stage5_1() (line 692-712) hardcodes gpt_out = {} and never calls call_gpt_for_level1(), fetch_text_bundle(), or build_gpt_input() — all of which are fully implemented but never invoked from anywhere in the file. Consequently the "GPT signals" section of score_company() (lines 640-648, checking pricing_power_signal, margin_direction_signal, management_credibility_score, etc.) always operates on an empty dict and contributes exactly 0 to every level1_score, silently. Also dead: extract_revenue_series(), normalize_segmentation(), extract_transcript_texts() — defined, never called.
Stage5_1.py:610-653 (dead branch), Stage5_1.py:220,234,437,516,547,563 (unreferenced functions)

Confirmed issue — category_config.py is entirely unused. Grep across the whole repo shows CATEGORY_RULES/DEFAULT_CATEGORY are never imported anywhere. It reads like an intended replacement for Stage3's/Stage4's hard-coded classification logic, but that switch was never made.
repo-wide grep, zero references outside the file itself

Confirmed issue — Stage5-1's kill_flags never gate anything. compute_kill_flags() returns human-readable warning strings (low CAGR, low ROIC, margin deterioration, negative FCF, high dilution), but nothing in Stage5_1, Stage5_2, or generate_report.py filters, penalizes, or even displays them in index.html's rendered UI — they only ever survive as an unused key in the raw JSON dump.
Stage5_1.py:586-608; confirmed no downstream reference in Stage5_2.py or generate_report.py

Confirmed issue — peer_count_used field is referenced but never populated. Stage5_2.py main()'s result_row includes "peer_count_used": peer_medians.get("peer_count_used"), but peer_medians is only ever built as {"peer_ev_to_fcf": ..., "peer_roic": ...} — that key is never set, so this output field is always None.
Stage5_2.py:771-774 vs Stage5_2.py:1194

Potential issue — Double exposure to one valuation ratio. score_quality_adjusted_value()'s absolute-valuation term and peer-relative-valuation term are both driven by the same ev_to_fcf figure (one compares it to a fixed 25x anchor, the other to the peer median). If a stock's EV/FCF is unusually low, both terms move favorably together, meaning up to 9 of roughly 43 max positive score points come from one ratio interpreted two ways. This may be intentional (absolute + relative framing is a legitimate analytical choice) but the magnitude interaction wasn't obviously accounted for.
Stage5_2.py:536-550

Potential issue — Durability overlay only edits text, never the number. The "durability penalty" logic (high ROIC/FCF volatility, cycle distortion, or >50% dilution) downgrades the GPT's decision_tilt from strong_buy to buy in the narrative, but never touches quality_adjusted_value_score or quadrant. Two stocks with identical QAV scores and quadrants can therefore carry different investment-view text purely based on this overlay, while ranking identically by every other output. Could be intentional (keep quant "pure," let GPT layer carry qualitative caution) but worth confirming that's the intent.
Stage5_2.py:948-990, Stage5_2.py:1124-1168

Observation — SBC penalty's stated clamp ceiling of +1 is unreachable. clamp(-(sbc)×50, -6, 1) — since sbc_to_revenue is a non-negative ratio, -(sbc)×50 can never exceed 0, so the term's real range is [-6, 0], not [-6, 1]. Cosmetic, not a functional bug, but the code implies a possible positive contribution that can never occur.
Stage5_2.py:533

Observation — Redundant valuation bands. valuation_bucket()'s two branches 15 < ev_fcf <= 25 → "fair" and 25 < ev_fcf < 30 → "fair" produce an identical result and could be one 15 < ev_fcf < 30 branch. Not a bug, just unmerged logic.
Stage5_2.py:446-450

Observation — Quality-bucket dilution has a scoring "dead zone." dilution_5y between 5% and 25% contributes 0 to quality_bucket's score — neither rewarded nor punished — creating a flat middle band bounded by two hard cliffs at 5% and 25% rather than a gradient.
Stage5_2.py:417-422

Observation — Redundant filter duplication across stages. net_debt_to_ebitda > 4 is enforced identically in both Stage1 (line 130) and Stage2 (line 75). Harmless (thresholds match exactly, so nothing conflicts) but it's dead redundancy, not layered defense-in-depth with different values.

Observation — Two disconnected "quality" scoring systems. Stage5-1's level1_score and Stage5-2's quality_bucket/QAV both score ROIC and FCF margin from the same raw fields but with different thresholds, weights, and caps, and Stage5-1's composite score is never blended numerically into Stage5-2's — only the raw metrics survive. This is architecturally coherent (Stage5-1 = "is it durable," Stage5-2 = "is it mispriced today," per the file's own docstring), but it means there isn't one "quality score" in this system — there are two, and they can disagree.

Observation (my interpretation, not from code) — ROIC-based quality signal is vulnerable to buyback distortion. AAPL's 52% ROIC (invested capital of only ~$32B against $3.9T EV) is real cached data, but AAPL's tiny invested-capital base is itself a byproduct of a decade of buybacks depleting book equity, not necessarily proof of exceptional new-capital efficiency. The model has no mechanism to distinguish "genuinely capital-light, high-return business" from "mature business that has shrunk its own equity base via repurchases." This is a domain-knowledge caveat, not a code defect — flagging as interpretation.

Observation — requirements.txt lists pandas and numpy, but neither is imported anywhere in the codebase (confirmed via grep). Unused dependency, no functional impact.

STEP 9 — How Your Stock Model Actually Thinks
This is fundamentally a two-question system layered on top of a growth-inflection screen, not a single unified valuation model. Question 1 (Stages 1–4): "is something changing for the better, right now, in this company's revenue growth trajectory, margins, or free cash flow?" Question 2 (Stage5): "given that a company is structurally strong, is it currently mispriced?" These are genuinely separate models bolted together — a momentum/inflection screen feeding a quality-vs-valuation screen.

What it fundamentally rewards:

Recent acceleration in revenue growth, operating margin, or FCF margin — and it rewards the same acceleration in up to three overlapping ways (confirmed in Step 7), so an inflecting company gets a disproportionately large score boost relative to a company that's merely stable.
High absolute ROIC and FCF margin (capped, but the caps are generous — 50% ROIC, no real ceiling on FCF margin).
Cheapness relative to both a fixed EV/FCF anchor (25x) and the company's own peer group simultaneously.
Capital discipline: low dilution, low stock-based compensation as a share of revenue.
Company size (a modest log-scale bonus for larger revenue bases), and, in the batch pipeline, sustained persistence — a stock that shows up as attractive on repeated runs slowly accrues a final_score bonus in generate_report.py.
What it fundamentally punishes:

Any TTM operating loss (Stage1's very first gate excludes it outright, before any scoring happens) — meaning early-stage or currently-unprofitable turnaround stories are excluded from the batch pipeline entirely, no matter how promising the trend, unless op margin is already positive.
Net debt above 4x EBITDA (hard exclude, twice).
Excluded sectors wholesale: REITs, miners, crypto miners, BDCs, MLPs, banks, asset managers, capital markets, oil & gas E&P/midstream, regulated utilities — these industries structurally cannot appear in the batch pipeline's output at all, regardless of fundamentals.
Companies whose growth/margin trend is flat or decelerating even if they are otherwise excellent (fails the multi-signal gate in Stage2 and never reaches Stage5 via the batch path).
Expensive valuation multiples relative to a hard 25x EV/FCF anchor and to peers.
What kind of company scores extremely well: A mid-to-large-cap business showing simultaneous acceleration in growth, margins, and FCF, already profitable, with high ROIC, low debt, minimal dilution/SBC, trading cheaper than both a fixed multiple and its peer set. Structurally, this looks for inflecting, high-quality compounders that the market hasn't caught up to yet — closer to a "quality-at-a-reasonable-price momentum" model than a pure value or pure growth model.

What kind of company can look fundamentally attractive but still score badly:

A mega-cap steady compounder like AAPL — which is exactly what the worked example shows. AAPL is unambiguously "high_quality" (52% ROIC, 24% FCF margin) but lands in HQ_Expensive → "Watch for Pullback," not because anything is wrong with the business, but because its EV/FCF (39x) sits above the model's fixed 25x anchor. More importantly: a stock like AAPL likely never even reaches Stage5 in the batch pipeline at all, because its 1-year growth is unlikely to be meaningfully accelerating versus its 2-year trend, so it would fail Stage2's multi-signal inflection gate and never survive to Stage3/4/5. The only way AAPL (or any steady, non-inflecting mega-cap) gets scored at all is through the single-stock web UI, which bypasses Stages 1–4 entirely. This is the single most important structural fact about how the model "thinks": the batch/email pipeline is not a general-purpose "find good stocks" screen — it is specifically an inflection-hunter, and it structurally cannot surface mature, stable, wonderful businesses that aren't currently accelerating.
A company with volatile or cyclically inflated recent margins (cycle_distortion_flag) gets its Stage5-1 score docked -2 and its GPT narrative softened — but its quality_adjusted_value_score and quadrant are untouched, so it can still rank at the top of a report section while carrying an un-scored earnings-normalization risk that only shows up if you read the GPT text.
A richly-valued but genuinely improving business gets penalized almost entirely on the valuation side, since the growth/margin-acceleration signal is so heavily (triple-)weighted upstream in Stage2 that by the time it reaches Stage5, only the valuation gate stands between it and a "Strong Buy."
Where valuation actually influences the answer: Exclusively in Stage5_2 — nowhere in Stages 1–4. It enters through the fixed 25x EV/FCF anchor and the peer-relative EV/FCF comparison, both baked into the QAV score, and separately (redundantly) as the row/column of the quality×valuation quadrant lookup table that drives investment_view.

Where growth actually influences the answer: Twice, independently, at different points: (1) Stage2's growth-acceleration signals (triple-counted, as shown) are the primary gate for even reaching the shortlist at all in the batch pipeline; (2) Stage5's rev_cagr_5y term contributes modestly to the final QAV score, but only up to ±6 points out of roughly 20–40 total possible — by the time you're at Stage5, growth has already done its main work as a gate, not as a score component.

Where risk actually influences the answer: Almost entirely as hard cliffs, not gradients — net debt/EBITDA > 4x kills a stock twice over (Stage1 and Stage2); dilution above 50% or ROIC/FCF volatility above set thresholds triggers a narrative downgrade only, with zero numeric consequence; SBC-heavy compensation structures get a modest, capped scoring penalty. There is no continuous "risk score" anywhere in the system — risk is either a binary exclusion or an unscored text flag.

Is this primarily a valuation model, quality model, growth model, or momentum model? It is best described as a growth/margin-inflection screen (momentum in fundamentals, not price) that gates entry, followed by a quality-vs-valuation overlay that determines the final verdict. It is not a discounted-cash-flow or intrinsic-value model of any kind — there is no explicit fair-value estimate anywhere in the codebase; "cheap" and "expensive" are always relative (to a fixed multiple or to peers), never absolute intrinsic worth.

If a stock receives a QAV score of 82 instead of 65, what fundamentally caused that difference (given the score's actual ceiling is roughly 43-ish across all 8 terms, a gap that large would actually require differences across nearly every term simultaneously): in practice, the two biggest possible swings are (1) ROIC/FCF-margin quality moving from mediocre to capped-maximum (worth up to ~16 points combined) and (2) EV/FCF moving from expensive to cheap relative to both the fixed anchor and peers (worth up to ~9 points combined) — those two axes, quality-capped and valuation-vs-two-benchmarks, are structurally capable of producing the largest score deltas in this system.

STEP 10 — Self-Validation Pass
I re-checked for alternative calculation paths before finalizing:

Confirmed run_single_ticker() inside Stage5_2.py (invoked when the script is run with a CLI arg) duplicates main()'s logic almost line-for-line — a second, near-identical implementation of the same EV rebuild/scoring path. Not a different formula, just duplicated code (an "Observation," not scored above for brevity, but worth knowing if you ever edit the scoring logic — you'd need to edit it in two places: Stage5_2.py:721-834 (analyze_single_stock_stage5_2, used by the web app) and Stage5_2.py:837-999 (run_single_ticker, used by CLI) and Stage5_2.py:1007-1221 (main, used by batch pipeline) — three separate copies of the same valuation/scoring block exist in this one file.
Confirmed no other file defines a competing score_quality_adjusted_value, quality_bucket, or investment_view — Stage5_2.py's versions are authoritative.
Confirmed email_summary.txt's stale "Mixed_or_Unclear" content is explained by, and consistent with, the quadrant-mismatch finding above (not a separate unexplained anomaly).
Everything in Steps 3–9 above is labeled Confirmed from code except the two places explicitly marked Observation (interpretation) — the buyback/ROIC caveat, and the score-of-82-vs-65 illustrative range (which is inference about magnitude, clearly flagged as such).