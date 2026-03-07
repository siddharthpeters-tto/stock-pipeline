import json
import os
from collections import defaultdict
from datetime import datetime, UTC

INPUT_FILE = "level2_results.json"
HISTORY_FILE = "pipeline_history.json"
OUTPUT_FILE = "email_summary.txt"

# --------------------------------
# Load latest pipeline results
# --------------------------------

with open(INPUT_FILE, "r") as f:
    data = json.load(f)

# --------------------------------
# Ensure history file exists
# --------------------------------

if not os.path.exists(HISTORY_FILE):
    with open(HISTORY_FILE, "w") as f:
        json.dump([], f)

with open(HISTORY_FILE, "r") as f:
    history = json.load(f)

from datetime import datetime, UTC
today = datetime.now(UTC).strftime("%Y-%m-%d")

# --------------------------------
# Update persistence history
# --------------------------------

for stock in data:

    history.append({
        "ticker": stock["ticker"],
        "date": today,
        "score": stock["quality_adjusted_value_score"],
        "quadrant": stock["quadrant"]
    })

with open(HISTORY_FILE, "w") as f:
    json.dump(history, f, indent=2)

# --------------------------------
# Count persistence
# --------------------------------

persistence_count = defaultdict(int)

for record in history:
    persistence_count[record["ticker"]] += 1

# --------------------------------
# Compute final score
# --------------------------------

for stock in data:

    ticker = stock["ticker"]

    base_score = stock["quality_adjusted_value_score"]

    persistence_bonus = persistence_count[ticker] * 0.5

    stock["final_score"] = base_score + persistence_bonus

# --------------------------------
# Identify top research candidates
# --------------------------------

research_candidates = []

for stock in data:

    quadrant = stock["quadrant"]
    decision = stock["gpt_nuance"]["decision_tilt"]

    if quadrant == "HQ_Cheap" and decision in ["buy", "strong_buy"]:
        research_candidates.append(stock)

research_candidates = sorted(
    research_candidates,
    key=lambda x: x["final_score"],
    reverse=True
)

# --------------------------------
# Group stocks by quadrant
# --------------------------------

groups = defaultdict(list)

for stock in data:
    groups[stock["quadrant"]].append(stock)

for quadrant in groups:

    groups[quadrant] = sorted(
        groups[quadrant],
        key=lambda x: x["final_score"],
        reverse=True
    )

# --------------------------------
# Build email report
# --------------------------------

lines = []

lines.append("PIPELINE RESULTS REPORT\n")

# --------------------------------
# Top Research Candidates
# --------------------------------

lines.append("===== TOP RESEARCH CANDIDATES =====\n")

if len(research_candidates) == 0:

    lines.append("No strong research candidates this run.\n")

else:

    for stock in research_candidates[:5]:

        ticker = stock["ticker"]
        score = round(stock["quality_adjusted_value_score"], 2)
        final_score = round(stock["final_score"], 2)
        decision = stock["gpt_nuance"]["decision_tilt"]

        lines.append(
            f"{ticker} | Base Score: {score} | Final Score: {final_score} | Signal: {decision}"
        )

# --------------------------------
# Quadrant Results
# --------------------------------

order = [
    "HQ_Cheap",
    "Mixed_or_Unclear",
    "HQ_Expensive",
    "LQ_Expensive"
]

for quadrant in order:

    if quadrant not in groups:
        continue

    lines.append(f"\n===== {quadrant} =====\n")

    for i, stock in enumerate(groups[quadrant], 1):

        ticker = stock["ticker"]
        base_score = round(stock["quality_adjusted_value_score"], 2)
        final_score = round(stock["final_score"], 2)
        persistence = persistence_count[ticker]
        bucket = stock["bucket"]

        lines.append(
            f"{i}. {ticker} | Score: {base_score} | Final: {final_score} | Persistence: {persistence} | {bucket}"
        )

# --------------------------------
# Write report
# --------------------------------

with open(OUTPUT_FILE, "w") as f:
    f.write("\n".join(lines))

print("Report generated: email_summary.txt")