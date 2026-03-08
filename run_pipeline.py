import subprocess
import os
import time

for script in scripts:
    start = time.time()
    print(f"\nRunning {script}...")

    subprocess.run(["python", script], check=True)

    print(f"{script} finished in {round(time.time()-start,2)} seconds")

files_to_clear = [
    "tickers.txt"
    "stage1_output.json",
    "stage2_output.json",
    "stage3_output.json",
    "prime_candidates.json",
    "level1_results.json",
    "level2_results.json",
    "email_summary.txt"
]

if not os.path.exists("pipeline_history.json"):
    with open("pipeline_history.json", "w") as f:
        f.write("[]")

for f in files_to_clear:
    if os.path.exists(f):
        os.remove(f)

scripts = [
    "buildUniverse.py",
    "Stage1.py",
    "Stage2.py",
    "Stage3.py",
    "Stage4.py",
    "Stage5_1.py",
    "Stage5_2.py"
    "generate_report.py"
]

for script in scripts:
    print(f"Running {script}...")
    subprocess.run(["python", script], check=True)

print("Pipeline finished.")