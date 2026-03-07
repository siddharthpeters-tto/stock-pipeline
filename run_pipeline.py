import subprocess
import os

files_to_clear = [
    "tickers.txt"
    "stage1_output.json",
    "stage2_output.json",
    "stage3_output.json",
    "prime_candidates.json",
    "level1_results.json",
    "level2_results.json"
]

for f in files_to_clear:
    if os.path.exists(f):
        os.remove(f)

scripts = [
    "buildUniverse.py",
    "Stage1.py",
    "Stage2.py",
    "Stage3.py",
    "Stage4.py",
    "Stage5-1.py",
    "Stage5-2.py"
]

for script in scripts:
    print(f"Running {script}...")
    subprocess.run(["python", script], check=True)

print("Pipeline finished.")