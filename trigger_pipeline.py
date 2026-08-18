from flask import Flask, request, send_file, send_from_directory, jsonify
import subprocess
import os
from Stage5_1 import analyze_single_stock_stage5_1
from Stage5_2 import analyze_single_stock_stage5_2
from concurrent.futures import ThreadPoolExecutor, as_completed


app = Flask(__name__)

SECRET_KEY = os.getenv("PIPELINE_SECRET")

@app.route("/")
def index():
    return send_file("index.html")

# Start the pipeline
@app.route("/run", methods=["POST"])
def run_pipeline():

    if request.headers.get("Authorization") != SECRET_KEY:
        return {"error": "Unauthorized"}, 401

    subprocess.Popen(["python", "run_pipeline.py"])

    return {"status": "pipeline started"}


# Retrieve the finished report
@app.route("/report", methods=["GET"])
def get_report():

    if not os.path.exists("email_summary.txt"):
        return {"error": "Report not ready"}, 404

    return send_file("email_summary.txt")


# ---------------------------------------------------------------------
# Market Rotation Monitor
# ---------------------------------------------------------------------

@app.route("/rotation")
def rotation_page():
    return send_file("rotation.html")


# Start the rotation pipeline
@app.route("/run-rotation", methods=["POST"])
def run_rotation_pipeline():

    if request.headers.get("Authorization") != SECRET_KEY:
        return {"error": "Unauthorized"}, 401

    subprocess.Popen(["python", "run_rotation.py"])

    return {"status": "rotation pipeline started"}


@app.route("/rotation-state", methods=["GET"])
def get_rotation_state():

    path = os.path.join("outputs", "rotation_state.json")

    if not os.path.exists(path):
        return {"error": "Rotation state not ready"}, 404

    return send_file(path)


@app.route("/rotation-report", methods=["GET"])
def get_rotation_report():

    path = os.path.join("outputs", "rotation_report.md")

    if not os.path.exists(path):
        return {"error": "Rotation report not ready"}, 404

    return send_file(path)


@app.route("/rotation-asset/<path:filename>", methods=["GET"])
def get_rotation_asset(filename):

    # only ever serve PNGs out of outputs/, and never let a path walk out of it
    safe_name = os.path.basename(filename)

    if not safe_name.lower().endswith(".png"):
        return {"error": "Not found"}, 404

    if not os.path.exists(os.path.join("outputs", safe_name)):
        return {"error": "Not found"}, 404

    return send_from_directory("outputs", safe_name)

@app.route("/analyze-stock", methods=["POST"])
def analyze_stock():

    data = request.get_json()

    ticker = data.get("ticker")
    include_peers = data.get("include_peers", False)

    ticker = ticker.upper()

    try:

        stage1 = analyze_single_stock_stage5_1(ticker)
        stage2 = analyze_single_stock_stage5_2(ticker, stage1)

        result = {
            "stage5_1": stage1,
            "stage5_2": stage2
        }

        # If competitor mode enabled
        if include_peers:

            peers = stage2.get("peer_symbols", [])
            peers = [p for p in peers if p != ticker]

            comparison = []

            def analyze_peer(p):

                try:
                    s1 = analyze_single_stock_stage5_1(p)
                    s2 = analyze_single_stock_stage5_2(p, s1)

                    return {
                        "ticker": p,
                        "roic": s2["metrics"].get("roic"),
                        "fcf_margin": s2["metrics"].get("fcf_margin"),
                        "rev_cagr_5y": s2["metrics"].get("rev_cagr_5y"),
                        "ev_to_fcf": s2["metrics"].get("ev_to_fcf"),
                        "quadrant": s2.get("quadrant"),
                        "quality_adjusted_value_score": s2.get("quality_adjusted_value_score")
                    }

                except:
                    return None


            with ThreadPoolExecutor(max_workers=6) as executor:

                futures = [executor.submit(analyze_peer, p) for p in peers]

                for future in as_completed(futures):

                    peer_result = future.result()

                    if peer_result:
                        comparison.append(peer_result)

            result["comparison"] = comparison

        return jsonify(result)

    except Exception as e:

        return {"error": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)