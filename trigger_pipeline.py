from flask import Flask, request, send_file
import subprocess
import os

app = Flask(__name__)

SECRET_KEY = os.getenv("PIPELINE_SECRET")

@app.route("/run", methods=["POST"])
def run_pipeline():
    if request.headers.get("Authorization") != SECRET_KEY:
        return {"error": "Unauthorized"}, 401

    subprocess.Popen(["python", "run_pipeline.py"])
    return send_file("email_summary.txt")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)