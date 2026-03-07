import requests
import os
from dotenv import load_dotenv
import json

# Load API key
load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

if not API_KEY:
    raise ValueError("FMP_API_KEY not found in .env file")

# Use NEW stable endpoint
url = f"https://financialmodelingprep.com/stable/income-statement?symbol=AAPL&apikey={API_KEY}"

response = requests.get(url)

print("Status Code:", response.status_code)

try:
    data = response.json()
    print("\nRaw JSON Response:\n")
    print(json.dumps(data, indent=2))  # Pretty print
except Exception as e:
    print("Error parsing JSON:", e)
