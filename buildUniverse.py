import os
import requests
from dotenv import load_dotenv

from fmp_helpers import normalize_fmp_rows, normalize_symbol

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

BASE_URL = "https://financialmodelingprep.com/stable/company-screener"

#Universe criters 10B -> 10T, US listed, no ETFs/funds, actively trading

def fetch(endpoint):
    return fetch_universe(endpoint)


def fetch_universe(exchange):

    url = (
        f"{BASE_URL}"
        f"?marketCapMoreThan=10000000000"
        f"&marketCapLowerThan=10000000000000"
        f"&exchange={exchange}"
        f"&country=US"
        f"&isEtf=false"
        f"&isFund=false"
        f"&isActivelyTrading=true"
        f"&limit=10000"
        f"&apikey={API_KEY}"
    )

    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        print(response.text)
        raise Exception("Failed to fetch universe")

    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"Unexpected universe response shape for {exchange}: {type(payload).__name__}")

    normalized = []
    for item in normalize_fmp_rows(payload):
        symbol = normalize_symbol(item.get("symbol"))
        if symbol:
            normalized.append({**item, "symbol": symbol})

    if not normalized:
        raise ValueError(f"Universe response for {exchange} was empty or invalid")

    return normalized


if __name__ == "__main__":

    nasdaq = fetch_universe("NASDAQ")
    nyse = fetch_universe("NYSE")

    symbols = set()

    for stock in nasdaq + nyse:
        symbol = stock.get("symbol")

        if (
            symbol
            and "-" not in symbol
            and "." not in symbol
        ):
            symbols.add(symbol)

    symbols = sorted(symbols)

    print(f"Universe size: {len(symbols)} stocks")

    with open("tickers.txt", "w") as f:
        for s in symbols:
            f.write(s + "\n")

    print("tickers.txt created")
