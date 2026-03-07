import os
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")

BASE_URL = "https://financialmodelingprep.com/stable/company-screener"

#Universe criters 10B -> 10T, US listed, no ETFs/funds, actively trading

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

    return response.json()


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
