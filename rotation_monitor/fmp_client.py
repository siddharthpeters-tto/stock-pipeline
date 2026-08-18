"""
rotation_monitor.fmp_client — thin FMP API wrapper for the rotation monitor.

Mirrors the `ApiClient` pattern already used in Stage5_1.py / Stage5_2.py
(dataclass, base_url + api_key, retrying `.get()`). Kept local to this
package rather than imported cross-module, matching how each stage in this
repo already owns its own client rather than sharing one.

Credentials come from the same environment variables as the rest of the
pipeline (FMP_API_KEY via .env) — nothing new to configure.
"""

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("FMP_BASE_URL", "https://financialmodelingprep.com/stable")
API_KEY = os.getenv("FMP_API_KEY")

REQUEST_TIMEOUT = 20
MAX_RETRIES = 3


class FmpApiError(RuntimeError):
    pass


@dataclass
class RotationFmpClient:
    base_url: str = BASE_URL
    api_key: Optional[str] = API_KEY

    def __post_init__(self):
        if not self.api_key:
            raise FmpApiError("FMP_API_KEY not found in environment (.env)")

    def get(self, path: str, params: Dict[str, Any]) -> Any:
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        params = dict(params)
        params["apikey"] = self.api_key

        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    return r.json()
                last_err = f"HTTP {r.status_code}: {r.text[:500]}"
            except requests.RequestException as e:
                last_err = str(e)

            if attempt < MAX_RETRIES:
                time.sleep(0.5 * attempt)

        raise FmpApiError(f"GET failed for {url} params={params}. Last error: {last_err}")

    def get_daily_bars(self, symbol: str, date_from: str, date_to: str):
        """
        Dividend-adjusted daily OHLCV for one symbol.

        Uses /historical-price-eod/dividend-adjusted, which returns
        adjOpen/adjHigh/adjLow/adjClose/volume, newest-date-first. This is
        treated as the canonical price series for all return, trend, and
        volume calculations in this module (adjusted prices "wherever
        appropriate", per the build spec).
        """
        data = self.get(
            "historical-price-eod/dividend-adjusted",
            {"symbol": symbol, "from": date_from, "to": date_to},
        )
        if not isinstance(data, list):
            return []
        return data
