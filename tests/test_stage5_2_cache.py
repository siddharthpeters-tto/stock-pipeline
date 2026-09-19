import importlib.util
from pathlib import Path

import fmp_helpers


ROOT = Path(__file__).resolve().parents[1]


def load_stage5_2(module_name):
    spec = importlib.util.spec_from_file_location(module_name, str(ROOT / "Stage5_2.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fmp_cache_hit_does_not_increment_request_counter(monkeypatch, tmp_path):
    stage5_2 = load_stage5_2("stage5_2_cache_counter")
    counter = fmp_helpers.FmpRequestCounter()
    monkeypatch.setattr(stage5_2, "CACHE_DIR", str(tmp_path))
    params = {"period": "FY", "limit": 1}
    stage5_2.write_cache("AAPL", "key_metrics", {"returnOnInvestedCapital": 0.2}, params)

    class FailingApi:
        def get(self, *args, **kwargs):
            raise AssertionError("cache hit should avoid the network")

    result = stage5_2.fetch_latest_key_metrics(FailingApi(), "AAPL")

    assert result["returnOnInvestedCapital"] == 0.2
    assert counter.snapshot() == {"total": 0, "by_endpoint": {}}


def test_cache_key_isolated_by_request_parameters(monkeypatch, tmp_path):
    stage5_2 = load_stage5_2("stage5_2_cache_isolation")
    monkeypatch.setattr(stage5_2, "CACHE_DIR", str(tmp_path))

    limit_one = {"period": "FY", "limit": 1}
    limit_five = {"period": "FY", "limit": 5}
    assert stage5_2.cache_path("AAPL", "key_metrics", limit_one) != stage5_2.cache_path(
        "AAPL", "key_metrics", limit_five
    )

    stage5_2.write_cache("AAPL", "key_metrics", {"request_limit": 1}, limit_one)
    assert stage5_2.read_cache("AAPL", "key_metrics", limit_five) is None
