from pathlib import Path


INDEX = Path(__file__).resolve().parents[1] / "index.html"


def test_snapshot_uses_existing_price_and_suggested_valuation_output():
    source = INDEX.read_text(encoding="utf-8")
    assert 'document.getElementById("summaryPrice").innerText = formatPrice(currentPrice)' in source
    assert "suggested.conservative?.buy_below_price" in source
    assert "suggested.base?.buy_below_price" in source
    assert "suggested.optimistic?.buy_below_price" in source
    assert "Suggested Buy-Below:</span><strong>N/M" in source


def test_valuation_tiles_consume_backend_states_without_frontend_thresholds():
    source = INDEX.read_text(encoding="utf-8")
    assert "s2.valuation_metric_states || {}" in source
    assert 'states.ev_to_fcf' in source
    assert 'states.fcf_yield' in source
    assert 'state-not_applicable' in source
    assert 'states.ev_to_sales' in source
    assert 'states.pe' in source
    assert 'states.ev_to_ebitda' in source
