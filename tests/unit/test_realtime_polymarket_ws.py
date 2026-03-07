from weather_arb.realtime import PolymarketWSStreamer


def test_polymarket_ws_normalize_payload() -> None:
    payload = {
        "type": "market",
        "data": [
            {"market_id": "1", "price": "0.42", "bestBid": 0.41, "bestAsk": 0.43, "timestamp": 123},
            {"market_id": "2", "lastTradePrice": 0.77},
        ],
    }
    ticks = PolymarketWSStreamer._normalize_payload(payload)
    assert len(ticks) == 2
    assert ticks[0]["id"] == "1"
    assert abs(ticks[0]["price"] - 0.42) < 1e-9
    assert ticks[1]["id"] == "2"
