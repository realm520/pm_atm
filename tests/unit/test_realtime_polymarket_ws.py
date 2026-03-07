from weather_arb.realtime import PolymarketWSStreamer


def test_polymarket_ws_normalize_payload() -> None:
    s = PolymarketWSStreamer(
        asset_ids=["a1"],
        asset_to_market_id={"a1": "m1"},
        condition_to_market_id={"cond1": "m1"},
    )

    payload = {
        "event_type": "price_change",
        "market": "cond1",
        "price_changes": [
            {"asset_id": "a1", "price": "0.42", "best_bid": "0.41", "best_ask": "0.43"}
        ],
        "timestamp": "123",
    }
    ticks = s._normalize_payload(payload)
    assert len(ticks) == 1
    assert ticks[0]["id"] == "m1"
    assert abs(ticks[0]["price"] - 0.42) < 1e-9
