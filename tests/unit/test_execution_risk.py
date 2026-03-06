from weather_arb.execution import ExecutionConfig, OrderBookLevel, SlippageModel
from weather_arb.risk import RiskConfig, RiskManager


def test_slippage_model_buy_sell() -> None:
    m = SlippageModel(ExecutionConfig(taker_fee_bps=0.0, impact_coef=0.0))
    asks = [OrderBookLevel(price=0.51, size=5), OrderBookLevel(price=0.52, size=5)]
    bids = [OrderBookLevel(price=0.49, size=5), OrderBookLevel(price=0.48, size=5)]

    buy_px = m.estimate_fill_price("BUY", 2.0, asks=asks, bids=bids)
    sell_px = m.estimate_fill_price("SELL", 2.0, asks=asks, bids=bids)

    assert buy_px >= 0.51
    assert sell_px <= 0.49


def test_risk_manager_limits() -> None:
    r = RiskManager(RiskConfig(max_positions=1, max_event_notional=1.0, max_total_notional=1.0, daily_loss_limit=-1.0))
    ok, _ = r.can_open("e1", qty=1.0, price=0.5, open_positions=[], day_realized_pnl=0.0)
    assert ok

    ok2, reason = r.can_open(
        "e2",
        qty=2.0,
        price=0.6,
        open_positions=[{"event_id": "e1", "qty": 1.0, "entry_price": 0.5}],
        day_realized_pnl=0.0,
    )
    assert not ok2
    assert reason in {"max_positions", "max_total_notional", "max_event_notional"}
