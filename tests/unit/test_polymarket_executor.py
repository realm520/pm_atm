from __future__ import annotations

from dataclasses import dataclass

from weather_arb.orders import ExecutionIntent, OrderSide, OrderStatus
from weather_arb.polymarket_executor import PolymarketExecutionConfig, PolymarketLiveExecutor


@dataclass
class DummyResp:
    status_code: int
    payload: dict

    @property
    def content(self) -> bytes:
        return b"{}"

    def json(self):
        return self.payload


class DummySession:
    def __init__(self) -> None:
        self.headers = {}

    def post(self, url, json=None, timeout=None):
        if url.endswith("/orders"):
            return DummyResp(200, {"exchange_order_id": "ex-1", "status": "NEW"})
        if url.endswith("/orders/ex-1/cancel"):
            return DummyResp(200, {"ok": True})
        return DummyResp(404, {})

    def get(self, url, timeout=None):
        if url.endswith("/orders/ex-1"):
            return DummyResp(
                200,
                {
                    "status": "FILLED",
                    "filled_qty": 2,
                    "avg_fill_price": 0.44,
                    "fills": [{"order_id": "ex-1", "qty": 2, "price": 0.44, "ts": "2026-01-01T00:00:00Z"}],
                },
            )
        return DummyResp(404, {})


def test_polymarket_executor_adapter_roundtrip() -> None:
    ex = PolymarketLiveExecutor(PolymarketExecutionConfig(base_url="https://example.com", api_key="k"))
    ex.session = DummySession()  # type: ignore[assignment]

    intent = ExecutionIntent(
        event_id="e1",
        asset_id="a1",
        side=OrderSide.BUY,
        qty=2.0,
        limit_price=0.44,
        timeout_sec=15.0,
        client_order_id="cid-xx",
    )
    oid, st, rej = ex.place_order(intent)
    assert oid == "ex-1"
    assert st == OrderStatus.NEW
    assert rej == ""

    ok = ex.cancel_order(oid)
    assert ok is True

    st2, fq, avg, fills, _ = ex.get_order_update(oid)
    assert st2 == OrderStatus.FILLED
    assert fq == 2.0
    assert avg == 0.44
    assert len(fills) == 1
