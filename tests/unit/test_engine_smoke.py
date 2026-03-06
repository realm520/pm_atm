import pandas as pd

from weather_arb.engine import PaperArbEngine


def test_engine_smoke() -> None:
    rows = []
    for i in range(240):
        rows.append(
            {
                "ts": i,
                "event_id": "evt-1" if i % 2 == 0 else "evt-2",
                "market_prob": 0.50 + (0.05 if i % 30 < 15 else -0.05),
                "ecmwf_prob": 0.58,
                "gfs_prob": 0.57,
                "hrrr_prob": 0.56,
                "nam_prob": 0.57,
                "ukmo_prob": 0.58,
                "cmc_prob": 0.57,
            }
        )

    df = pd.DataFrame(rows)
    result = PaperArbEngine().run(df)
    assert "summary" in result
    assert "trades" in result
