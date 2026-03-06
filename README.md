# Weather Mispricing Arbitrage (Research)

用于研究“天气预测模型共识概率 vs 预测市场隐含概率”之间错价的策略框架。

## 功能

- 六模型动态加权（基于 rolling Brier score）
- 错价 z-score 触发开平仓
- 交易成本建模（fee/slippage）
- walk-forward 回测
- 参数网格搜索
- Polymarket 只读市场数据客户端（发现+快照）

## 目录

```text
src/weather_arb/
  model_ensemble.py
  strategy.py
  validation.py
  polymarket.py
scripts/
  run_backtest.py
tests/unit/
  test_strategy_smoke.py
```

## 运行

```bash
pip install pandas numpy requests pytest
PYTHONPATH=src python scripts/run_backtest.py --input weather_market_data.csv --out-dir outputs
```

## 输入 CSV 最少字段

- `ts`
- `event_id`
- `market_prob`
- `label` (最终真实结果, 0/1)
- `ecmwf_prob`, `gfs_prob`, `hrrr_prob`, `nam_prob`, `ukmo_prob`, `cmc_prob`

## 输出

- `outputs/trades.csv`
- `outputs/walk_forward.csv`
- `outputs/grid_search.csv`

## 注意

这是研究框架，不构成投资建议。实盘前请补齐：
- 实时数据时间戳对齐与延迟测量
- 订单簿冲击与流动性建模
- 风险限额与熔断机制
