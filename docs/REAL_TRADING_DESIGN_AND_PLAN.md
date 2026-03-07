# Real Trading Design & Development Plan

## 决策
- 存储：SQLite（WAL + busy_timeout）
- 并发策略：单 writer（应用层串行写入）
- 执行方式：taker-only（第一阶段）
- 订单超时：15s 撤单
- 直接真单：是（低仓位冷启动 + 全量监控）
- 监控主题：Telegram topic_id=52

## 架构

```text
signal -> risk -> execution_service -> exchange_adapter
                    |                    |
                    v                    v
                  sqlite             exchange api/ws
                    |
                    v
             alerts + circuit breaker
```

## 订单状态机

- PENDING_SUBMIT
- NEW
- PARTIALLY_FILLED
- FILLED (terminal)
- CANCEL_REQUESTED
- CANCELED (terminal)
- REJECTED (terminal)
- EXPIRED (terminal)
- FAILED (terminal)

## 告警阈值（5分钟窗口）
- reject_rate warn: 3%
- reject_rate crit: 8%
- hard stop: 12% 或连续拒单 >= 5

## 已实现（本轮）
1. `orders.py`：订单领域模型与转移规则
2. `order_store.py`：SQLite 持久化（订单+fills）
3. `execution_service.py`：下单、轮询刷新、超时撤单、执行风控指标
4. `exchange_sim.py`：仿真交易所执行器
5. `live.py`：告警支持 Telegram topic（message_thread_id）
6. `run_live_paper.py`：新增 `--telegram-thread-id`

## 下一步（接真单）
1. 新建 `polymarket_executor.py`（签名、下单、撤单、查单）
2. 将 live runner 增加 execution mode（paper/live）
3. 引入仓位账本（部分成交一致性）
4. 对接 topic 52 的运行告警（Bot Token + chat_id + thread_id）

## 上线建议
- Day1-2：目标仓位 20%-30%
- 达标后逐步提升（单日不超 30%）
- 熔断与 kill-switch 必开
