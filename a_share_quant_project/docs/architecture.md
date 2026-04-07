# 架构说明

## 当前主架构

```text
DataProvider
  -> DataService
    -> MarketRepository
      -> SQLiteStore

StrategyService
  -> BacktestEngine
    -> PortfolioEngine
    -> RiskEngine
    -> ExecutionEngine
    -> OrderRepository
    -> AccountRepository
    -> AuditRepository
    -> BacktestRunRepository
    -> ReportService
```

## 分层职责

### adapters/
外部系统边界层：
- 数据源适配器：CSV / Tushare / AKShare
- 券商适配器：Mock / QMT / PTrade

### services/
应用服务层：
- 负责编排，不承载复杂领域状态
- 将配置、仓储、引擎组装成可执行主链

### engines/
核心业务引擎：
- `PortfolioEngine`：目标仓位转订单
- `RiskEngine`：规则校验 + 顺序资金 / 仓位约束
- `ExecutionEngine`：执行与执行拒绝收敛
- `BacktestEngine`：整条研究主链协调器

### repositories/
持久化边界：
- 市场数据
- 订单与成交
- 账户与持仓快照
- 审计日志
- 回测运行元数据

## 当前关键设计

### 1. 历史证券池过滤

`BacktestEngine` 不再盲信全量 `securities` 表，而是在每个交易日基于：
- `list_date`
- `delist_date`
- 当日是否有 bar
构造有效证券池。

### 2. 运行隔离

系统通过 `run_id` 建模一次完整回测运行，并将其贯穿：
- `backtest_runs`
- `orders`
- `fills`
- `position_snapshots`
- `account_snapshots`
- `audit_logs`

### 3. 风控-执行边界

风险拒绝和执行拒绝被明确区分为两类状态：
- `PRE_TRADE_REJECTED`
- `EXECUTION_REJECTED`

### 4. 预留层

以下组件当前仍然是预留层而非主运行入口：
- `core/events.py` 中的订阅能力
- `ui/main_window.py`

## 当前未建设项

- CI/CD
- 容器化部署
- 多账户并行
- 基准收益曲线与超额指标
- 因子列式缓存层


## 当前外部依赖边界

- 在线数据源与券商适配器都通过 best-effort timeout 包装调用。
- `data.request_timeout_seconds` 控制 AKShare / Tushare 外部调用等待时限。
- `broker.operation_timeout_seconds` 控制 QMT / PTrade 适配器边界调用等待时限。
- 该 timeout 机制用于避免主流程无限阻塞，但对不支持原生取消的第三方客户端，无法保证底层阻塞调用被立即终止。
