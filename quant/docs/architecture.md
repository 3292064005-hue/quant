# 架构说明

## 当前主架构

```text
DataProvider
  -> DataService
    -> MarketRepository
    -> DataImportRepository
      -> SQLiteStore

StrategyService (registry / loader)
  -> BacktestService
    -> BacktestEngine
      -> RunLifecycleManager
      -> TradeDayCursor
      -> RebalancePlanner
      -> ExecutionCoordinator
      -> DayPersistenceUnit
      -> PortfolioValuator
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
- `contract_adapter.py + mappers.py` 负责把第三方原始载荷映射为领域对象

### services/
应用服务层：
- 负责编排，不承载复杂领域状态
- `DataService` 统一管理在线同步 / CSV 导入 / 导入审计
- `BacktestService` 可按 `backtest.data_access_mode` 选择预加载或流式回测路径
- 启动装配已按命令最小依赖拆分：storage / data / report / full-backtest 四类上下文

### engines/
核心业务引擎：
- `PortfolioEngine`：目标仓位转订单
- `RiskEngine`：规则校验 + 顺序资金 / 仓位约束
- `ExecutionEngine`：执行与执行拒绝收敛
- `BacktestEngine`：整条研究主链协调器，但内部职责已进一步拆解

### core/
- `PortfolioValuator`：纯估值组件，统一账户 / 持仓快照与 EOD `daily_pnl`
- `EventBus`：进程内同步事件发布器；当前已接入最小消费链，用于回测日终事件收集
- `timeout_utils`：共享线程池 + best-effort timeout 包装
- `path_utils`：运行时路径解析与配置路径标准化
- `audit_actions`：固定审计动作枚举，避免散落字符串

### repositories/
持久化边界：
- 市场数据
- 订单与成交
- 账户与持仓快照
- 审计日志
- 回测运行元数据
- 导入运行与质量事件

## 当前关键设计

### 1. 估值与执行解耦

`Broker` 不再负责主链 EOD `daily_pnl` 计算。当前链路中：

- broker 负责账户现金、持仓数量与成交结果
- `PortfolioValuator` 负责：
  - 组合估值
  - 缺行情回退策略
  - EOD `daily_pnl`
  - 回撤计算

### 2. 运行模式边界

当前配置显式引入 `app.runtime_mode`：

- `research_backtest`：唯一正式支持的主链，要求 `broker.provider=mock`
- `paper_trade` / `live_trade`：仅保留真实 broker 边界与 runtime 检查，不再复用 `BacktestEngine`

`BacktestEngine` 现在只依赖研究回测语义的 `SimulatedExecutionPort`；QMT/PTrade 映射器保留在 `LiveBrokerPort` 边界。

### 3. 交易日驱动回测

回测主链支持两种模式：

- `preload`：适合样例数据和研究脚本
- `stream`：以交易日为主轴逐日读取 `bars_daily`

其中 `preload` 模式不再为了逐日迭代额外构造完整 `bars_by_date`，而是通过 `PreloadedDayBatchBuilder` 按日期归并已有 `bars_by_symbol`。

### 3. 日级事务边界

同一交易日内以下写入在一个显式事务内完成：

- orders
- fills
- account_snapshots
- position_snapshots
- audit_logs

失败时会整体回滚，避免出现撕裂状态。

### 4. 市场数据导入原子性

同一次导入中的以下写入在单个事务中完成：

- `securities`
- `trading_calendar`
- `bars_daily`
- `data_import_quality_events`

同时通过事务外的 `data_import_runs` 保留成功 / 失败结论，避免“数据回滚了但失败痕迹也丢失”。

### 5. 运行时边界契约

`QMT` / `PTrade` 适配层不再把第三方 payload 直接透传到业务链路，而是在边界层统一映射为：

- `AccountSnapshot`
- `PositionSnapshot`
- `Fill`
- `OrderRequest`

`check_broker_runtime()` 现已覆盖：

- 必要方法是否存在
- 方法签名能否接受工程约定参数
- 可选样本 payload 是否满足领域映射契约

### 6. 数据谱系与实验产物

`backtest_runs` 当前已显式记录：

- `import_run_id`
- `data_source`
- `data_start_date / data_end_date`
- `dataset_digest`
- `degradation_flags_json / warnings_json`
- `entrypoint / strategy_version / runtime_mode / report_artifacts_json`

`ReportService` 会把上述信息写入报告，并在重建报告时从数据库恢复数据谱系与 benchmark 曲线。

### 7. SQLite schema migration

数据库从单一“运行时补列”升级为版本化迁移：

- `schema_version`
- v1：兼容列 + 外键 + 索引
- v2：导入审计表
- v3：账户 / 持仓快照唯一索引
- v4：策略注册元数据与回测运行数据谱系字段
- v5：运行谱系/策略索引

## 当前仍属预留层

- `ui/main_window.py`：已明确降级为桌面原型展示层，不是完整业务前端
- `core/events.py`：提供发布 / 订阅原语，当前只接入了最小事件收集消费链，复杂编排仍属保留扩展点
- `benchmark_symbol`：当前已进入基准曲线与相对指标链；若缺少基准行情，报告会显式只输出策略净值指标

## 当前未建设项

- CI/CD 发布流水线
- 容器化部署
- 多账户并行
- 因子列式缓存层


## 补充：真实 broker CLI 注入路径

- `bootstrap(..., broker_clients={...})` 仍保留为最低层显式注入接口。
- 对于脚本/CLI，新增 `broker.client_factory` / `--broker-client-factory` 作为操作层注入路径。
- client factory 路径格式为 `package.module:callable`，工厂可返回单个 client，或返回 provider->client 映射。
- `check_runtime` 若检测到 client factory，会在 shallow 检查之外继续验证客户端方法契约。
