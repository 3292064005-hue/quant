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
- `DataService` 现为 façade：在线同步 / CSV 导入仍走统一入口，但导入落库、谱系计算、preload/stream 读取已拆到独立协作组件
- `BacktestService` 可按 `backtest.data_access_mode` 选择预加载或流式回测路径
- 启动装配已按命令最小依赖拆分：storage / data / report / full-backtest 四类上下文

### engines/
核心业务引擎：
- `PortfolioEngine`：目标仓位转订单
- `RiskEngine`：规则校验 + 顺序资金 / 仓位约束
- `ExecutionEngine`：执行与执行拒绝收敛
- `BacktestEngine`：整条研究主链协调器；运行期协作件已下沉到 `engines/backtest_runtime.py`

### core/
- `PortfolioValuator`：纯估值组件，统一账户 / 持仓快照与 EOD `daily_pnl`
- `EventBus`：进程内同步事件发布器；当前已接入统一 `runtime_events` 镜像链，回测运行事件与 operator/session 事件都可汇总到正式运行事件流
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

- `research_backtest`：正式研究/回测主链，要求 `broker.provider=mock`
- `paper_trade` / `live_trade`：独立 runtime assembly；不复用 `BacktestEngine`，而是走 `TradeOrchestratorService + OperatorTradeWorkflow` 的正式命令链
- 两条主链现已共享 `SharedExecutionContractService`：
  - research/backtest 通过它统一解析 `required_history_bars / should_rebalance / generate_targets`
  - paper/live 通过它统一执行基础 pre-trade 输入校验与 projected target weights 计算

`BacktestEngine` 现在只依赖研究回测语义的 `SimulatedExecutionPort`；QMT/PTrade 映射器保留在 `LiveBrokerPort` 边界。两条主链仍保持独立 orchestrator，但不再各自维护一份轻微漂移的执行合同解析逻辑。

### 3. 交易日驱动回测

回测主链支持两种模式：

- `preload`：适合样例数据和研究脚本
- `stream`：以交易日为主轴逐日读取 `bars_daily`，并通过第二条流式摘要链生成 dataset digest，不回退全量加载

其中 `preload` 模式不再为了逐日迭代额外构造完整 `bars_by_date`，而是通过 `PreloadedDayBatchBuilder` 按日期归并已有 `bars_by_symbol`。

交易日/会话解析不再默认把 bars 日期直接等同于正式交易日历；当前通过 `TradingSessionService` 统一执行：
- `demo`：允许用 bars 推导交易日
- `derive`：允许推导但记录强告警
- `strict`：缺少正式日历时直接阻断

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

- `dataset_version_id`
- `import_run_id`
- `import_run_ids`
- `data_source`
- `data_start_date / data_end_date`
- `dataset_digest`
- `degradation_flags_json / warnings_json`
- `entrypoint / strategy_version / runtime_mode / report_artifacts_json / run_manifest_json`

`ReportService` 会把上述信息写入报告，并在重建报告时优先使用 `run_manifest_json` 中的 `benchmark_initial_value` 恢复 benchmark 曲线；若存在运行事件，还会同步写出独立事件日志；当 sidecar 文件缺失时，重建链会回退使用 manifest 中持久化的 `run_event_summary`，避免事件统计静默清零。运行状态现在拆为 `RUNNING -> ENGINE_COMPLETED -> COMPLETED`，若仅产物写出失败则落为 `ARTIFACT_EXPORT_FAILED`，避免把业务结果错误标成整体失败。

### 7. SQLite schema migration

数据库从单一“运行时补列”升级为版本化迁移：

- `schema_version`
- v1：兼容列 + 外键 + 索引
- v2：导入审计表
- v3：账户 / 持仓快照唯一索引
- v4：策略注册元数据与回测运行数据谱系字段
- v5：运行谱系/策略索引
- v6：精确谱系与 dataset_versions
- v7：dataset version 指纹
- v8：run manifest 合约
- v9：run events 完整明细入库，并将 artifact 路径收口为可迁移相对路径
- v10：策略组件声明/能力标签入库，运行前检查输出四层能力状态
- v11：订单执行字段扩展（order_type / time_in_force / filled_quantity / avg_fill_price / last_error）
- v12：strategy blueprint
- v13：research runs
- v14：operator trade sessions / command events
- v15：fills 补齐 broker_order_id 链接字段，支持 operator trade session 与外部 broker 回补对账


### 8. 装配层第二阶段解耦

当前装配层不再由单一 `bootstrap.py` 或单一 `assembly_steps.py` 承担全部职责，而是拆为：

- `assembly_core.py`：基础 context / registry 安装
- `assembly_broker.py`：broker 与 execution engine 装配
- `assembly_registry.py`：provider / component / workflow / plugin 注册
- `assembly_services.py`：data / strategy / report / backtest 服务栈装配
- `assembly_steps.py`：仅作为兼容导出层

### 9. DataService / BacktestEngine 第二阶段收口

- `services/data_service.py`：保留对外 façade 和旧 API
- `services/data_import_persistence.py`：导入审计与事务落库
- `services/data_lineage_builder.py`：谱系 digest 与 dataset version 汇总
- `services/data_market_reader.py`：preload/stream 数据组装
- `services/data_service_types.py`：Loaded/Streaming bundle 契约与流式谱系跟踪器
- `engines/backtest_runtime.py`：承担交易日游标、调仓规划、执行协调、事务持久化、preload 批构造

这样可以继续保持旧调用方不变，同时把大体量中心文件拆成可独立复核与演进的协作件。

## 当前仍属预留层

- `ui/main_window.py`：已明确定位为桌面只读运营面板，不是完整业务前端
- `services/ui_read_models.py`：桌面层稳定读模型；把 runtime check / provider / workflow / research recent-runs 从原始 registry/result 打平成版本化 projection
- `core/events.py`：提供发布 / 订阅原语与 `EventJournal` 追加式历史；当前已把 `DAY_CLOSED / ORDER_FILLED / ORDER_REJECTED` 等回测运行事件与 operator/session 运行事件统一镜像到 `runtime_events`，复杂异步/跨节点编排仍属保留扩展点
- `benchmark_symbol`：当前默认示例已收口到 `600000.SH`，能够直接与仓内 sample_data 对齐生成基准曲线；若缺少基准行情，报告会显式只输出策略净值指标

## 当前保留扩展项

以下能力已从“缺失”推进为“可用基础版”，但仍有继续扩展空间：

- 多账户作用域：订单 / 成交 / trade session 已补齐 `account_id`，operator submit/sync/reconcile 已按账户作用域推进；operator 账户/持仓快照现也会正式落库，并在 `operator_snapshot` 中同时输出实时视图与 `persisted_account / persisted_positions`。跨账户 supervisor 并行调度与账户级 portfolio/risk 聚合仍属后续扩展
- research 缓存层：`dataset_summary / feature_snapshot / signal_snapshot` 已进入持久化缓存表并支持命中/裁剪；缓存键现已纳入 `cache_schema_version`、provider signature、strategy version，并优先按 dataset version/digest 做失效控制。`research_run_edges` 已作为正式边表写入，用于表达 `contains_step / session_member / related_by_dataset`。更大规模列式存储、批处理调度与并行执行仍属后续扩展
- research artifact lineage：主记录 / 子步骤 / batch 根节点已收口；更复杂的跨批次 lineage 分析与实验管理仍可继续增强
- broker 事件推进：paper/live 现已具备正式 `OperatorSupervisorService`、CAS 风格 claim/renew/release 语义、可续租会话租约、`operator_run_supervisor` 脚本入口，以及 broker `subscribe_execution_reports(...)` 订阅链；当适配器不支持订阅或订阅中断时，会自动回退到 `poll_execution_reports/query_trades` 轮询同步。release 现在只在真实释放成功时写 `SUPERVISOR_RELEASED`，否则写 `SUPERVISOR_RELEASE_SKIPPED`。当前仍未扩展到分布式 supervisor 集群、跨节点协调与真实券商 push 环境的生产级验证

已补齐最小工程化交付基础：

- `.github/workflows/ci.yml`：仓内 CI 质量门禁
- `Dockerfile`：最小容器化运行入口
- `scripts/verify_release.py` + `scripts/build_clean_release.py`：发布前校验与干净打包


## 补充：真实 broker CLI 注入路径

- `bootstrap(..., broker_clients={...})` 仍保留为最低层显式注入接口。
- `bootstrap_operator_context(...)`：保留 paper/live 只读 operator snapshot 装配入口。
- `bootstrap_trade_operator_context(...)`：新增正式 operator trade 装配入口，负责 paper/live lane 的写路径、命令会话、共享 RiskEngine pre-trade 校验、正式订单实体落库与审计留痕；内部 `order_id` 在服务层与仓储层双重收口，避免跨入口复用时污染历史订单归属。
- 对于脚本/CLI，新增 `broker.client_factory` / `--broker-client-factory` 作为操作层注入路径。
- client factory 路径格式为 `package.module:callable`，工厂可返回单个 client，或返回 provider->client 映射。
- `check_runtime` 若检测到 client factory，会在 shallow 检查之外继续验证客户端方法契约。


## 10. v0.5.5 扩展边界

### 10.1 执行内核

- `domain/models.py`：订单层新增 `OrderType / TimeInForce / OrderTicket / ExecutionReport / LiveOrderSubmission`
- `engines/execution_models/`：正式拆出 `fill / fee / slippage / tax` 模型
- `engines/execution_engine.py`：执行结果不再只有 `fills + rejected`，还包含 `tickets + reports`
- `core/events.py`：新增 `EventType.ORDER_SUBMITTED / ORDER_ACCEPTED / ORDER_PARTIALLY_FILLED / EXECUTION_REPORT`

### 10.2 Provider / Workflow / Plugin

- `providers/`：把 `calendar / instrument / bar / feature / dataset` 暴露为正式 provider
- `workflows/`：把 `backtest / report / research / replay` 暴露为正式 workflow
- `strategy.research_signal_run_id`：允许把 `workflow.research` 产出的正式 `signal_snapshot` 重新绑定到 `workflow.backtest` 的输入合同
- `plugins/`：当前内建 `risk / analyser / scheduler / broker / dataset` 五类插件，并支持通过配置启停/外部发现
- `plugin_manager`：当前除注册/配置外，已补齐 `context_ready / before_workflow_run / after_workflow_run / shutdown` 生命周期与执行留痕
- `app/bootstrap.py`：已降为薄入口；装配逻辑继续拆到 `runtime_assembly.py + assembly_core.py + assembly_broker.py + assembly_registry.py + assembly_services.py`

### 10.3 UI Operator Plane

`ui/main_window.py` 不再只堆单页文本，而是拆成：

- 边界说明
- 配置摘要
- 运行时健康
- 策略生命周期
- 订单执行
- 风险告警
- 导入审计
- 报告与回放

该界面仍是只读 operator plane，不提供写操作入口；正式写路径统一走 `scripts/operator_submit_order.py` / `a-share-quant-operator-submit-order`。当 broker 外部副作用与本地账本之间发生断裂时，恢复入口统一走 `scripts/operator_reconcile_session.py` / `a-share-quant-operator-reconcile-session`。


## 11. 审计修复补充

- 补齐运行时子包 `__init__.py`，确保源码态与安装态包发现结果一致
- `backtest.execution` 的内建模型选择器现由配置契约严格校验并实际参与 `bootstrap` 组装
- 发布洁净度补充 `.gitignore`、`MANIFEST.in` 与 `scripts/build_clean_release.py`，显式排除 `build/dist/*.egg-info/__pycache__/runtime/.git/.coverage*` 等产物；clean release 失败时也会回收 staging 目录


## 增量架构收口（v0.5.5+）

- `providers/feature_provider.py` 已从单函数包装升级为正式特征目录与批量横截面产出入口。
- `providers/dataset_provider.py` 已输出正式 `DatasetRequest` / `DatasetSummary`，避免 research workflow 继续依赖裸 `DataService`。
- `workflows/research_workflow.py` 已覆盖 `snapshot / feature / signal / experiment` 四类 research 摘要产物。
- `engines/execution_registry.py` 引入 `ExecutionModelRegistry`，执行模型扩展不再要求直接修改 `_build_execution_engine()` 的条件分支。
- `services/strategy_service.py` 在保留 class-path 兼容装载的前提下，补充正式 `strategy_blueprint`，把策略组件拆分为 `universe / factor / signal / portfolio / execution / risk / benchmark`，并把 component_manifest 真正绑定到回测执行运行时。


## 12. v0.5.2 生命周期与 research 正式入口

- 新增 `scripts/research.py` 与 `a-share-quant-research` 入口，使 `workflow.research` 从测试/注册表对象升级为正式操作入口。
- `workflow.backtest / report / replay / research` 当前都会向 `PluginManager` 发出执行前后 hook；PluginDescriptor 也显式声明 capability_tags / hook_contracts，避免插件只剩“已注册”而没有能力契约。
- UI operator snapshot 当前额外暴露：
  - `available_provider_details`
  - `available_workflow_details`
  - `installed_plugin_details`
  - `plugin_lifecycle_events`
  - `registered_components`
  - `recent_research_runs`
  - `latest_execution_summary / latest_risk_alerts / latest_report_replay_summary`：统一由 `RunQueryService` 提供只读查询模型，供 CLI snapshot 与桌面 UI 共享


## 交付口收口（v0.5.5 delivered）

- 包级官方入口统一为 `python -m a_share_quant`；Docker 也改为复用该入口，不再指向无模块执行保护的 `a_share_quant.cli`。
- `scripts/daily_run.py` 默认只消费库内已存在数据；若要显式导入再跑，必须传 `--import-csv`（兼容别名 `--csv`）。这样导入批次与回测批次的谱系语义不再隐式漂移。
- 新增 `scripts/operator_snapshot.py`，作为 paper/live lane 的正式只读 operator 入口；仅装配 broker 读路径、runtime checks 与最新运行摘要，不提供任何写操作。
- 发布前质量门禁统一收口到 `scripts/verify_release.py`：基础门禁始终执行仓内内存语法编译检查、禁用 pytest cache provider 的 `pytest -q -p no:cacheprovider`、入口 smoke、源码树 operator acceptance、wheel 安装态 bundled-config / console_scripts / launcher stub 实跑，以及 clean release zip 校验；若当前环境已安装 `ruff` / `mypy`，则自动追加全仓 `ruff check .` 与 `mypy .`。同时，UI / Tushare / AKShare 可选运行面现在通过本地 shim 驱动真实代码路径 smoke，不再停留在 import 级检查。
- research workflow 新增 batch task spec，允许通过 `configs/research_batch.json` 这类 JSON 任务文件执行批量 experiment summary；batch summary 作为唯一主记录持久化，任务级 experiment 与 dataset/feature/signal 子步骤统一挂到该 batch 主记录下。


- operator acceptance profile：仓内新增 `configs/operator_paper_trade_demo.yaml` + `a_share_quant.demo.operator_demo_broker:create_client`，用于 release gate 与 clean checkout 的 paper/live operator 烟雾链自证；真实 broker 仍通过 `configs/operator_paper_trade.yaml` + `broker.client_factory` 接入。
