# A 股量化研究与交易工作站

当前发布版本：`0.5.5`

本工程当前定位为 **research/backtest 正式主链 + paper/live operator command 主链 + 桌面只读 operator plane** 的量化工作站底座，聚焦三条主链闭环：

- 数据采集、本地治理与导入审计
- 策略回测 / 风控 / 模拟执行 / 审计
- 报表、数据谱系与运行留痕

本轮落地的主改造不是表层重构，而是把几个会影响企业级稳定性的关键边界真正收口：

- 市场数据导入改为**显式原子事务**，避免 `securities / trading_calendar / bars_daily` 撕裂写入
- 新增 `data_import_runs / data_import_quality_events`，导入质量事件不再只打日志
- QMT / PTrade 适配器改为**领域对象归一化边界**，不再把第三方原始 payload 直接透传到引擎层
- `check_broker_runtime()` 从“只看方法存在”升级为“方法签名 + 可选样本 payload 映射校验”
- `BacktestEngine` 内部拆分为生命周期管理、交易日推进、调仓规划、执行协调、日终持久化五段职责
- 执行层升级为**订单状态机 + OrderTicket + ExecutionReport + fill/fee/slippage/tax model** 的正式内核
- 新增 `providers/ + workflows/ + plugins/`，把 research 数据读取、工作流编排、扩展点注册收口为正式边界
- 装配层已进入第二阶段：`bootstrap.py` 仅保留薄入口，具体职责拆到 `runtime_assembly.py + assembly_core.py + assembly_broker.py + assembly_registry.py + assembly_services.py`
- 预加载回测不再重复构造 `bars_by_date` 大映射，降低 preload 模式额外内存放大
- SQLite 从“运行时轻量补丁”升级为**版本化 schema migration**，并补齐唯一索引与导入审计表
- 运行时路径默认改为**相对配置文件目录解析**，消除 cwd 敏感；示例配置同步显式写为 `../runtime/...`
- CLI 入口统一下沉到 `a_share_quant.cli`，同时改走 workflow 层，而不是直接把 CLI 绑死到 service
- 桌面 UI 拆成 runtime / strategy lifecycle / order monitor / risk alert / import audit / replay 多面板只读 operator plane

## 1. 适用范围

- 研究环境：支持 CSV、AKShare、Tushare 数据适配器
- 运行模式：`research_backtest / paper_trade / live_trade` 三条 lane 已正式区分
- 执行环境：research backtest 仍要求 `broker.provider=mock`；paper/live 通过 `operator trade workflow` 消费真实 broker
- 产品形态：headless 回测主链已闭环；paper/live 现已补齐正式命令链、会话留痕与 operator snapshot；桌面端仍明确定位为只读运营面板

## 2. 目录说明

```text
configs/                 配置文件
sample_data/             示例行情数据
docs/                    架构与操作文档
scripts/                 兼容脚本入口（薄壳）
a_share_quant/           主代码包与统一 CLI
tests/                   单元 / 集成 / 回放测试
```

## 3. 快速开始

### 3.1 安装依赖

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

也可以安装为可执行 CLI：

```bash
pip install -e .
```

### 3.2 初始化数据库

```bash
python scripts/init_db.py --config configs/app.yaml
```

或：

```bash
a-share-quant-init-db --config configs/app.yaml
```

### 3.3 导入示例行情

```bash
python scripts/sync_market_data.py --config configs/app.yaml --csv sample_data/daily_bars.csv
```

导入结果会额外输出 `import_run_id`，用于排查导入审计；回测阶段则会生成独立的 `dataset_version_id`，把真实使用的数据快照与导入批次集合以稳定 provenance 指纹绑定。

### 3.4 运行回测

```bash
python scripts/daily_run.py --config configs/app.yaml
# 若希望导入 CSV 后再跑：
python scripts/daily_run.py --config configs/app.yaml --import-csv sample_data/daily_bars.csv
```

### 3.5 基于真实回测结果重建报告

默认重建最近一次已完成回测：

```bash
python scripts/generate_report.py --config configs/app.yaml
```

指定 run_id：

```bash
python scripts/generate_report.py --config configs/app.yaml --run-id run_xxxxxxxxxxxxxxxx
```

### 3.6 运行前健康检查

检查当前配置对应的数据源 / broker / 可选 UI 运行时是否具备。输出会同时给出 `config_ok / boundary_ok / client_contract_ok / operable_ok` 四层能力状态；当使用 `--strict` 时，只有在全部检查项通过且 `operable_ok=true` 时才返回 0：

```bash
python scripts/check_runtime.py --config configs/app.yaml --check-ui --strict
```

若需要在 CLI 中对真实 `qmt/ptrade` 执行客户端契约检查，请先把配置切到 `paper_trade/live_trade + qmt/ptrade` 的合法组合，再通过配置或参数提供 broker client factory：

```bash
python scripts/check_runtime.py \
  --config configs/app.yaml \
  --broker-client-factory your_pkg.your_module:build_client \
  --strict
```

### 3.7 执行正式 research workflow

```bash
python scripts/research.py --config configs/app.yaml --artifact experiment --csv sample_data/daily_bars.csv
```

示例数据的默认演示参数已收口到 `lookback=3 / top_n=2`，直接运行也会得到非空特征与信号产物。

可选 `--artifact`：`dataset / feature / signal / experiment / experiment-batch / recent-runs`。该入口会正式走 `workflow.research`，并把结果持久化到 `research_runs`。其中 `experiment` 现已区分“1 条用户主记录 + 同 session 的内部 dataset/feature/signal 子步骤”；`experiment-batch` 则以 batch summary 作为唯一主记录，任务级 experiment 与其子步骤统一挂到 batch 主记录下。`recent-runs` 默认只展示主记录，不再被内部步骤或 batch 子任务污染。

### 3.8 启动桌面只读运营面板与 operator 命令入口

```bash
python scripts/launch_ui.py --config configs/app.yaml

# paper/live lane 仓内自带 acceptance profile（默认配置即指向该 demo profile）
python scripts/operator_snapshot.py
python scripts/operator_submit_order.py --symbol 600000.SH --side BUY --price 10.50 --quantity 100 --trade-date 2026-01-05

# paper/live lane 对接真实 broker（显式覆盖配置或 client factory）
python scripts/operator_snapshot.py --config configs/operator_paper_trade.yaml --broker-client-factory your.module:create_client
python scripts/operator_submit_order.py   --config configs/operator_paper_trade.yaml   --broker-client-factory your.module:create_client   --symbol 600000.SH --side BUY --price 10.50 --quantity 100 --trade-date 2026-01-05 --approved
```

当前入口只启动**桌面只读运营面板**：展示配置摘要、运行时检查结果、最近导入/最近回测/质量事件的只读运营摘要与桌面边界说明；运行时检查结果与 CLI `check_runtime` 走同一套构造逻辑，并显式展示四层能力状态，不会再出现 UI 误绿灯。桌面层当前还增加了版本化 `ui_*` projection/read-model，避免直接消费 registry/raw result 导致字段错位。该入口仅支持官方范围 `research_backtest + mock`；paper/live 的只读查询请使用 `operator_snapshot`。

正式 `operator_submit_order` 命令链当前已额外收口三点：1）`order_id` 不再只靠 CLI 保证唯一；服务层会对空值、批内重复、历史冲突 ID 自动重签发，仓储层也会拒绝拿既有 `order_id` 改写另一笔订单，从而避免审计记录被跨会话覆盖；2）pre-trade reject 也会写入正式 `orders` 表，而不是只存在于事件流；3）paper/live operator 预交易校验与仓内共享 `RiskEngine` 对齐，`lot size / ST / 停牌 / 涨跌停` 等规则不再只停留在 research/backtest 侧。

注意：`configs/app.yaml` 默认是 `research_backtest + mock`，不能直接用于 operator 命令。当前 operator CLI 默认已切到仓内自带 acceptance profile `configs/operator_paper_trade_demo.yaml`；若要接入真实 broker，请改用 `configs/operator_paper_trade.yaml` 并显式提供 `broker.client_factory`（或通过 `--broker-client-factory` 覆盖）。

若当前环境未安装 `PySide6`，脚本会在启动前明确报错并给出安装提示，而不是在业务链路中途失败。运行态依赖现已与开发依赖拆分：生产/容器安装使用 `requirements.txt`，开发门禁使用 `requirements-dev.txt`。

### 3.9 运行测试

```bash
pytest -q
```

当前仓内自动化校验覆盖：配置、broker 契约、事务回滚、回放留痕、脚本入口、路径解析、Provider/Workflow/Plugin 注册、插件生命周期、research CLI、执行模型部分成交与启动异常收口。

补充说明：本轮已额外收口 **安装态包发现与交付洁净度**，确保 `pip install -e .` / wheel 安装不会遗漏运行关键子包，并显式排除 `build/dist/*.egg-info/__pycache__/runtime` 等发布污染物。

## 4. 当前关键配置

- `app.path_resolution_mode`
  - `config_dir`：相对配置文件目录解析，默认推荐
  - `cwd`：相对当前工作目录解析，仅用于兼容旧行为
- `backtest.data_access_mode`
  - `preload`：一次性加载行情后回测
  - `stream`：以交易日为主轴流式读取行情，并在不回退全量 preload 的前提下生成数据谱系
- `backtest.valuation.missing_price_policy`
  - `last_known`：优先使用最近一次有效价
  - `avg_cost`：缺失时退回持仓成本
  - `reject`：缺失时直接拒绝生成 EOD 快照
- `backtest.execution.*`
  - `event_mode`：当前仅支持 `bus`，表示执行生命周期事件走正式 EventBus
  - `fill_model`：当前内建 `volume_share`
  - `max_volume_share`：控制单 bar 可成交量上限
  - `allow_partial_fill`：是否允许在 volume share 约束下产生部分成交
  - `min_trade_lot`：最小整数手，A 股默认 100
- `data.allow_degraded_data`
  - 在线数据源发生降级时是否允许继续写入本地库
- `data.fail_on_degraded_data`
  - 是否将在线数据降级提升为失败
- `data.calendar_policy`
  - `demo`：允许用 bars 交易日推导会话，面向样例/演示
  - `derive`：允许推导但记录强告警
  - `strict`：缺正式交易日历时直接拒绝继续
- `app.runtime_mode`
  - `research_backtest`：正式研究/回测模式
  - `paper_trade` / `live_trade`：正式 operator trade lane，支持真实 broker 命令链与会话留痕
- `broker.operation_timeout_seconds`
  - 券商适配器 best-effort 超时
- `broker.strict_contract_mapping`
  - `true`：第三方 payload 不能严格映射为领域对象时立即失败
  - `false`：进入 best-effort 兼容模式，账户/持仓/成交会尽量回退为可用领域对象，并记录 warning
- `broker.client_factory`
  - 可选真实 broker 客户端工厂路径，格式 `package.module:callable`
  - 主要用于 `check_runtime` 与未来独立的 paper/live orchestration；research backtest 主链不会消费它
  - 工厂可返回当前 provider 的 client，或返回 `{"qmt": client, "ptrade": client}` 映射
  - 工厂可使用零参数签名，或声明 `config` / `provider` 关键字参数
- `plugins.enabled_builtin / disabled / external`
  - 支持按配置启停内建插件，并通过 `package.module:PluginClass` 装载外部插件
  - 外部插件必须是 `AppPlugin` 实例、子类或零参工厂返回值
- `strategy.params`
  - 通用策略构造参数字典；优先级高于兼容字段 `lookback/top_n/holding_days`
  - 外部策略若需要额外参数，应通过该字段显式提供
  - 当前策略注册会额外持久化 `component_manifest / capability_tags`，用于追踪信号/因子/组合/执行组件契约
- `strategy.research_signal_run_id`
  - 可选 research `signal_snapshot` 运行标识
  - 配置后，默认回测会把 `workflow.research` 落库的信号产物重新绑定到主回测执行链
  - 绑定时会校验 `promotion_package`，防止不兼容 research 产物直接晋级到执行链
  - CLI 也可通过 `--research-run-id` 临时覆盖

## 5. 架构边界

### 已落实

- 统一配置加载与运行时路径标准化
- 交易日驱动回测主线
- A 股 100 股整数手约束
- ST / 停牌 / 涨跌停 / 黑名单 / 熔断风控
- 订单、成交、持仓、账户、审计入库
- 日级事务边界与关键表外键引用完整性
- 市场导入运行审计与质量事件留痕
- MockBroker 完整可运行实现
- QMT / PTrade 适配器领域映射层
- 版本化 SQLite schema migration（当前 schema version = 25）
- 历史库升级到当前版本时会自动执行 migration；但旧库中已有行情行不会被自动回填精确 provenance，若需要严格行级来源需重新导入对应数据
- 报告重建、文档、脚本、测试闭环
- 非 broker 命令按最小依赖装配，不再被真实券商配置阻断
- schema 资源内置进包，源码树与安装态的数据库初始化入口统一

### 仍属预留或实验性质的部分

- `ui/main_window.py`：当前已升级为分面式桌面只读 operator plane，消费 projection/read model，但仍不是完整业务前端
- `core/events.py`：已升级为正式事件类型 + 同步 EventBus + `EventJournal` 追加式历史；回测运行事件与 operator/session 事件现在都会镜像进入统一 `runtime_events` 事件流，便于统一回放、摘要与只读查询；跨进程/异步分布式编排仍属后续扩展
- `providers/` / `workflows/` / `plugins/`：已作为正式边界落地；workflow 注册现受 runtime lane profile 约束，插件支持配置启停与外部发现
- `plugins/` 已补齐 `configure -> context_ready -> before_workflow_run -> after_workflow_run -> shutdown` 生命周期，workflow 不再只是“注册名字而不消费插件”
- `component_registry` 已从纯 metadata 注册升级为“组件对象 + descriptor 契约”双轨，operator plane 可区分 declarative / executable / runtime_instance
- `benchmark_symbol`：当前默认示例已改为 `600000.SH`，能够与仓内 sample_data 直接对齐生成基准曲线；若后续换成不存在的基准行情，报告会显式只输出策略净值指标
- `workflow.operator_trade` 已具备正式 `account_id` 作用域、allowlist 校验、CAS 风格 supervisor claim/renew/release 语义、可重复轮询的 `sync_session_events` 事件推进链，以及跨进程 `operator_run_supervisor` / broker `subscribe_execution_reports` 订阅入口；supervisor 现在会周期性续租并记录 `SUPERVISOR_RENEWED`，release 仅在实际释放成功时记录 `SUPERVISOR_RELEASED`，否则写入 `SUPERVISOR_RELEASE_SKIPPED`。会话级 `last_synced_at / last_supervised_at / supervisor_owner / supervisor_lease_expires_at / supervisor_mode / broker_event_cursor` 元信息已进入正式持久化，operator 账户/持仓快照也会在 submit/sync/reconcile 路径中落入正式持久化，并在 `operator_snapshot` 中同时输出 `persisted_account / persisted_positions` 与实时 `account_views`。当前仍未扩展到分布式 supervisor 集群与真实券商 push 环境的生产级验证
- research 现已具备持久化 `dataset_summary / feature_snapshot / signal_snapshot` 缓存表与命中路径，可跨实验复用结果；缓存键现已纳入 `cache_schema_version`、provider signature、strategy version，并优先按 dataset version/digest 做失效控制。research run 之间还会落正式 `research_run_edges`（如 `contains_step / session_member / related_by_dataset`）以支撑 replay/report 图谱查询。批处理调度器与更大规模的列式缓存/并行编排仍属后续扩展
- 回测运行已持久化 `dataset_version_id / import_run_id / import_run_ids / data_source / data_start_date / data_end_date / dataset_digest / strategy_version / runtime_mode / report_artifacts / run_manifest`，并且首次写出的报告文件会与该 artifact 清单保持一致；manifest 中会显式记录 `benchmark_initial_value`、可选 `event_log_path`、`artifact_status / artifact_errors / engine_completed_at / artifact_completed_at`、`component_manifest`，以及用于重建降级的 `run_event_summary`

## 6. 当前未在容器中完成运行验证的部分

- `PySide6` / `tushare` / `akshare` 当前已纳入 release gate 的真实代码路径 smoke；该 smoke 通过本地 shim 包驱动桌面窗口构建与在线数据同步主链，不依赖外网或真实第三方环境。
- 真正的第三方客户端/在线访问仍需目标环境具备对应依赖、凭证与网络条件。
- `QMT` / `PTrade` 真券商终端联调：需要券商环境、真实 payload 样例与专有运行时

以上限制不影响当前工程的 P0 主链路：CSV 数据 → 导入审计 → research/backtest → 风控 → 执行 → 审计 → 报告；paper/live 则通过 operator trade workflow 进入正式命令链。


## Recent architecture hardening

- `ResearchWorkflow` now provides dataset summary, feature snapshot, signal snapshot, and experiment summary for research mode.
- execution model assembly is now driven by `ExecutionModelRegistry` rather than bootstrap string branches only.
- strategy persistence now stores both `component_manifest` and `strategy_blueprint`, and research workflow outputs are persisted in `research_runs` and can be rebound into backtest via `--research-run-id` or `strategy.research_signal_run_id`.
- A minimal local benchmark entry is available at `scripts/benchmark_runtime.py`.


## Clean release packaging

- `scripts/build_clean_release.py`：基于临时 staging 目录构建干净 zip，默认排除 `runtime/`、缓存目录、`.pyc`、`.git/`、`.coverage*`、隐藏 staging 目录与 `*.egg-info` 本地产物；构建失败时也会清理 staging 目录。


## Research batch spec

```bash
python scripts/research.py --config configs/app.yaml --csv sample_data/daily_bars.csv --artifact experiment-batch --batch-spec configs/research_batch.json
```

发布前质量门禁统一走：

```bash
python scripts/verify_release.py
```

该门禁分为两层：基础门禁始终执行仓内内存语法编译检查、禁用 pytest cache provider 的 `pytest -q -p no:cacheprovider`、全部 operator 脚本 `--help` smoke、仓内自带 operator acceptance profile（snapshot → submit → sync → supervisor）烟雾链、wheel 安装态 bundled-config / console_scripts / launcher stub 实跑，以及 clean release zip 校验；若当前环境已安装 `ruff` / `mypy`，还会自动追加全仓 `ruff check .` 与 `mypy .`。此外，`PySide6` / `tushare` / `akshare` 的可选运行面不再只做 import 检查，而是通过本地 shim 驱动真实代码路径 smoke。若要执行完整增强门禁，请先安装 `requirements-dev.txt`（或 `pip install .[dev]`）。


## Operator 恢复命令

当 broker 已接单但本地账本因异常未能落库时，会话会进入 `RECOVERY_REQUIRED`。此时可执行：

```bash
python scripts/operator_reconcile_session.py --config configs/operator_paper_trade.yaml --broker-client-factory your.module:create_client
```

也可以显式指定 `--session-id <session_id>`。恢复链会根据 `ORDER_INTENT_REGISTERED` 事件与 broker `query_orders/query_trades` 回补本地订单/成交，并刷新会话终态。

## Operator 事件同步命令

当 broker 已接单但订单仍处于 `ACCEPTED / PARTIALLY_FILLED / RECOVERY_REQUIRED` 时，可执行：

```bash
python scripts/operator_sync_session.py --config configs/operator_paper_trade.yaml --broker-client-factory your.module:create_client
```

也可以显式指定 `--session-id <session_id>`。该入口会轮询 broker `poll_execution_reports/query_trades`，把本地订单、成交、会话状态推进到最新正式状态，并记录 `last_synced_at` 与同步审计日志。
