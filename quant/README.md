# A 股量化研究与交易工作站

本工程当前定位为 **CSV / mock / headless 的 research backtest 工作站底座**，聚焦三条主链闭环：

- 数据采集、本地治理与导入审计
- 策略回测 / 风控 / 模拟执行 / 审计
- 报表、数据谱系与运行留痕

本轮落地的主改造不是表层重构，而是把几个会影响企业级稳定性的关键边界真正收口：

- 市场数据导入改为**显式原子事务**，避免 `securities / trading_calendar / bars_daily` 撕裂写入
- 新增 `data_import_runs / data_import_quality_events`，导入质量事件不再只打日志
- QMT / PTrade 适配器改为**领域对象归一化边界**，不再把第三方原始 payload 直接透传到引擎层
- `check_broker_runtime()` 从“只看方法存在”升级为“方法签名 + 可选样本 payload 映射校验”
- `BacktestEngine` 内部拆分为生命周期管理、交易日推进、调仓规划、执行协调、日终持久化五段职责
- 预加载回测不再重复构造 `bars_by_date` 大映射，降低 preload 模式额外内存放大
- SQLite 从“运行时轻量补丁”升级为**版本化 schema migration**，并补齐唯一索引与导入审计表
- 运行时路径默认改为**相对配置文件目录解析**，消除 cwd 敏感；示例配置同步显式写为 `../runtime/...`
- CLI 入口统一下沉到 `a_share_quant.cli`，同时保留 `scripts/` 兼容薄壳

## 1. 适用范围

- 研究环境：支持 CSV、AKShare、Tushare 数据适配器
- 运行模式：当前正式支持 `app.runtime_mode=research_backtest`
- 执行环境：research backtest 仅允许 `broker.provider=mock`；QMT/PTrade 当前保留为 runtime 校验与未来 paper/live orchestration 边界，不再被 `daily_run / a-share-quant` 隐式接入
- 产品形态：headless 主链已闭环；桌面端当前明确降级为原型展示层，仅提供配置与运行时状态展示，不再伪装为已接交易主链的工作台

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

导入结果会额外输出 `import_run_id`，可用于排查导入审计。

### 3.4 运行回测

```bash
python scripts/daily_run.py --config configs/app.yaml
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

检查当前配置对应的数据源 / broker / 可选 UI 运行时是否具备：

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

### 3.7 启动桌面原型 UI

```bash
python scripts/launch_ui.py --config configs/app.yaml
```

当前入口只启动**桌面原型层**：展示配置摘要、运行时检查结果与桌面边界说明；不会再隐式构建 broker，也不会伪装成可直接执行回测/下单的完整工作台。

若当前环境未安装 `PySide6`，脚本会在启动前明确报错并给出安装提示，而不是在业务链路中途失败。

### 3.8 运行测试

```bash
pytest -q
```

当前仓内自动化校验覆盖：配置、broker 契约、事务回滚、回放留痕、脚本入口、路径解析与启动异常收口。

## 4. 当前关键配置

- `app.path_resolution_mode`
  - `config_dir`：相对配置文件目录解析，默认推荐
  - `cwd`：相对当前工作目录解析，仅用于兼容旧行为
- `backtest.data_access_mode`
  - `preload`：一次性加载行情后回测
  - `stream`：以交易日为主轴流式读取行情
- `backtest.valuation.missing_price_policy`
  - `last_known`：优先使用最近一次有效价
  - `avg_cost`：缺失时退回持仓成本
  - `reject`：缺失时直接拒绝生成 EOD 快照
- `data.allow_degraded_data`
  - 在线数据源发生降级时是否允许继续写入本地库
- `data.fail_on_degraded_data`
  - 是否将在线数据降级提升为失败
- `app.runtime_mode`
  - `research_backtest`：当前正式支持的研究回测模式
  - `paper_trade` / `live_trade`：已作为架构边界显式保留，但本工程尚未提供正式 operator workflow
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
- `strategy.params`
  - 通用策略构造参数字典；优先级高于兼容字段 `lookback/top_n/holding_days`
  - 外部策略若需要额外参数，应通过该字段显式提供

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
- 版本化 SQLite schema migration
- 报告重建、文档、脚本、测试闭环
- 非 broker 命令按最小依赖装配，不再被真实券商配置阻断
- schema 资源内置进包，源码树与安装态的数据库初始化入口统一

### 仍属预留或实验性质的部分

- `ui/main_window.py`：当前已明确降级为桌面原型展示层，不是完整业务前端
- `core/events.py`：发布链可用，但未建设复杂订阅编排体系
- `benchmark_symbol`：当前已进入基准曲线与超额指标链；若基准行情不存在，则报告会显式只输出策略净值指标
- 回测运行已持久化 `import_run_id / data_source / data_start_date / data_end_date / dataset_digest / strategy_version / runtime_mode / report_artifacts`，并且首次写出的报告文件会与该 artifact 清单保持一致

## 6. 当前未在容器中完成运行验证的部分

- `PySide6` 桌面窗口运行验证：当前容器未安装 PySide6，只完成了源码与结构级校验；桌面层当前定位为原型展示层
- `tushare` / `akshare` 在线数据拉取：需要目标环境具备对应包与访问条件
- `QMT` / `PTrade` 真券商终端联调：需要券商环境、真实 payload 样例与专有运行时

以上限制不影响当前工程的 P0 主链路：CSV 数据 → 回测 → 风控 → 执行 → 审计 → 报告。
