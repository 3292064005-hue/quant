# 实施摘要

## 本轮版本

- 版本：0.2.1
- 定位：CSV / mock / headless 的 research backtest 工作站底座

## 本轮落地的主线

### P0
- 引入 `app.runtime_mode`，明确 `research_backtest / paper_trade / live_trade` 边界
- `BacktestEngine` 固定为 research_backtest 主链，不再隐式复用真实 broker 语义
- research_backtest 模式强制 `broker.provider=mock`；真实 broker 收口到 runtime 校验与未来独立 orchestration
- `backtest_runs` 写入数据谱系：`import_run_id / data_source / data_start_date / data_end_date / dataset_digest / degradation_flags / warnings`

### P1
- `benchmark_symbol` 从元数据提升为基准曲线 + 相对指标链（若基准行情可用）
- `StrategyService` 升级为 registry / loader，支持 `strategy.class_path / strategy.params` 与策略版本持久化
- `StrategyRepository` 支持保存、读取、列出启用策略
- `ReportService` 写出并重建数据谱系、benchmark 曲线、运行产物清单

### P2
- `FactorEngine` 接入默认动量策略
- `ExecutionService` 接入主调用链
- `EventBus` 补入真实消费者，消除“只有发布没有消费”的状态
- 文档同步为 research-first 语义，清除真实 broker 被 `daily_run` 隐式支持的旧表述

## 新增/重点变更文件

- `a_share_quant/config/models.py`
- `a_share_quant/app/bootstrap.py`
- `a_share_quant/cli.py`
- `a_share_quant/domain/models.py`
- `a_share_quant/services/data_service.py`
- `a_share_quant/services/backtest_service.py`
- `a_share_quant/services/report_service.py`
- `a_share_quant/services/strategy_service.py`
- `a_share_quant/repositories/backtest_run_repository.py`
- `a_share_quant/repositories/data_import_repository.py`
- `a_share_quant/repositories/strategy_repository.py`
- `a_share_quant/engines/backtest_engine.py`
- `a_share_quant/core/metrics.py`
- `a_share_quant/storage/sqlite_store.py`
- `a_share_quant/schema.sql`

## 验证

- `pytest -q`：61 passed
- `python -m compileall -q a_share_quant tests`：通过
- 脚本烟测：
  - `python scripts/init_db.py --config configs/app.yaml`
  - `python scripts/check_runtime.py --config configs/app.yaml --strict`
  - `python scripts/sync_market_data.py --config configs/app.yaml --csv sample_data/daily_bars.csv`
  - `python scripts/daily_run.py --config configs/app.yaml --skip-import`
  - `python scripts/generate_report.py --config configs/app.yaml`
- wheel 构建：`python -m pip wheel . -w dist` 通过

## 未真实环境验证

- PySide6 真桌面交互
- Tushare / AKShare 在线拉取
- QMT / PTrade 真终端联调

## 本轮补充修正

- 修复失败导入批次被错误挂接到后续回测谱系的问题：仅引用最近一次 `COMPLETED` 导入运行
- 修复首次写出的报告文件 `artifacts.report_paths` 为空的问题：报告文件与 DB artifact 清单现已同源一致
- `check_runtime` 新增 `runtime_mode / broker.provider` 组合合法性校验，消除误绿灯
- 研究回测 CLI 去除 `--broker-client-factory` 噪声参数；真实 broker factory 保留在 `check_runtime` / 未来 paper-live 边界
- 策略 loader 增加通用 `strategy.params` 参数契约，外部策略不再受限于固定四个初始化参数
