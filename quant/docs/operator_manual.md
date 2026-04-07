# 操作手册

## 1. 初始化

```bash
python scripts/init_db.py --config configs/app.yaml
```

默认示例配置使用 `app.path_resolution_mode=config_dir`，并通过 `../runtime/...` 明确把产物落在项目根目录的 `runtime/` 下。

## 2. 导入 CSV 数据

```bash
python scripts/sync_market_data.py \
  --config configs/app.yaml \
  --provider csv \
  --csv sample_data/daily_bars.csv
```

输出中会包含：

- `import_run_id`
- `degradation_flags`
- `warnings`

若导入失败，可按 `import_run_id` 查询 `data_import_runs / data_import_quality_events`。

## 3. 同步在线数据

### Tushare

```bash
python scripts/sync_market_data.py \
  --config configs/app.yaml \
  --provider tushare \
  --start-date 20260101 \
  --end-date 20260131
```

### AKShare

```bash
python scripts/sync_market_data.py \
  --config configs/app.yaml \
  --provider akshare \
  --start-date 20260101 \
  --end-date 20260131
```

注意：在线数据同步若出现 `degradation_flags / warnings`，表示系统发生了“降级成功”而不是“完全成功”。可通过日志和 `data_import_quality_events` 排查具体退化点。

## 4. 运行默认回测

前置约束：`daily_run / a-share-quant` 当前只支持 `app.runtime_mode=research_backtest` 且 `broker.provider=mock`。若配置为 `qmt/ptrade`，命令会在 CLI 层直接失败并提示这是未来 paper/live orchestration 边界。


```bash
python scripts/daily_run.py --config configs/app.yaml --csv sample_data/daily_bars.csv
```

或直接基于数据库已有数据运行：

```bash
python scripts/daily_run.py --config configs/app.yaml --skip-import
```

## 5. 重建回测报告

重建最近一次已完成运行的报告：

```bash
python scripts/generate_report.py --config configs/app.yaml
```

按 run_id 重建：

```bash
python scripts/generate_report.py --config configs/app.yaml --run-id run_xxxxxxxxxxxxxxxx
```

## 6. 查看产物

回测报告中当前会包含：

- 策略净值曲线
- 若 benchmark 可用，则包含基准曲线与超额指标
- 数据谱系（导入批次、数据源、时间窗、dataset_digest、degradation flags、warnings）
- 运行清单（entrypoint、strategy_version、runtime_mode、report_paths）；首次 `daily_run` 产出的报告文件即包含完整 `report_paths`


### 报表
默认目录：

```text
runtime/reports/
```

文件示例：

```text
momentum_top_n_run_xxxxxxxxxxxxxxxx_backtest.json
momentum_top_n_backtest.json
```

### 数据库
默认路径：

```text
runtime/a_share_quant.db
```

可重点查看：

- `backtest_runs`
- `orders`
- `fills`
- `account_snapshots`
- `position_snapshots`
- `audit_logs`
- `data_import_runs`
- `data_import_quality_events`
- `schema_version`

## 7. 测试

```bash
pytest -q
```

测试会使用临时数据库和临时报表目录，不会污染手工运行环境。

## 8. 非 mock 券商模式

注意：真实 broker 不再被研究回测主链隐式接入。它们当前的正式用途是：

- `check_runtime` 契约校验
- 未来独立的 paper/live operator workflow 边界


若将 `broker.provider` 切到 `qmt` 或 `ptrade`：

- 必须在配置中提供 `endpoint` 与 `account_id`
- 必须提供真实客户端对象：可以直接通过 `bootstrap(..., broker_clients={...})` 注入，也可以通过 `broker.client_factory` / `--broker-client-factory` 让 CLI 动态构造
- 当前工程不会凭空构造券商运行时；client factory 需要由使用方提供
- `init_db` / `sync_market_data` / `generate_report` 这类非 broker 命令不会再因为真实券商配置而被阻断
- 退出应用或脚本时，会显式调用 broker `close()` 释放资源
- `broker.strict_contract_mapping=true` 时，返回载荷必须能严格映射成领域对象
- `broker.strict_contract_mapping=false` 时，适配器进入 best-effort 兼容模式，并将降级行为写入 warning 日志
- `check_runtime --broker-sample-payload-file` 会与 `broker.strict_contract_mapping` 保持一致：严格模式走严格映射，兼容模式走 best-effort 校验
- 若只是研究回测，请保持 `app.runtime_mode=research_backtest` 与 `broker.provider=mock`；`daily_run / a-share-quant` 已不再暴露 `--broker-client-factory` 参数
- `broker.client_factory` 约定格式为 `package.module:callable`；工厂可返回当前 provider 的 client，或返回 provider->client 映射
- `check_runtime` 现在会同时校验 `app.runtime_mode` 与 `broker.provider` 的组合是否合法，避免出现“runtime 检查通过、主链实际无法启动”的误绿灯
- 外部策略若需要自定义构造参数，请使用 `strategy.params`；兼容字段 `lookback/top_n/holding_days` 仍可继续使用

## 9. 关键配置建议

- `app.path_resolution_mode=config_dir`：推荐默认值，避免 cwd 敏感
- `app.path_resolution_mode=cwd`：只在需要兼容旧脚本行为时使用
- `backtest.data_access_mode=preload`：适合样例数据与小规模研究
- `backtest.data_access_mode=stream`：适合更大样本，降低一次性内存占用
- `backtest.valuation.missing_price_policy=last_known`：默认推荐
- `backtest.valuation.missing_price_policy=reject`：适合对估值完整性要求更高的研究环境

## 10. 运行前健康检查

在切换到 `tushare` / `akshare` / `qmt` / `ptrade` 或准备启动桌面 UI 前，先执行：

```bash
python scripts/check_runtime.py --config configs/app.yaml --check-ui --strict
```

该脚本会显式检查：

- 当前配置对应的数据源依赖是否安装
- `tushare` token 是否具备
- `qmt` / `ptrade` 是否已配置 endpoint / account_id
- 可选 JSON 样本 payload 是否能映射成领域对象（`--broker-sample-payload-file`）
- PySide6 UI 运行依赖是否安装

注意：CLI 运行前检查默认是**shallow mode**，用于验证基础配置与可选样本载荷；不会在命令行里凭空伪造真实券商客户端。若提供 `broker.client_factory` 或 `--broker-client-factory`，则会进一步实例化真实客户端并检查方法契约。

桌面原型 UI 建议通过以下入口启动：

```bash
python scripts/launch_ui.py --config configs/app.yaml
```
