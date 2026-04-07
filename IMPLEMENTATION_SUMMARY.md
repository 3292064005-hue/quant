# 实施摘要（问题逐项修复版）

## 本轮实施主线

### P0
- 建立统一的外部调用 timeout 契约
  - 新增 `a_share_quant/core/timeout_utils.py`
  - 新增 `ExternalServiceTimeoutError`
  - `TushareDataAdapter` / `AKShareDataAdapter` 接入 `request_timeout_seconds`
  - `QMTAdapter` / `PTradeAdapter` 接入 `operation_timeout_seconds`
  - timeout 异常语义不再被适配器错误吞没为通用 `DataSourceError`
- 取消 `backtest_runs` 成功状态双写
  - `BacktestEngine.run()` 不再写成功完成状态
  - 成功收尾统一由 `BacktestService.run()` 负责
- 修复日志目录硬编码
  - 新增 `app.logs_dir`
  - `bootstrap()` 改为按配置初始化日志目录
- 补齐执行层缺行情边界
  - `ExecutionEngine.execute()` 遇到缺失 bar 时改为结构化拒单，而不是抛 `KeyError`

### P1
- 补充 timeout / broker 合同 / 执行缺行情 的测试覆盖
- 保持 `run_id` 运行隔离、风控-执行闭环、资源释放链持续有效
- 脚本入口继续显式关闭 `AppContext`

### P2
- README 同步新增日志目录与 timeout 配置说明
- 通过 broker 适配器合同测试收紧 QMT / PTrade 的代码侧边界

## 实际改动文件

### 直接实现文件
- `a_share_quant/core/exceptions.py`
- `a_share_quant/core/timeout_utils.py`
- `a_share_quant/config/models.py`
- `a_share_quant/app/bootstrap.py`
- `a_share_quant/services/data_service.py`
- `a_share_quant/adapters/data/tushare_adapter.py`
- `a_share_quant/adapters/data/akshare_adapter.py`
- `a_share_quant/adapters/broker/qmt_adapter.py`
- `a_share_quant/adapters/broker/ptrade_adapter.py`
- `a_share_quant/engines/backtest_engine.py`
- `a_share_quant/engines/execution_engine.py`
- `configs/app.yaml`
- `configs/data.yaml`
- `configs/broker/qmt.yaml`
- `configs/broker/ptrade.yaml`
- `README.md`

### 新增测试文件
- `tests/unit/test_timeout_contracts.py`
- `tests/unit/test_backtest_service.py`
- `tests/unit/test_execution_engine.py`
- `tests/unit/test_broker_adapters.py`

## 为保证兼容性所做处理
- timeout 配置均提供默认值，未显式配置时保持原工程可运行
- timeout 仅包裹外部边界调用，不改动内部领域对象结构
- `BacktestService` 仍保持原调用签名；只是把成功状态收口为单点写入
- `ExecutionEngine` 对“缺行情”从抛异常改为拒单，不影响正常有行情主路径
- `app.logs_dir` 新增为向后兼容字段，旧配置未填写时默认仍写入 `runtime/logs`

## 新增或调整的配置 / 注释 / 异常边界
- `app.logs_dir`
- `data.request_timeout_seconds`
- `broker.operation_timeout_seconds`
- `ExternalServiceTimeoutError`
- timeout 工具函数文档明确为 **best-effort** 时限控制
- `ExecutionEngine.execute()` 文档更新为“不因单笔缺行情中断整批执行”

## 本地验证结果
- `pytest -q`：25 项测试通过
- `python scripts/init_db.py --config configs/app.yaml`：通过
- `python scripts/sync_market_data.py --config configs/app.yaml --provider csv --csv sample_data/daily_bars.csv`：通过
- `python scripts/daily_run.py --config configs/app.yaml --csv sample_data/daily_bars.csv`：通过
- `python scripts/generate_report.py --config configs/app.yaml`：通过

## 仍需明确但未伪装成已完成的边界
- `PySide6` 在当前容器中仍未安装，因此仅完成 UI 源码级检查，未做窗口运行验证
- `QMT / PTrade` 真券商终端联调仍需要目标券商环境；本轮新增的是代码侧 timeout 与合同测试，不是伪装成真实联调
- timeout 为跨依赖的 best-effort 实现；对不支持原生取消的第三方阻塞调用，无法承诺底层线程立即终止
