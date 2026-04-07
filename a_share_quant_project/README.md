# A 股量化研究与交易工作站

本工程是按既定方案从零实现的 **A 股量化研究 + 风控 + 执行桥接** 桌面工作站。它优先保证以下闭环：

- 数据采集与本地治理
- 策略研究与事件驱动回测
- 风控拦截与订单生成
- MockBroker 执行与审计留痕
- QMT / PTrade 适配边界预留

## 1. 适用范围

- 研究环境：支持 CSV、AKShare、Tushare 数据适配器
- 执行环境：默认使用 MockBroker；QMT/PTrade 通过适配层接入
- 产品形态：桌面端优先，当前容器中由于缺少 PySide6，仅提供可编译的 UI 源码与可运行的 headless 入口

## 2. 目录说明

```text
configs/                 配置文件
sample_data/             示例行情数据
docs/                    架构与操作文档
scripts/                 初始化、同步、回测、报表脚本
a_share_quant/           主代码包
tests/                   单元/集成/回放测试
```

## 3. 快速开始

### 3.1 安装依赖

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3.2 初始化数据库

```bash
python scripts/init_db.py --config configs/app.yaml
```

### 3.3 导入示例行情

```bash
python scripts/sync_market_data.py --config configs/app.yaml --csv sample_data/daily_bars.csv
```

### 3.4 运行回测

```bash
python scripts/daily_run.py --config configs/app.yaml
```

### 3.5 生成报告

```bash
python scripts/generate_report.py --config configs/app.yaml
```

### 3.6 运行测试

```bash
pytest -q
```

## 4. 架构边界

### 已落实
- 统一配置加载
- 事件驱动回测主线
- A 股 100 股整数手约束
- ST / 停牌 / 涨跌停 / 黑名单 / 熔断风控
- 订单、成交、持仓、账户、审计入库
- MockBroker 完整可运行实现
- QMT / PTrade 适配器接口层
- 文档、脚本、测试闭环

### 未在当前容器中完成运行验证的部分
- `PySide6` 桌面窗口运行验证：当前容器未安装 PySide6，只完成了源码与语法级校验
- `tushare` / `akshare` 在线数据拉取：需要目标环境具备对应包与访问条件
- `QMT` / `PTrade` 真券商终端联调：需要券商环境和专有运行时

以上限制不影响当前工程的 P0 主链路：CSV 数据 → 回测 → 风控 → 执行 → 审计 → 报告。

## 5. 当前关键配置

- `app.logs_dir`：日志输出目录
- `data.request_timeout_seconds`：在线数据源 best-effort 超时
- `broker.operation_timeout_seconds`：券商适配器 best-effort 超时

注意：timeout 采用线程等待包装，对不支持原生取消的第三方客户端属于 best-effort 时限控制，超时后会尽快向上抛错，但无法保证底层阻塞调用被立即终止。
