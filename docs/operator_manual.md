# 操作手册

## 1. 初始化

```bash
python scripts/init_db.py --config configs/app.yaml
```

## 2. 导入 CSV 数据

```bash
python scripts/sync_market_data.py \
  --config configs/app.yaml \
  --provider csv \
  --csv sample_data/daily_bars.csv
```

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

## 4. 运行默认回测

```bash
python scripts/daily_run.py --config configs/app.yaml --csv sample_data/daily_bars.csv
```

或直接基于数据库已有数据运行：

```bash
python scripts/daily_run.py --config configs/app.yaml --skip-import
```

## 5. 查看产物

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

## 6. 测试

```bash
pytest -q
```

测试会使用临时数据库和临时报表目录，不会污染手工运行环境。

## 7. 非 mock 券商模式

若将 `broker.provider` 切到 `qmt` 或 `ptrade`：

- 必须在配置中提供 `endpoint` 与 `account_id`
- 必须在 `bootstrap(..., broker_clients={...})` 中注入真实客户端对象
- 当前工程不会凭空构造券商运行时
