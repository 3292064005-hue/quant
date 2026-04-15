# Operator Manual (Read-Only Operator Plane)

## Scope

Current desktop UI is a **read-only operator plane** backed by a unified projection/query layer rather than ad-hoc snapshot assembly. It now consumes versioned `ui_*` projections instead of reading raw registry/result payloads directly.
The runtime layer now has dedicated assemblies for `research_backtest / paper_trade / live_trade`, and paper/live already expose a formal operator trade workflow on the CLI.
The desktop UI itself still does not place orders, cancel orders, or start interactive broker sessions.

## Tabs

- 边界说明：shows supported scope and explicit non-goals
- 配置摘要：shows runtime, broker, database, and execution-model configuration
- 运行时健康：shows `config_ok / boundary_ok / client_contract_ok / operable_ok`
- 策略生命周期：shows available workflows and the latest backtest run
- 订单执行：shows latest run, order status counts, recent orders, and recent fills
- 风险告警：shows import quality events plus latest risk/execution audit logs
- 导入审计：shows import-run level audit information
- 报告与回放：shows report artifacts, event summary, lineage graph, plugins, providers, and recent research context

## Runtime notes

- UI depends on `PySide6`
- UI reads the same runtime checks used by CLI `check_runtime`
- UI is intentionally read-only; workflow triggering remains CLI-driven in this release
- UI launcher is officially scoped to `research_backtest + mock`; paper/live read-only inspection stays on `operator_snapshot`，写路径恢复与持续事件推进则通过 `operator_sync_session` / `operator_run_supervisor` 完成


## Read-only operator snapshot

For `paper_trade` / `live_trade`, use `scripts/operator_snapshot.py` as the formal read-only operator entry. The CLI now defaults to the repo-owned acceptance profile `configs/operator_paper_trade_demo.yaml`, so a clean checkout can run operator snapshot without falling back into `research_backtest + mock`. The entry still routes through `RunQueryService.build_operator_snapshot(...)`, returns runtime checks, latest run summaries, recent runtime events, and an `account_views` array so one snapshot can expose the default account plus every configured/allowed account scope. Each account view can include realtime broker state as well as persisted `persisted_account` / `persisted_positions` snapshots captured during submit/sync/reconcile flows.

For write-path execution, use `scripts/operator_submit_order.py` (or `a-share-quant-operator-submit-order`) for manual orders, and `scripts/operator_submit_signal.py` (or `a-share-quant-operator-submit-signal`) when promoting a recorded research `signal_snapshot` into the formal operator lane. Both commands run the same CLI-level preflight before bootstrap: wrong runtime lane / mock broker / missing broker client factory are converted into clean `SystemExit` messages instead of raw traceback. The shared path creates a formal trade session, runs unified pre-trade validation through the RiskEngine, persists formal order entities plus command intent events before broker side effects, submits accepted orders through the configured broker adapter, and records ticket / execution-report / fill linkage for replay and audit. The signal path first resolves `ExecutionIntent -> PortfolioDelta -> OrderRequest`, then reuses the same operator orchestrator rather than bypassing it. To reduce operator-side ambiguity, `operator_submit_signal` now requires an explicit `research_run_id`; it no longer falls back to “the latest signal snapshot”. Internal `order_id` uniqueness is enforced at both the service and repository boundaries: blank / duplicate / historical-conflict IDs are reissued before submission, and conflicting reuse of an existing `order_id` is rejected at persistence time. Pre-trade rejected orders are also persisted as formal orders with status `PRE_TRADE_REJECTED`, so session replay and audit no longer depend on event-only reconstruction. If broker side effects happen but local persistence fails, the session is marked `RECOVERY_REQUIRED` and can be repaired through `scripts/operator_reconcile_session.py` / `a-share-quant-operator-reconcile-session`.

## Event sync path

For sessions that remain open after broker acceptance, use `scripts/operator_sync_session.py` (or `a-share-quant-operator-sync-session`) to perform a single poll-based sync against `poll_execution_reports/query_trades` and advance the local ledger. The sync path preserves `account_id` scope, records `last_synced_at`, and is intended for manual recovery or read-mostly supervision rather than interactive fill editing.

For continuous supervision, use `scripts/operator_run_supervisor.py` (or `a-share-quant-operator-run-supervisor`). The sync and supervisor console scripts are now included in the packaged wheel, not just the source tree scripts. That path now claims open sessions with compare-and-swap style lease semantics, records `supervisor_owner / supervisor_lease_expires_at / supervisor_mode / last_supervised_at`, emits `SUPERVISOR_RENEWED` heartbeats while a session remains under supervision, and writes `SUPERVISOR_RELEASED` only when the release actually succeeds; otherwise it records `SUPERVISOR_RELEASE_SKIPPED`. It prefers broker `subscribe_execution_reports(...)` when the configured adapter supports it, and falls back to the same poll sync path used by `operator_sync_session` when subscription is unavailable or interrupted. Broker event source selection is controlled by `broker.event_source_mode = auto | poll | subscribe`.

## Daily run semantics

`scripts/daily_run.py` now defaults to running against the existing database snapshot. To import CSV and run in one command, pass `--import-csv sample_data/daily_bars.csv` (legacy alias `--csv` is still accepted).
