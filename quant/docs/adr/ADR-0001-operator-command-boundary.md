# ADR-0001 Operator Command Boundary

## 状态
Accepted

## 背景
operator submit / reconcile / sync / supervisor 先前并未全部经过统一 workflow 边界，导致 plugin hook、审计、命令生命周期和 CLI 行为不一致。

## 决策
- 正式 operator 命令统一经由 `workflow.operator_trade`
- CLI 不再直接调用 orchestrator / supervisor service 作为正式入口
- workflow 负责统一 before/after hook、异常透传与 payload 规范

## 后果
- 新增 operator 命令时必须先进入 workflow 层
- plugin / metrics / audit 的扩展点具备统一挂载位置
- 若未来接入 UI 写路径，也应调用 workflow/command facade，而不是直连 service
