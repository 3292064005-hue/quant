# Implementation Summary

当前交付版本：`0.5.6`

## 发布说明

本文件用于发布元数据同步与安装态构建校验，不承载实现正确性的唯一结论。
以最终审计报告、测试结果与源码为准。

## 当前版本要点

- UI 桌面面板正式读取版本化 `ui_*` projection；原始 snapshot 仅保留兼容层。
- research 的 `dataset / feature / signal` 查询默认不写 `research_runs`，显式 `--record` 或 `research.record_query_runs=true` 才持久化。
- operator 会话事件与 runtime event 读取口优先统一到 `runtime_events`。
- 发布包应保持洁净，不包含运行时输出或临时日志文件。
