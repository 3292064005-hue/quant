# ADR-0002 Config and Release Source of Truth

## 状态
Accepted

## 背景
仓库存在 `configs/` 与 `a_share_quant/resources/configs/` 双份配置，以及 README / architecture / implementation summary 的版本号人工同步问题，发布门禁容易失配。

## 决策
- `configs/` 作为配置唯一真相源
- `a_share_quant/resources/configs/` 视为 package data 投影，由 `a_share_quant.resources.config_sync.sync_packaged_configs()` 同步生成
- release 版本号以 `a_share_quant.__version__` 为单一真相源
- 文档版本文本由 `scripts/sync_release_metadata.py` 统一同步/校验

## 后果
- 构建与发布前必须运行 config/release 同步脚本
- 测试不再接受人工维护双份配置的漂移
- 文档版本号更新不再依赖手工多点编辑
