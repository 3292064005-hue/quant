"""UI 边界说明面板。"""
from __future__ import annotations

from a_share_quant.ui.panels.common import build_page, build_text_group


def build_boundary_panel() -> object:
    """构建边界说明面板。"""
    delivered = (
        "当前版本已交付三条正式能力线：\n"
        "1. research_backtest + mock 的完整研究/回测/报告闭环\n"
        "2. paper_trade / live_trade 的 operator trade workflow（CLI 写路径）\n"
        "3. 桌面 UI 的只读 operator projection 面板\n\n"
        "桌面窗口仍不直接承载写操作；正式写路径通过 operator workflow/CLI 执行，"
        "以保持命令链、风控链、审计链和回放链一致。"
    )
    boundaries = (
        "当前仍未在桌面 UI 内交付的能力：\n"
        "- 撤单/改单/批量审批等交互式命令面板\n"
        "- 断线恢复与 broker 异常补偿的人工处理台\n"
        "- 多账户/多 broker supervisor 视图\n\n"
        "当前交互原则：\n"
        "- UI 只消费 projection/read model\n"
        "- 真正写操作必须经过 operator workflow + risk gate + audit"
    )
    return build_page(
        "边界说明",
        [
            build_text_group("已交付范围", delivered),
            build_text_group("当前边界", boundaries),
        ],
    )
