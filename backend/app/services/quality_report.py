"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class QualityReportService(BaseModuleService):
    MODULE = "quality_report"
    ENTITY = "质量评估"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["报告编号", "日志来源", "完整率", "解析成功率", "字段覆盖率", "时效延迟", "评估周期", "报告状态"]
    REQUIRED_FIELDS = ["报告编号", "日志来源", "完整率"]
    STATUS_ORDER = ["待生成", "已生成", "需复核", "已发布"]
    ACTION_RULES = {"生成报告": "已生成", "提交复核": "需复核", "发布报告": "已发布"}
    NEGATIVE_ACTIONS = []
