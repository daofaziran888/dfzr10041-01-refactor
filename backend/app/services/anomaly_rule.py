"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class AnomalyRuleService(BaseModuleService):
    MODULE = "anomaly_rule"
    ENTITY = "告警规则"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["规则编号", "事件类型", "检测条件", "告警级别", "触发频次", "聚合窗口", "通知渠道", "规则状态"]
    REQUIRED_FIELDS = ["规则编号", "事件类型", "检测条件"]
    STATUS_ORDER = ["待启用", "生效中", "已触发", "已关闭"]
    ACTION_RULES = {"启用规则": "生效中", "临时关闭": "已触发", "恢复规则": "已关闭"}
    NEGATIVE_ACTIONS = []
