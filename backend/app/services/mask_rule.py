"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class MaskRuleService(BaseModuleService):
    MODULE = "mask_rule"
    ENTITY = "脱敏配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "目标字段", "脱敏方式", "掩码模式", "保留位数", "适用来源", "脱敏顺序", "规则状态"]
    REQUIRED_FIELDS = ["规则编号", "目标字段", "脱敏方式"]
    STATUS_ORDER = ["草稿", "生效中", "遗漏告警", "已停用"]
    ACTION_RULES = {"启效规则": "生效中", "标记遗漏": "遗漏告警", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]
