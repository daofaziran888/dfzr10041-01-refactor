"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class FieldEnrichService(BaseModuleService):
    MODULE = "field_enrich"
    ENTITY = "增强规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "目标字段", "增强来源", "映射字典", "默认值", "优先级", "适用事件", "规则状态"]
    REQUIRED_FIELDS = ["规则编号", "目标字段", "增强来源"]
    STATUS_ORDER = ["草稿", "生效中", "映射缺失", "已停用"]
    ACTION_RULES = {"启效规则": "生效中", "标记缺失": "映射缺失", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]
