"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class DedupFilterService(BaseModuleService):
    MODULE = "dedup_filter"
    ENTITY = "去重规则"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["规则编号", "事件类型", "去重字段", "去重窗口", "指纹算法", "命中次数", "忽略次数", "规则状态"]
    REQUIRED_FIELDS = ["规则编号", "事件类型", "去重字段"]
    STATUS_ORDER = ["草稿", "生效中", "规则失效", "已停用"]
    ACTION_RULES = {"启效规则": "生效中", "标记失效": "规则失效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]
