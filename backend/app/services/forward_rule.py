"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ForwardRuleService(BaseModuleService):
    MODULE = "forward_rule"
    ENTITY = "转发规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "事件类型", "目标地址", "转发格式", "批量大小", "重试上限", "超时秒数", "规则状态"]
    REQUIRED_FIELDS = ["规则编号", "事件类型", "目标地址"]
    STATUS_ORDER = ["草稿", "生效中", "转发异常", "已停用"]
    ACTION_RULES = {"启效规则": "生效中", "标记异常": "转发异常", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]
