"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ParserRuleService(BaseModuleService):
    MODULE = "parser_rule"
    ENTITY = "解析方案"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["方案编号", "日志来源", "正则表达式", "字段定义", "时间格式", "多行模式", "优先级", "方案状态"]
    REQUIRED_FIELDS = ["方案编号", "日志来源", "正则表达式"]
    STATUS_ORDER = ["草稿", "生效中", "解析异常", "已废弃"]
    ACTION_RULES = {"启效方案": "生效中", "标记异常": "解析异常", "废弃方案": "已废弃"}
    NEGATIVE_ACTIONS = []
