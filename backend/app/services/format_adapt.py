"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class FormatAdaptService(BaseModuleService):
    MODULE = "format_adapt"
    ENTITY = "适配器配置"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["适配器编号", "输入格式", "输出格式", "字段映射", "转换规则", "错误策略", "适配来源数", "适配状态"]
    REQUIRED_FIELDS = ["适配器编号", "输入格式", "输出格式"]
    STATUS_ORDER = ["草稿", "生效中", "转换异常", "已废弃"]
    ACTION_RULES = {"启效适配": "生效中", "标记异常": "转换异常", "废弃适配": "已废弃"}
    NEGATIVE_ACTIONS = []
