"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class AuditEventService(BaseModuleService):
    MODULE = "audit_event"
    ENTITY = "审计记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "操作模块", "操作人", "操作类型", "操作详情", "操作时间", "来源IP", "审计结论"]
    REQUIRED_FIELDS = ["记录编号", "操作模块", "操作人"]
    STATUS_ORDER = ["已记录", "待复核", "已通过", "已驳回"]
    ACTION_RULES = {"提交复核": "待复核", "确认通过": "已通过", "驳回记录": "已驳回"}
    NEGATIVE_ACTIONS = ["驳回记录"]
