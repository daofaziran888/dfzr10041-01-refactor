"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class TenantIsolationService(BaseModuleService):
    MODULE = "tenant_isolation"
    ENTITY = "租户配置"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["租户编号", "租户名称", "日志来源", "存储分区", "保留策略", "数据配额", "当前用量", "配置状态"]
    REQUIRED_FIELDS = ["租户编号", "租户名称", "日志来源"]
    STATUS_ORDER = ["待配置", "已生效", "配额告警", "已停用"]
    ACTION_RULES = {"启效配置": "已生效", "调整配额": "配额告警", "停用租户": "已停用"}
    NEGATIVE_ACTIONS = ["停用租户"]
