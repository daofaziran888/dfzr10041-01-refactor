"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ArchivePolicyService(BaseModuleService):
    MODULE = "archive_policy"
    ENTITY = "归档方案"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["方案编号", "事件类型", "保留天数", "归档格式", "压缩方式", "归档路径", "最后归档", "方案状态"]
    REQUIRED_FIELDS = ["方案编号", "事件类型", "保留天数"]
    STATUS_ORDER = ["待触发", "归档中", "已完成", "归档异常"]
    ACTION_RULES = {"触发归档": "归档中", "暂停归档": "已完成", "标记异常": "归档异常"}
    NEGATIVE_ACTIONS = []
