"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class SchemaVersionService(BaseModuleService):
    MODULE = "schema_version"
    ENTITY = "版本记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["版本编号", "日志来源", "字段变更", "新增字段", "删除字段", "兼容状态", "影响事件数", "版本时间"]
    REQUIRED_FIELDS = ["版本编号", "日志来源", "字段变更"]
    STATUS_ORDER = ["已记录", "兼容", "不兼容", "已确认"]
    ACTION_RULES = {"确认兼容": "兼容", "标记不兼容": "不兼容", "废弃版本": "已确认"}
    NEGATIVE_ACTIONS = []
