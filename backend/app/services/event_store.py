"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class EventStoreService(BaseModuleService):
    MODULE = "event_store"
    ENTITY = "事件记录"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["记录编号", "事件类型", "原始内容", "解析字段", "发生时间", "来源主机", "存储分区", "索引状态"]
    REQUIRED_FIELDS = ["记录编号", "事件类型", "原始内容"]
    STATUS_ORDER = ["已写入", "已索引", "索引失效", "已归档"]
    ACTION_RULES = {"重建索引": "已索引", "标记失效": "索引失效", "归档记录": "已归档"}
    NEGATIVE_ACTIONS = []
