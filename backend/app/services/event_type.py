"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class EventTypeService(BaseModuleService):
    MODULE = "event_type"
    ENTITY = "事件类型"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["类型编号", "类型名称", "分类标签", "严重级别", "告警阈值", "采样比例", "保留天数", "类型状态"]
    REQUIRED_FIELDS = ["类型编号", "类型名称", "分类标签"]
    STATUS_ORDER = ["草稿", "已上线", "分级调整", "已下线"]
    ACTION_RULES = {"上线类型": "已上线", "调整分级": "分级调整", "下线类型": "已下线"}
    NEGATIVE_ACTIONS = ["下线类型"]
