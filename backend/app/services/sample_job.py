"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class SampleJobService(BaseModuleService):
    MODULE = "sample_job"
    ENTITY = "采样计划"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["计划编号", "事件类型", "采样比例", "归并键", "归并窗口", "保存策略", "最近采样", "计划状态"]
    REQUIRED_FIELDS = ["计划编号", "事件类型", "采样比例"]
    STATUS_ORDER = ["待执行", "采样中", "已完成", "策略调整"]
    ACTION_RULES = {"启动采样": "采样中", "调整策略": "策略调整", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = []
