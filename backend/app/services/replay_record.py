"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ReplayRecordService(BaseModuleService):
    MODULE = "replay_record"
    ENTITY = "重放任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "来源事件", "重放范围", "重放目标", "重放速度", "重试次数", "当前进度", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "来源事件", "重放范围"]
    STATUS_ORDER = ["待重放", "重放中", "已完成", "重放失败"]
    ACTION_RULES = {"启动重放": "重放中", "暂停重放": "已完成", "标记失败": "重放失败"}
    NEGATIVE_ACTIONS = []
