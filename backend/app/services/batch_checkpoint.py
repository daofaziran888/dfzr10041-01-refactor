"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class BatchCheckpointService(BaseModuleService):
    MODULE = "batch_checkpoint"
    ENTITY = "检查点记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["记录编号", "采集作业", "文件路径", "偏移位置", "已处理行", "校验哈希", "检查时间", "记录状态"]
    REQUIRED_FIELDS = ["记录编号", "采集作业", "文件路径"]
    STATUS_ORDER = ["已记录", "校验通过", "偏移异常", "已确认"]
    ACTION_RULES = {"触发校验": "校验通过", "标记异常": "偏移异常", "确认通过": "已确认"}
    NEGATIVE_ACTIONS = []
