"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class IngestMetricService(BaseModuleService):
    MODULE = "ingest_metric"
    ENTITY = "指标记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["记录编号", "日志来源", "采集速率", "处理延迟", "错误率", "积压行数", "采样时间", "指标状态"]
    REQUIRED_FIELDS = ["记录编号", "日志来源", "采集速率"]
    STATUS_ORDER = ["正常", "延迟告警", "积压告警", "异常中断"]
    ACTION_RULES = {"确认告警": "延迟告警", "清空积压": "积压告警", "重置指标": "异常中断"}
    NEGATIVE_ACTIONS = []
