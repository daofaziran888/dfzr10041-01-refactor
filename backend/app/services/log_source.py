"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class LogSourceService(BaseModuleService):
    MODULE = "log_source"
    ENTITY = "日志来源"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["源编号", "源名称", "应用标识", "日志路径", "日志格式", "字符编码", "采集方式", "接入状态"]
    REQUIRED_FIELDS = ["源编号", "源名称", "应用标识"]
    STATUS_ORDER = ["待验证", "已接入", "格式异常", "已停采"]
    ACTION_RULES = {"验证来源": "已接入", "标记异常": "格式异常", "停采来源": "已停采"}
    NEGATIVE_ACTIONS = []
