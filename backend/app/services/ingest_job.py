"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class IngestJobService(BaseModuleService):
    MODULE = "ingest_job"
    ENTITY = "采集任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["作业编号", "日志来源", "采集模式", "文件游标", "批次大小", "并发线程", "处理行数", "跳过行数"]
    REQUIRED_FIELDS = ["作业编号", "日志来源", "采集模式"]
    STATUS_ORDER = ["待执行", "采集中", "已完成", "部分失败"]
    ACTION_RULES = {"启动采集": "采集中", "暂停作业": "已完成", "标记完成": "部分失败"}
    NEGATIVE_ACTIONS = []
