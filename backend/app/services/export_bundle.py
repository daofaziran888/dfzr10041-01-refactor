"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ExportBundleService(BaseModuleService):
    MODULE = "export_bundle"
    ENTITY = "导出包"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["包编号", "事件类型", "时间范围", "导出格式", "压缩方式", "文件大小", "行数统计", "导出状态"]
    REQUIRED_FIELDS = ["包编号", "事件类型", "时间范围"]
    STATUS_ORDER = ["待导出", "打包中", "已完成", "导出失败"]
    ACTION_RULES = {"启动导出": "打包中", "取消导出": "已完成", "重试导出": "导出失败"}
    NEGATIVE_ACTIONS = []
