"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class TagTaxonomyService(BaseModuleService):
    MODULE = "tag_taxonomy"
    ENTITY = "标签定义"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["标签编号", "标签名称", "父级标签", "适用事件", "自动规则", "关联事件数", "标签层级", "标签状态"]
    REQUIRED_FIELDS = ["标签编号", "标签名称", "父级标签"]
    STATUS_ORDER = ["草稿", "已启用", "规则冲突", "已废弃"]
    ACTION_RULES = {"启用标签": "已启用", "标记冲突": "规则冲突", "废弃标签": "已废弃"}
    NEGATIVE_ACTIONS = []
