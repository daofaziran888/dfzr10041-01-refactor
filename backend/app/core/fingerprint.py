"""事件日志指纹字段选择与共用计算。

普通去重（``IngestPipeline.dedup_events``）与历史规则去重
（``DedupFilter.fingerprint``）原先各写了一遍“取哪些字段、字段值从哪里取”
的逻辑。这里把它收拢成一个可复用的数据对象 ``FingerprintFieldSelection``：

- ``ORIGINAL_FIELDS``：只取原始解析字段（普通去重使用）；
- ``ORIGINAL_THEN_MASKED``：原始字段为空时回退到已有脱敏字段
  （基于规则的历史去重使用），命中/忽略统计仍留在调用方。

两种取值方式与旧实现逐字节一致，直接调用方的既有行为不变。
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Mapping

# 字段值来源：与旧实现保持同名语义，便于复盘记录直接回查。
ORIGINAL_FIELDS = "original_fields"
ORIGINAL_THEN_MASKED = "original_then_masked"
VALUE_SOURCES = (ORIGINAL_FIELDS, ORIGINAL_THEN_MASKED)


@dataclass(frozen=True)
class FingerprintFieldSelection:
    """指纹字段选择数据：字段集合 + 取值方式 + 摘要算法。

    计算规则（与重构前完全一致）：
    1. 指纹原文以事件类型编号开头，后面按字段名排序拼接 ``字段=值``；
    2. ``原始字段`` 模式只读 ``parsed_fields``，缺字段按空串处理；
    3. ``原始字段后回退脱敏`` 模式在原始值为空时读 ``masked_fields``；
    4. 各段以 ``|`` 连接后做 SHA256。
    """

    fields: tuple[str, ...] = ()
    value_source: str = ORIGINAL_FIELDS
    algo: str = "sha256"

    def __post_init__(self) -> None:
        if self.value_source not in VALUE_SOURCES:
            raise ValueError(f"未知的指纹字段取值方式：{self.value_source}")
        if self.algo != "sha256":
            raise ValueError(f"暂不支持的指纹算法：{self.algo}")
        # 旧实现每次计算时对字段名排序，这里在构造时固定下来。
        object.__setattr__(self, "fields", tuple(sorted(self.fields)))

    # ---------- 构造入口 ----------

    @classmethod
    def ordinary(cls, fields: list[str] | tuple[str, ...]) -> "FingerprintFieldSelection":
        """普通去重：只按原始解析字段取值。"""
        return cls(fields=tuple(fields), value_source=ORIGINAL_FIELDS)

    @classmethod
    def rule(cls, fields: list[str] | tuple[str, ...]) -> "FingerprintFieldSelection":
        """基于规则的历史去重：原始字段为空时回退已有脱敏字段。"""
        return cls(fields=tuple(fields), value_source=ORIGINAL_THEN_MASKED)

    # ---------- 选择与计算 ----------

    @property
    def selected_fields(self) -> list[str]:
        """实际参与指纹计算的字段（已排序）。"""
        return list(self.fields)

    def value_for(self, record: Any, field: str) -> str:
        """按取值方式取得某字段参与指纹的值。"""
        return self.value_from_fields(
            field,
            getattr(record, "parsed_fields", None) or {},
            getattr(record, "masked_fields", None) or {},
        )

    def value_from_fields(self, field: str,
                          parsed_fields: Mapping[str, str],
                          masked_fields: Mapping[str, str] | None = None) -> str:
        """按取值方式从字段映射中取得某字段参与指纹的值。"""
        value = parsed_fields.get(field)
        if value:
            return value
        if self.value_source == ORIGINAL_THEN_MASKED and masked_fields:
            return masked_fields.get(field) or ""
        return value or ""

    def field_values_from_fields(self, parsed_fields: Mapping[str, str],
                                 masked_fields: Mapping[str, str] | None = None
                                 ) -> list[dict[str, str]]:
        """按字段顺序输出 (字段, 值, 取值来源)，供复盘留证与核对。"""
        masked_fields = masked_fields or {}
        pairs: list[dict[str, str]] = []
        for field in self.fields:
            if parsed_fields.get(field):
                source, value = "parsed", parsed_fields[field]
            elif self.value_source == ORIGINAL_THEN_MASKED and masked_fields.get(field):
                source, value = "masked", masked_fields[field]
            else:
                source, value = "empty", ""
            pairs.append({"field": field, "value": value, "source": source})
        return pairs

    def field_values(self, record: Any) -> list[dict[str, str]]:
        """按字段顺序输出 (字段, 值, 取值来源)，供复盘留证与核对。"""
        return self.field_values_from_fields(
            getattr(record, "parsed_fields", None) or {},
            getattr(record, "masked_fields", None) or {},
        )

    def payload_from_fields(self, event_type_id: str,
                            parsed_fields: Mapping[str, str],
                            masked_fields: Mapping[str, str] | None = None) -> str:
        """按字段映射生成指纹原文（不含摘要），可原样归档供重算核对。"""
        key_parts = [event_type_id]
        for field in self.fields:
            key_parts.append(
                f"{field}={self.value_from_fields(field, parsed_fields, masked_fields)}"
            )
        return "|".join(key_parts)

    def payload(self, record: Any) -> str:
        """生成指纹原文（不含摘要），可原样归档供重算核对。"""
        return self.payload_from_fields(
            record.event_type.type_id,
            getattr(record, "parsed_fields", None) or {},
            getattr(record, "masked_fields", None) or {},
        )

    def fingerprint(self, record: Any) -> str:
        """按选择数据生成事件指纹。"""
        return hashlib.sha256(self.payload(record).encode()).hexdigest()

    def fingerprint_from_fields(self, event_type_id: str,
                                parsed_fields: Mapping[str, str],
                                masked_fields: Mapping[str, str] | None = None) -> str:
        """按字段映射生成事件指纹（复盘重算归档指纹时使用）。"""
        payload = self.payload_from_fields(event_type_id, parsed_fields, masked_fields)
        return hashlib.sha256(payload.encode()).hexdigest()

    def describe(self) -> dict[str, Any]:
        """选择数据的可序列化描述，用于复盘凭证与归档回查。"""
        return {
            "fields": list(self.fields),
            "value_source": self.value_source,
            "algo": self.algo,
        }
