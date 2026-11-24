"""历史复盘领域模型：归档快照、入口凭证、复盘项与会话。

复盘的目标不是“猜”历史来源，而是用可核验的入口凭证和归档当时的字段
重算出归档指纹，再与归档记录逐条比对。这里把参与核验的数据结构固定下来，
序列化结果可直接落库、跨进程重启后回查。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from app.core.fingerprint import (
    ORIGINAL_THEN_MASKED, VALUE_SOURCES, FingerprintFieldSelection,
)

# ---------- 复盘项状态 ----------

STATUS_PENDING_ENTRY = "待核"          # 缺少入口凭证或凭证不完整，保留待核
STATUS_PENDING_ORIGINAL = "待补原行"    # 凭证可信，但当前原行缺失，不能交付
STATUS_VERIFIED = "已核验"             # 凭证、归档指纹、当前原行三方一致
STATUS_MISMATCH = "失配隔离"           # 原行字段与归档不符，隔离并保留位置
STATUS_DISPATCHED = "已交付待确认"      # 原行已交给调查采样，等待采样端确认
STATUS_CONFIRMED = "已确认"            # 采样端确认已收下

# 会话状态
SESSION_PENDING = "待核"
SESSION_READY = "可交付"
SESSION_AWAITING_ACK = "已交付待确认"
SESSION_BLOCKED = "部分受阻"
SESSION_DONE = "已完成"


def utc_now() -> str:
    """统一的时间戳格式，便于落库和跨重启比较。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def canonical_json(value: Any) -> str:
    """稳定序列化：键排序、紧凑分隔，保证校验和可重算。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: Any) -> str:
    text = value if isinstance(value, str) else canonical_json(value)
    return hashlib.sha256(text.encode()).hexdigest()


# ---------- 归档快照 ----------


@dataclass
class ArchiveRow:
    """一条归档原行的“当时”快照，指纹依据也一并冻结。"""

    record_id: str
    event_type_id: str
    row_index: int = 0
    fingerprint_fields: tuple[str, ...] = ()
    value_source: str = ORIGINAL_THEN_MASKED
    parsed_fields: dict[str, str] = field(default_factory=dict)
    masked_fields: dict[str, str] = field(default_factory=dict)
    fingerprint: str = ""

    def __post_init__(self) -> None:
        self.fingerprint_fields = tuple(self.fingerprint_fields)
        if self.value_source not in VALUE_SOURCES:
            raise ValueError(f"归档行 {self.record_id} 的取值方式非法：{self.value_source}")
        if not self.fingerprint:
            self.fingerprint = self.computed_fingerprint()

    def selection(self) -> FingerprintFieldSelection:
        """归档当时的指纹字段选择数据。"""
        return FingerprintFieldSelection(
            fields=self.fingerprint_fields, value_source=self.value_source,
        )

    def computed_fingerprint(self) -> str:
        """按归档当时字段重算指纹。"""
        return self.selection().fingerprint_from_fields(
            self.event_type_id, self.parsed_fields, self.masked_fields,
        )

    def checksum(self) -> str:
        """行级校验和：入口凭证用它证明“当时字段”未被改写。"""
        return _sha256({
            "record_id": self.record_id,
            "event_type_id": self.event_type_id,
            "row_index": self.row_index,
            "fingerprint_fields": list(self.fingerprint_fields),
            "value_source": self.value_source,
            "parsed_fields": self.parsed_fields,
            "masked_fields": self.masked_fields,
            "fingerprint": self.fingerprint,
        })

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "event_type_id": self.event_type_id,
            "row_index": self.row_index,
            "fingerprint_fields": list(self.fingerprint_fields),
            "value_source": self.value_source,
            "parsed_fields": dict(self.parsed_fields),
            "masked_fields": dict(self.masked_fields),
            "fingerprint": self.fingerprint,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ArchiveRow":
        return cls(
            record_id=str(payload["record_id"]),
            event_type_id=str(payload["event_type_id"]),
            row_index=int(payload.get("row_index", 0)),
            fingerprint_fields=tuple(payload.get("fingerprint_fields") or ()),
            value_source=str(payload.get("value_source") or ORIGINAL_THEN_MASKED),
            parsed_fields=dict(payload.get("parsed_fields") or {}),
            masked_fields=dict(payload.get("masked_fields") or {}),
            fingerprint=str(payload.get("fingerprint") or ""),
        )


@dataclass
class ArchiveBatch:
    """一次归档批次：归档清单本身可核验，供入口凭证比对。"""

    batch_id: str
    source_id: str
    partition: str
    rows: list[ArchiveRow] = field(default_factory=list)
    archived_at: str = field(default_factory=utc_now)

    def ordered_rows(self) -> list[ArchiveRow]:
        return sorted(self.rows, key=lambda row: (row.row_index, row.record_id))

    def row_map(self) -> dict[str, ArchiveRow]:
        return {row.record_id: row for row in self.rows}

    def row_checksums(self) -> dict[str, str]:
        return {row.record_id: row.checksum() for row in self.rows}

    def manifest_checksum(self) -> str:
        """按行顺序串联行校验和，形成归档清单指纹。"""
        return _sha256([row.checksum() for row in self.ordered_rows()])

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "source_id": self.source_id,
            "partition": self.partition,
            "archived_at": self.archived_at,
            "manifest_checksum": self.manifest_checksum(),
            "row_count": len(self.rows),
            "rows": [row.to_dict() for row in self.ordered_rows()],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ArchiveBatch":
        return cls(
            batch_id=str(payload["batch_id"]),
            source_id=str(payload.get("source_id") or ""),
            partition=str(payload.get("partition") or ""),
            archived_at=str(payload.get("archived_at") or utc_now()),
            rows=[ArchiveRow.from_dict(row) for row in payload.get("rows") or []],
        )


# ---------- 入口凭证 ----------


@dataclass(frozen=True)
class CredentialCheck:
    """入口凭证核验结果。

    ``ok`` 表示凭证整体可信（批次身份、来源、分区、自身校验和一致）。
    ``covered`` 是本次凭证实际给出入口证明的行；
    ``row_problems`` 是逐行的问题（校验和不符等），只影响对应行。
    归档中未被凭证覆盖的行不构成本凭证的错误，它们保持“待核”。
    """

    ok: bool
    credential_id: str
    problems: list[str] = field(default_factory=list)
    covered: tuple[str, ...] = ()
    row_problems: dict[str, str] = field(default_factory=dict)

    def reason(self) -> str:
        if not self.ok:
            return "入口凭证待核：" + "；".join(self.problems)
        if self.row_problems:
            return "入口凭证已核验，部分行仍待核：" + "；".join(
                f"{rid} {reason}" for rid, reason in sorted(self.row_problems.items()))
        return "入口凭证有效"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "credential_id": self.credential_id,
            "problems": list(self.problems),
            "covered": list(self.covered),
            "row_problems": dict(self.row_problems),
            "reason": self.reason(),
        }


@dataclass(frozen=True)
class EntryCredential:
    """可核验的入口凭证：证明某批归档原行及其当时字段确实来自该入口。"""

    credential_id: str
    batch_id: str
    source_id: str
    partition: str
    revision: int = 1
    issued_by: str = ""
    issued_at: str = field(default_factory=utc_now)
    row_checksums: dict[str, str] = field(default_factory=dict)
    note: str = ""
    declared_checksum: str = ""

    def content(self) -> dict[str, Any]:
        return {
            "credential_id": self.credential_id,
            "batch_id": self.batch_id,
            "source_id": self.source_id,
            "partition": self.partition,
            "revision": self.revision,
            "issued_by": self.issued_by,
            "issued_at": self.issued_at,
            "row_checksums": dict(sorted(self.row_checksums.items())),
            "note": self.note,
        }

    def checksum(self) -> str:
        return _sha256(self.content())

    def stale_checksum(self) -> bool:
        """凭证自称的校验和与内容是否一致（防止凭证被拼改）。"""
        return bool(self.declared_checksum) and self.declared_checksum != self.checksum()

    def to_dict(self) -> dict[str, Any]:
        payload = self.content()
        payload["checksum"] = self.declared_checksum or self.checksum()
        return payload

    @classmethod
    def from_batch(cls, batch: ArchiveBatch, *, credential_id: str,
                   revision: int = 1, issued_by: str = "",
                   issued_at: str | None = None, note: str = "",
                   row_checksums: dict[str, str] | None = None,
                   declared_checksum: str = "") -> "EntryCredential":
        """按归档清单签发完整凭证；也可传入部分行清单生成“不完整”凭证。"""
        return cls(
            credential_id=credential_id,
            batch_id=batch.batch_id,
            source_id=batch.source_id,
            partition=batch.partition,
            revision=revision,
            issued_by=issued_by,
            issued_at=issued_at or utc_now(),
            row_checksums=dict(row_checksums if row_checksums is not None
                               else batch.row_checksums()),
            note=note,
            declared_checksum=declared_checksum,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "EntryCredential":
        return cls(
            credential_id=str(payload["credential_id"]),
            batch_id=str(payload["batch_id"]),
            source_id=str(payload.get("source_id") or ""),
            partition=str(payload.get("partition") or ""),
            revision=int(payload.get("revision", 1)),
            issued_by=str(payload.get("issued_by") or ""),
            issued_at=str(payload.get("issued_at") or utc_now()),
            row_checksums={str(k): str(v) for k, v in (payload.get("row_checksums") or {}).items()},
            note=str(payload.get("note") or ""),
            declared_checksum=str(payload.get("checksum") or ""),
        )

    def verify(self, batch: ArchiveBatch | None) -> CredentialCheck:
        """逐项核验：凭证身份、批次一致、行清单与逐行校验和。

        行清单为空视为“没有入口证明”，整体不可用；
        行清单部分覆盖归档时只核验覆盖到的行，其余行保持待核。
        """
        if batch is None:
            return CredentialCheck(False, self.credential_id, ["找不到对应归档批次"])
        problems: list[str] = []
        if self.stale_checksum():
            problems.append("凭证自身校验和与内容不符")
        if self.batch_id != batch.batch_id:
            problems.append(f"凭证批次 {self.batch_id} 与归档 {batch.batch_id} 不一致")
        if self.source_id != batch.source_id:
            problems.append(f"凭证来源 {self.source_id or '空'} 与归档 {batch.source_id} 不一致")
        if self.partition != batch.partition:
            problems.append(f"凭证分区 {self.partition or '空'} 与归档 {batch.partition} 不一致")
        if not self.row_checksums:
            problems.append("凭证缺少行清单（入口证明不完整）")
        if problems:
            return CredentialCheck(False, self.credential_id, problems)

        batch_checksums = batch.row_checksums()
        covered = tuple(sorted(set(self.row_checksums) & set(batch_checksums)))
        extra = sorted(set(self.row_checksums) - set(batch_checksums))
        if extra:
            return CredentialCheck(
                False, self.credential_id,
                ["凭证包含归档外行：" + "、".join(extra)],
            )
        row_problems = {
            record_id: "当时字段校验和不符"
            for record_id in covered
            if self.row_checksums[record_id] != batch_checksums[record_id]
        }
        return CredentialCheck(True, self.credential_id, covered=covered,
                               row_problems=row_problems)


# ---------- 当前原行 ----------


@dataclass
class OriginalRow:
    """当前实际原行：复盘要和归档快照比对，确认没有被换过字段。"""

    record_id: str
    event_type_id: str
    parsed_fields: dict[str, str] = field(default_factory=dict)
    masked_fields: dict[str, str] = field(default_factory=dict)
    content: str = ""
    location: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "event_type_id": self.event_type_id,
            "parsed_fields": dict(self.parsed_fields),
            "masked_fields": dict(self.masked_fields),
            "content": self.content,
            "location": self.location,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "OriginalRow":
        return cls(
            record_id=str(payload["record_id"]),
            event_type_id=str(payload.get("event_type_id") or ""),
            parsed_fields=dict(payload.get("parsed_fields") or {}),
            masked_fields=dict(payload.get("masked_fields") or {}),
            content=str(payload.get("content") or ""),
            location=str(payload.get("location") or ""),
        )


# ---------- 复盘项与会话 ----------


@dataclass
class ReviewItem:
    review_id: str
    record_id: str
    row_index: int = 0
    status: str = STATUS_PENDING_ENTRY
    recorded_fingerprint: str = ""
    recomputed_fingerprint: str = ""
    basis: dict[str, Any] = field(default_factory=dict)
    mismatch: dict[str, Any] | None = None
    sample_id: str = ""
    dispatched_at: str = ""
    ack_at: str = ""
    basis_locked: bool = False
    credential_revision: int = 0
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "review_id": self.review_id,
            "record_id": self.record_id,
            "row_index": self.row_index,
            "status": self.status,
            "recorded_fingerprint": self.recorded_fingerprint,
            "recomputed_fingerprint": self.recomputed_fingerprint,
            "basis": dict(self.basis),
            "mismatch": self.mismatch,
            "sample_id": self.sample_id,
            "dispatched_at": self.dispatched_at,
            "ack_at": self.ack_at,
            "basis_locked": self.basis_locked,
            "credential_revision": self.credential_revision,
            "note": self.note,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ReviewItem":
        return cls(
            review_id=str(payload["review_id"]),
            record_id=str(payload["record_id"]),
            row_index=int(payload.get("row_index", 0)),
            status=str(payload.get("status") or STATUS_PENDING_ENTRY),
            recorded_fingerprint=str(payload.get("recorded_fingerprint") or ""),
            recomputed_fingerprint=str(payload.get("recomputed_fingerprint") or ""),
            basis=dict(payload.get("basis") or {}),
            mismatch=payload.get("mismatch"),
            sample_id=str(payload.get("sample_id") or ""),
            dispatched_at=str(payload.get("dispatched_at") or ""),
            ack_at=str(payload.get("ack_at") or ""),
            basis_locked=bool(payload.get("basis_locked")),
            credential_revision=int(payload.get("credential_revision", 0)),
            note=str(payload.get("note") or ""),
        )


@dataclass
class ReviewSession:
    review_id: str
    batch_id: str
    credential_id: str = ""
    credential_revision: int = 0
    status: str = SESSION_PENDING
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    resume_cursor: int = 0
    pending_record_ids: list[str] = field(default_factory=list)
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "review_id": self.review_id,
            "batch_id": self.batch_id,
            "credential_id": self.credential_id,
            "credential_revision": self.credential_revision,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "resume_cursor": self.resume_cursor,
            "pending_record_ids": list(self.pending_record_ids),
            "note": self.note,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ReviewSession":
        return cls(
            review_id=str(payload["review_id"]),
            batch_id=str(payload["batch_id"]),
            credential_id=str(payload.get("credential_id") or ""),
            credential_revision=int(payload.get("credential_revision", 0)),
            status=str(payload.get("status") or SESSION_PENDING),
            created_at=str(payload.get("created_at") or utc_now()),
            updated_at=str(payload.get("updated_at") or utc_now()),
            resume_cursor=int(payload.get("resume_cursor", 0)),
            pending_record_ids=list(payload.get("pending_record_ids") or []),
            note=str(payload.get("note") or ""),
        )


def selected_field_locations(archive_row: ArchiveRow,
                             current_row: OriginalRow) -> list[dict[str, Any]]:
    """比对归档当时字段与当前原行，指出失配位置（字段名+字段序号+值）。"""
    selection = archive_row.selection()
    archived = selection.field_values_from_fields(
        archive_row.parsed_fields, archive_row.masked_fields)
    current = selection.field_values_from_fields(
        current_row.parsed_fields, current_row.masked_fields)
    locations: list[dict[str, Any]] = []
    for index, (old, new) in enumerate(zip(archived, current)):
        if old["value"] != new["value"] or old["source"] != new["source"]:
            locations.append({
                "field": old["field"],
                "field_index": index,
                "archived_value": old["value"],
                "archived_source": old["source"],
                "current_value": new["value"],
                "current_source": new["source"],
                "in_fingerprint": True,
            })
    # 指纹字段之外的字段被换过同样要指出，避免“只保住关键字段”的漏检。
    archived_all = _merged_fields(archive_row.parsed_fields, archive_row.masked_fields)
    current_all = _merged_fields(current_row.parsed_fields, current_row.masked_fields)
    selected = set(selection.fields)
    for field in sorted(set(archived_all) | set(current_all)):
        if field in selected:
            continue
        if archived_all.get(field, "") != current_all.get(field, ""):
            locations.append({
                "field": field,
                "field_index": None,
                "archived_value": archived_all.get(field, ""),
                "archived_source": "parsed_or_masked",
                "current_value": current_all.get(field, ""),
                "current_source": "parsed_or_masked",
                "in_fingerprint": False,
            })
    return locations


def _merged_fields(parsed: Mapping[str, str], masked: Mapping[str, str]) -> dict[str, str]:
    merged = dict(masked)
    merged.update(parsed)
    return merged


def summarize_status(items: Iterable[ReviewItem]) -> str:
    """由复盘项状态推导会话状态；未核验时一律停留在“待核”。"""
    items = list(items)
    if not items:
        return SESSION_PENDING
    statuses = [item.status for item in items]
    if all(status == STATUS_CONFIRMED for status in statuses):
        return SESSION_DONE
    if any(status in (STATUS_DISPATCHED, STATUS_CONFIRMED) for status in statuses):
        return SESSION_AWAITING_ACK
    if any(status in (STATUS_MISMATCH, STATUS_PENDING_ORIGINAL) for status in statuses):
        return SESSION_BLOCKED
    if any(status == STATUS_VERIFIED for status in statuses):
        return SESSION_READY
    return SESSION_PENDING
