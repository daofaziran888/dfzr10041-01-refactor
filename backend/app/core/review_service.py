"""历史复盘与调查采样：凭证核验、归档指纹重算、失配隔离、交付与确认。

设计要点（与需求逐条对应）：

- 复盘只认可核验的入口凭证 + 归档当时字段；没有凭证就停在“待核”，
  绝不按时间、来源名之类的线索猜一个来源补上。
- 通过校验的原行与实际交付的原行是同一份数据；交付后调查采样据此生成样本。
- 交付、确认、凭证更正都按 (review_id, record_id) 幂等，重复调用不会增发样本。
- 已派出的样本锁定指纹依据 ``basis_locked``，后续凭证更正只做复核记录，
  不重写其去重依据；未交付原行则随凭证修订刷新依据，继续可交付。
- 会话、复盘项、样本、交付记录和审计事件全部落 JSON，重启后按编号回查。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from app.core.dedup_sample import DedupSamplePipeline
from app.core.fingerprint import FingerprintFieldSelection
from app.core.models import EventRecord, EventTypeDef, SampleConfig
from app.core.review_models import (
    ArchiveBatch, ArchiveRow, EntryCredential, OriginalRow,
    ReviewItem, ReviewSession, STATUS_CONFIRMED, STATUS_DISPATCHED,
    STATUS_MISMATCH, STATUS_PENDING_ENTRY, STATUS_PENDING_ORIGINAL,
    STATUS_VERIFIED, selected_field_locations, summarize_status, utc_now,
)
from app.core.review_store import ReviewStore

# ---------- 调查采样状态 ----------

SAMPLE_PENDING = "待派出"
SAMPLE_DISPATCHED = "已派出"
SAMPLE_CONFIRMED = "已确认"


class InvestigationSampler:
    """调查采样端：收下复盘交付的原行，生成调查样本并派出、确认。

    采样端只认交付时传入的“实际原行 + 指纹依据”快照，不回头读复盘侧现状，
    因此复盘侧后续的凭证更正不会影响已经生成并派出的样本。
    """

    def __init__(self, store: ReviewStore | None = None,
                 pipeline: DedupSamplePipeline | None = None) -> None:
        self.store = store or ReviewStore()
        self.pipeline = pipeline or DedupSamplePipeline(
            dedup_rules=[], sample_configs=[])

    # ---------- 生成 ----------

    def _samples_for(self, review_id: str) -> list[dict[str, Any]]:
        return [
            sample for sample in self.store.all("samples").values()
            if sample.get("review_id") == review_id
        ]

    def find_sample(self, review_id: str, record_id: str) -> dict[str, Any] | None:
        """按复盘编号+原行编号查找样本，用于交付幂等。"""
        for sample in self._samples_for(review_id):
            if sample.get("record_id") == record_id:
                return sample
        return None

    @staticmethod
    def _to_event_record(original_row: OriginalRow) -> EventRecord:
        """把实际交付的原行还原成事件记录，交给既有采样管道。"""
        return EventRecord(
            record_id=original_row.record_id,
            event_type=EventTypeDef(
                type_id=original_row.event_type_id or "UNKNOWN",
                type_name=original_row.event_type_id or "未知事件",
                category_tag="investigation",
            ),
            original_content=original_row.content,
            parsed_fields=dict(original_row.parsed_fields),
            masked_fields=dict(original_row.masked_fields),
            enriched_fields={},
            event_time=datetime.now(timezone.utc),
            source_host=original_row.location or "archive",
        )

    def receive(self, review_id: str, item: dict[str, Any],
                original_row: OriginalRow) -> tuple[dict[str, Any], bool]:
        """收下一条已核验原行并生成调查样本。

        返回 (样本, 是否新建)。同一复盘编号下同一原行只生成一个样本；
        新样本的抽样结果由既有 ``sample_job`` 采样管道给出。
        """
        existing = self.find_sample(review_id, str(item["record_id"]))
        if existing is not None:
            return existing, False
        record = self._to_event_record(original_row)
        config = SampleConfig(
            plan_id=f"INV-{review_id}", event_type_id=record.event_type.type_id,
            sample_ratio=1.0, merge_key="",
        )
        picked = self.pipeline.sample([record], config) or [record]
        # 样本编号按归档行号派生：多次/多进程调用得到的编号一致，避免并发撞号。
        ordinal = int(item.get("row_index", 0)) + 1
        sample_id = f"SAMP-{review_id}-{ordinal:04d}"
        if self.store.get("samples", sample_id) is not None:
            ordinal = len(self._samples_for(review_id)) + 1
            sample_id = f"SAMP-{review_id}-{ordinal:04d}"
        sample = {
            "sample_id": sample_id,
            "review_id": review_id,
            "record_id": item["record_id"],
            "archive_batch_id": item["basis"].get("archive_batch_id", ""),
            "status": SAMPLE_PENDING,
            "basis": dict(item["basis"]),
            "basis_locked": False,
            "delivered_row": original_row.to_dict(),
            "sampled_record_id": picked[0].record_id,
            "created_at": utc_now(),
            "dispatched_at": "",
            "ack_at": "",
        }
        self.store.put("samples", sample["sample_id"], sample)
        self.store.append_event({
            "at": sample["created_at"], "action": "调查采样收下原行",
            "review_id": review_id, "record_id": item["record_id"],
            "sample_id": sample["sample_id"], "new": True,
        })
        return sample, True

    def dispatch(self, sample_id: str) -> dict[str, Any] | None:
        """派出样本；重复派出保持首次派出时间不变。"""
        sample = self.store.get("samples", sample_id)
        if sample is None:
            return None
        if sample["status"] in (SAMPLE_DISPATCHED, SAMPLE_CONFIRMED):
            return sample
        sample["status"] = SAMPLE_DISPATCHED
        sample["dispatched_at"] = utc_now()
        sample["basis_locked"] = True
        self.store.put("samples", sample_id, sample)
        self.store.append_event({
            "at": sample["dispatched_at"], "action": "调查样本已派出",
            "review_id": sample["review_id"], "record_id": sample["record_id"],
            "sample_id": sample_id, "new": False,
        })
        return sample

    def acknowledge(self, sample_id: str) -> tuple[dict[str, Any] | None, bool]:
        """复盘端收到采样确认；重复确认幂等，返回 (样本, 是否首次确认)。"""
        sample = self.store.get("samples", sample_id)
        if sample is None:
            return None, False
        if sample["status"] == SAMPLE_CONFIRMED:
            return sample, False
        sample["status"] = SAMPLE_CONFIRMED
        sample["ack_at"] = utc_now()
        self.store.put("samples", sample_id, sample)
        self.store.append_event({
            "at": sample["ack_at"], "action": "复盘端确认送达",
            "review_id": sample["review_id"], "record_id": sample["record_id"],
            "sample_id": sample_id, "new": False,
        })
        return sample, True

    def list_samples(self, review_id: str) -> list[dict[str, Any]]:
        return sorted(self._samples_for(review_id), key=lambda s: str(s["sample_id"]))


class ReviewService:
    """历史复盘：按入口凭证重算归档指纹，核验实际原行后交付调查采样。"""

    def __init__(self, store: ReviewStore | None = None,
                 sampler: InvestigationSampler | None = None) -> None:
        self.store = store or ReviewStore()
        self.sampler = sampler or InvestigationSampler(self.store)

    # ---------- 归档与凭证登记 ----------

    def register_archive(self, batch: ArchiveBatch) -> ArchiveBatch:
        self.store.put("archives", batch.batch_id, batch.to_dict())
        return batch

    def get_archive(self, batch_id: str) -> ArchiveBatch | None:
        payload = self.store.get("archives", batch_id)
        return ArchiveBatch.from_dict(payload) if payload else None

    def register_credential(self, credential: EntryCredential) -> EntryCredential:
        self.store.put("credentials", credential.credential_id, credential.to_dict())
        return credential

    def get_credential(self, credential_id: str) -> EntryCredential | None:
        payload = self.store.get("credentials", credential_id)
        return EntryCredential.from_dict(payload) if payload else None

    def _append_event(self, action: str, **fields: Any) -> None:
        self.store.append_event({"at": utc_now(), "action": action, **fields})

    # ---------- 会话与复盘项存取 ----------

    def _load_session(self, review_id: str) -> ReviewSession | None:
        payload = self.store.get("reviews", review_id)
        return ReviewSession.from_dict(payload) if payload else None

    def _save_session(self, session: ReviewSession) -> None:
        session.updated_at = utc_now()
        self.store.put("reviews", session.review_id, session.to_dict())

    def _load_items(self, review_id: str) -> list[ReviewItem]:
        payload = self.store.get("items", review_id) or []
        return [ReviewItem.from_dict(item) for item in payload]

    def _save_items(self, review_id: str, items: Iterable[ReviewItem]) -> None:
        self.store.put("items", review_id,
                       [item.to_dict() for item in items])

    # ---------- 开审 ----------

    def start_review(self, batch_id: str, credential: EntryCredential | None = None,
                     review_id: str | None = None, note: str = "") -> dict[str, Any]:
        """按归档批次开立复盘会话；未给凭证时全部条目停在“待核”。"""
        batch = self.get_archive(batch_id)
        if batch is None:
            raise ValueError(f"归档批次 {batch_id} 不存在")
        review_id = review_id or f"RVW-{batch_id}"
        if self._load_session(review_id) is not None:
            raise ValueError(f"复盘 {review_id} 已存在，请继续处理或换编号")
        session = ReviewSession(review_id=review_id, batch_id=batch_id, note=note)
        items = [
            ReviewItem(
                review_id=review_id, record_id=row.record_id,
                row_index=row.row_index, status=STATUS_PENDING_ENTRY,
                recorded_fingerprint=row.fingerprint,
            )
            for row in batch.ordered_rows()
        ]
        self._refresh_pending(session, items)
        self._save_items(review_id, items)
        self._save_session(session)
        self._append_event("开立复盘", review_id=review_id, batch_id=batch_id,
                           items=len(items))
        if credential is not None:
            self.submit_credential(review_id, credential)
        return self.trace(review_id)

    # ---------- 凭证核验 ----------

    def submit_credential(self, review_id: str, credential: EntryCredential,
                          correction: bool = False) -> dict[str, Any]:
        """提交（或补送更正）入口凭证。

        - 凭证无效：会话保持待核/受阻，原行依据不动，不做任何来源猜测。
        - 凭证有效：逐条重算归档指纹并核对当前原行；
          已派出样本锁定依据，只记录复核，不重写 basis。
        """
        session = self._load_session(review_id)
        if session is None:
            raise ValueError(f"复盘 {review_id} 不存在")
        batch = self.get_archive(session.batch_id)
        if batch is None:
            raise ValueError(f"复盘 {review_id} 的归档批次 {session.batch_id} 不存在")
        check = credential.verify(batch)
        self.register_credential(credential)

        items = self._load_items(review_id)
        by_record = {item.record_id: item for item in items}
        archive_rows = batch.row_map()

        if not check.ok:
            # 凭证不成立时不做任何来源推测，也不推翻此前已核验的结论；
            # 从未核验过的条目继续停在“待核”。
            for item in items:
                if item.status in (STATUS_DISPATCHED, STATUS_CONFIRMED) or item.basis_locked:
                    continue
                if item.status not in (STATUS_VERIFIED, STATUS_MISMATCH,
                                       STATUS_PENDING_ORIGINAL):
                    item.status = STATUS_PENDING_ENTRY
                if item.status == STATUS_PENDING_ENTRY:
                    item.note = check.reason()
            self._refresh_pending(session, items)
            self._save_items(review_id, items)
            self._save_session(session)
            self._append_event("入口凭证待核", review_id=review_id,
                               credential_id=credential.credential_id,
                               revision=credential.revision,
                               problems=check.problems)
            return self.trace(review_id)

        session.credential_id = credential.credential_id
        session.credential_revision = credential.revision

        # 本次凭证覆盖的行即本次复核范围；未被覆盖的行保持原状态（待核/待补）。
        for record_id in check.covered:
            item = by_record.get(record_id)
            row = archive_rows.get(record_id)
            if item is None or row is None:
                continue
            if item.basis_locked:
                # 已派出样本的去重依据不可改写，只留下复核记录。
                item.note = (
                    f"已派出样本依据锁定；凭证 {credential.credential_id}"
                    f"（第 {credential.revision} 版）仅复核不重写"
                )
                continue
            if record_id in check.row_problems:
                item.status = STATUS_PENDING_ENTRY
                item.note = check.row_problems[record_id] + "，保留待核"
                continue
            self._verify_item(item, row, credential)

        # 凭证之外的归档行：原先因“凭证不全”停留在待核的，继续保持待核。
        for item in items:
            if item.record_id in check.covered or item.basis_locked:
                continue
            if item.status in (STATUS_DISPATCHED, STATUS_CONFIRMED):
                continue
            if item.status == STATUS_PENDING_ENTRY:
                item.note = "入口凭证未覆盖此行，保留待核"

        self._refresh_pending(session, items)
        self._save_items(review_id, items)
        self._save_session(session)
        self._append_event(
            "更正凭证复核" if correction else "入口凭证核验",
            review_id=review_id, credential_id=credential.credential_id,
            revision=credential.revision,
            verified=sum(1 for i in items if i.status == STATUS_VERIFIED),
            mismatched=sum(1 for i in items if i.status == STATUS_MISMATCH),
            locked=sum(1 for i in items if i.basis_locked),
        )
        return self.trace(review_id)

    def _verify_item(self, item: ReviewItem, row: ArchiveRow,
                     credential: EntryCredential) -> None:
        """单条核验：重算归档指纹 + 比对当前原行字段。"""
        selection = row.selection()
        recomputed = row.computed_fingerprint()
        current = self.get_original_row(item.record_id)
        values = row.selection().field_values_from_fields(
            row.parsed_fields, row.masked_fields)
        basis = {
            "review_id": item.review_id,
            "record_id": item.record_id,
            "archive_batch_id": self._batch_of(item.review_id),
            "archive_fingerprint": row.fingerprint,
            "recomputed_fingerprint": recomputed,
            "fingerprint_fields": selection.selected_fields,
            "value_source": selection.value_source,
            "algo": selection.algo,
            "fingerprint_payload": row.selection().payload_from_fields(
                row.event_type_id, row.parsed_fields, row.masked_fields),
            "field_values": values,
            "credential_id": credential.credential_id,
            "credential_revision": credential.revision,
            "verified_at": utc_now(),
        }
        item.recorded_fingerprint = row.fingerprint
        item.recomputed_fingerprint = recomputed
        item.basis = basis
        item.credential_revision = credential.revision
        item.mismatch = None

        if recomputed != row.fingerprint:
            # 归档自身指纹对不上：不能当作已核验，保留待核。
            item.status = STATUS_PENDING_ENTRY
            item.note = "归档指纹与当时字段重算结果不一致，保留待核"
            return
        if current is None:
            item.status = STATUS_PENDING_ORIGINAL
            item.note = "入口凭证有效，但当前原行缺失，保留待补"
            return
        locations = selected_field_locations(row, current)
        if locations:
            item.status = STATUS_MISMATCH
            item.note = "当前原行字段与归档不符，已隔离失配样本"
            item.mismatch = {
                "isolated": True,
                "locations": locations,
                "record_id": item.record_id,
                "row_index": row.row_index,
                "archived_fingerprint": row.fingerprint,
                "original_location": current.location,
                "checked_with_credential": credential.credential_id,
                "isolated_at": utc_now(),
            }
            return
        item.status = STATUS_VERIFIED
        item.note = "入口凭证、归档指纹、当前原行三方一致"

    def _batch_of(self, review_id: str) -> str:
        session = self._load_session(review_id)
        return session.batch_id if session else ""

    # ---------- 当前原行 ----------

    def set_original_row(self, row: OriginalRow) -> OriginalRow:
        """登记/更新当前实际原行；复盘核验以这里的内容为准。"""
        self.store.put("originals", row.record_id, row.to_dict())
        return row

    def get_original_row(self, record_id: str) -> OriginalRow | None:
        payload = self.store.get("originals", record_id)
        return OriginalRow.from_dict(payload) if payload else None

    # ---------- 交付 ----------

    def deliver_verified(self, review_id: str) -> dict[str, Any]:
        """把通过核验且尚未交付的原行交给调查采样。

        幂等键是 (review_id, record_id)：已派出的原行不会再发；
        被隔离的失配样本、待补原行的条目都不会进入交付。
        """
        session = self._load_session(review_id)
        if session is None:
            raise ValueError(f"复盘 {review_id} 不存在")
        items = self._load_items(review_id)
        delivered_now: list[str] = []
        for item in items:
            if item.status != STATUS_VERIFIED:
                continue
            original = self.get_original_row(item.record_id)
            if original is None:
                item.status = STATUS_PENDING_ORIGINAL
                item.note = "交付前当前原行缺失，保留待补"
                continue
            sample, created = self.sampler.receive(review_id, item.to_dict(), original)
            dispatched = self.sampler.dispatch(sample["sample_id"])
            item.sample_id = dispatched["sample_id"] if dispatched else sample["sample_id"]
            item.status = STATUS_DISPATCHED
            item.dispatched_at = dispatched["dispatched_at"] if dispatched else ""
            item.basis_locked = True
            if item.basis:
                item.basis = dict(item.basis)
                item.basis["locked_at"] = item.dispatched_at
            if created:
                delivered_now.append(item.record_id)

        self._refresh_pending(session, items)
        self._save_items(review_id, items)
        self._save_session(session)
        self._append_event("复盘交付调查采样", review_id=review_id,
                           delivered=delivered_now,
                           dispatched_total=sum(
                               1 for i in items if i.status == STATUS_DISPATCHED))
        return self.trace(review_id)

    # ---------- 确认 ----------

    def acknowledge_sample(self, sample_id: str) -> dict[str, Any]:
        """复盘端收到采样确认，并把对应复盘项标记为已确认。"""
        sample, first = self.sampler.acknowledge(sample_id)
        if sample is None:
            raise ValueError(f"调查样本 {sample_id} 不存在")
        review_id = sample["review_id"]
        items = self._load_items(review_id)
        for item in items:
            if item.record_id == sample["record_id"]:
                item.status = STATUS_CONFIRMED
                item.ack_at = sample["ack_at"]
                item.sample_id = sample["sample_id"]
                item.basis_locked = True
        session = self._load_session(review_id)
        if session is not None:
            self._refresh_pending(session, items)
            self._save_items(review_id, items)
            self._save_session(session)
        return {"sample": sample, "first_ack": first,
                "review": self.trace(review_id)}

    # ---------- 续办 ----------

    def resume_review(self, review_id: str) -> dict[str, Any]:
        """重启后继续本次复盘：只重算待办去向，不重发已交付原行。"""
        session = self._load_session(review_id)
        if session is None:
            raise ValueError(f"复盘 {review_id} 不存在")
        items = self._load_items(review_id)
        self._refresh_pending(session, items)
        self._save_session(session)
        self._append_event("恢复复盘", review_id=review_id,
                           pending=list(session.pending_record_ids),
                           resume_cursor=session.resume_cursor)
        return self.trace(review_id)

    def _refresh_pending(self, session: ReviewSession,
                         items: list[ReviewItem]) -> None:
        pending = [
            item.record_id for item in items
            if item.status not in (STATUS_DISPATCHED, STATUS_CONFIRMED)
        ]
        session.pending_record_ids = pending
        session.resume_cursor = next(
            (index for index, item in enumerate(items)
             if item.record_id in pending),
            len(items),
        )
        session.status = summarize_status(items)

    # ---------- 回查 ----------

    def trace(self, review_id: str) -> dict[str, Any]:
        """按复盘编号回查：会话、复盘项、指纹依据、实际交付原行与样本。"""
        session = self._load_session(review_id)
        if session is None:
            raise ValueError(f"复盘 {review_id} 不存在")
        items = self._load_items(review_id)
        samples = self.sampler.list_samples(review_id)
        return {
            "session": session.to_dict(),
            "items": [item.to_dict() for item in items],
            "samples": samples,
            "mismatches": [
                {"record_id": item.record_id, **item.mismatch}
                for item in items if item.mismatch
            ],
            "deliveries": [
                {
                    "sample_id": sample["sample_id"],
                    "record_id": sample["record_id"],
                    "status": sample["status"],
                    "basis": sample["basis"],
                    "delivered_row": sample["delivered_row"],
                    "dispatched_at": sample["dispatched_at"],
                    "ack_at": sample["ack_at"],
                }
                for sample in samples
            ],
        }

    def list_reviews(self) -> list[dict[str, Any]]:
        return list(self.store.all("reviews").values())

    def audit_events(self) -> list[dict[str, Any]]:
        return self.store.events()


# ---------- 便捷构造：由事件记录生成归档批次 ----------


def archive_batch_from_records(batch_id: str, source_id: str, partition: str,
                               records: list[Any],
                               fingerprint_fields: list[str] | tuple[str, ...],
                               value_source: str = "original_then_masked",
                               ) -> ArchiveBatch:
    """按归档当时字段生成快照；指纹字段选择数据一并冻结在行内。"""
    selection = FingerprintFieldSelection(
        fields=tuple(fingerprint_fields), value_source=value_source)
    rows = [
        ArchiveRow(
            record_id=record.record_id,
            event_type_id=record.event_type.type_id,
            row_index=index,
            fingerprint_fields=selection.fields,
            value_source=selection.value_source,
            parsed_fields=dict(record.parsed_fields),
            masked_fields=dict(record.masked_fields),
        )
        for index, record in enumerate(records)
    ]
    return ArchiveBatch(batch_id=batch_id, source_id=source_id,
                        partition=partition, rows=rows)
