"""测试：历史复盘与调查采样，含重启、补送凭证、失配隔离与幂等交付。

覆盖数据流：
- 普通去重 dedup_events 与规则去重 deduplicate 对空原字段/脱敏值的差异
- 缺入口凭证 → 待核，不猜来源
- 凭证完整 → 重算归档指纹并核验
- 原行字段被换过 → 隔离失配样本并指出位置
- 样本已派出但复盘未确认时重启 → 补送凭证不会增发、不会重写依据
- 未交付原行在恢复后继续处理
"""
from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.dedup_sample import DedupSamplePipeline
from app.core.fingerprint import (
    ORIGINAL_FIELDS, ORIGINAL_THEN_MASKED, FingerprintFieldSelection,
)
from app.core.models import (
    DedupFilter, EventRecord, EventTypeDef,
)
from app.core.pipeline import IngestPipeline
from app.core.review_models import (
    ArchiveRow, EntryCredential, OriginalRow, STATUS_CONFIRMED, STATUS_DISPATCHED,
    STATUS_MISMATCH, STATUS_PENDING_ENTRY, STATUS_PENDING_ORIGINAL, STATUS_VERIFIED,
    SESSION_AWAITING_ACK, SESSION_BLOCKED, SESSION_DONE, SESSION_READY,
)
from app.core.review_service import (
    SAMPLE_DISPATCHED, InvestigationSampler, ReviewService,
    archive_batch_from_records,
)
from app.core.review_store import ReviewStore


# ---------- 构造辅助 ----------


def make_record(seq: int, client_ip: str = "", url: str = "",
                masked_ip: str = "", masked_url: str = "") -> EventRecord:
    """构造一条原始字段可为空、脱敏值可独立设置的事件记录。"""
    return EventRecord(
        record_id=f"EVEN-{seq:04d}",
        event_type=EventTypeDef(
            type_id="EVEN-0001", type_name="访问事件",
            category_tag="access", severity="INFO",
        ),
        original_content=f"line-{seq}",
        parsed_fields={"client_ip": client_ip, "url": url},
        masked_fields={"client_ip": masked_ip, "url": masked_url},
        enriched_fields={},
        event_time=datetime(2026, 11, 1, 10, 0, seq),
        source_host="host-a",
        store_partition="20261101",
    )


def make_hot_rows(records: list[EventRecord]) -> list[OriginalRow]:
    """把事件记录登记成“当前实际原行”（默认与归档一致）。"""
    return [
        OriginalRow(
            record_id=record.record_id,
            event_type_id=record.event_type.type_id,
            parsed_fields=dict(record.parsed_fields),
            masked_fields=dict(record.masked_fields),
            content=record.original_content,
            location="hot/20261101",
        )
        for record in records
    ]


def new_service(tmpdir: str, file_name: str = "state.json") -> tuple[ReviewService, ReviewStore]:
    store = ReviewStore(os.path.join(tmpdir, file_name))
    return ReviewService(store), store


def build_archive(batch_id: str, records: list[EventRecord],
                  fields: list[str] | None = None):
    return archive_batch_from_records(
        batch_id, "LOG_-0001", "20261101", records,
        fields or ["client_ip", "url"],
    )


# ---------- 一、两处旧调用：指纹字段选择差异 ----------


class TestLegacyDedupCalls:
    """普通去重与规则去重共用字段选择数据，但保留各自的既有行为。"""

    def test_ordinary_and_rule_dedup_differ_with_empty_original_fields(self):
        """两个原请求字段都空、脱敏值 x/y：普通去重留一行，规则去重留两行。"""
        records = [
            make_record(1, client_ip="", url="", masked_ip="x", masked_url="x"),
            make_record(2, client_ip="", url="", masked_ip="y", masked_url="y"),
        ]

        ordinary = IngestPipeline().dedup_events(records, ["client_ip", "url"])
        assert [row.record_id for row in ordinary] == ["EVEN-0001"]

        rule = DedupFilter(rule_id="DEDU-TEST", event_type_id="EVEN-0001",
                           dedup_fields=["client_ip", "url"])
        pipeline = DedupSamplePipeline(dedup_rules=[rule])
        deduped, hit, ignored = pipeline.deduplicate(records, rule)
        assert [row.record_id for row in deduped] == ["EVEN-0001", "EVEN-0002"]
        assert (hit, ignored) == (0, 0)
        assert pipeline.get_stats()["DEDU-TEST"] == {"hit_count": 0, "ignore_count": 0}

    def test_rule_dedup_keeps_order_and_hit_stats(self):
        """规则去重命中后次序与命中统计保持原语义。"""
        records = [
            make_record(1, "10.0.0.1", "/a"),
            make_record(2, "10.0.0.2", "/b"),
            make_record(3, "10.0.0.1", "/a"),  # 与第 1 条重复
            make_record(4, "10.0.0.3", "/c"),
        ]
        rule = DedupFilter(rule_id="DEDU-ORDER", event_type_id="EVEN-0001",
                           dedup_fields=["client_ip", "url"])
        pipeline = DedupSamplePipeline(dedup_rules=[rule])
        deduped, hit, ignored = pipeline.deduplicate(records, rule)
        assert [row.record_id for row in deduped] == ["EVEN-0001", "EVEN-0002", "EVEN-0004"]
        assert hit == 1
        assert ignored == 1
        assert rule.hit_count == 1 and rule.ignore_count == 1

    def test_fingerprint_matches_legacy_formula(self):
        """共用选择的指纹与重构前手工拼接的结果一致。"""
        import hashlib
        record = make_record(1, client_ip="10.0.0.1", url="/a")
        legacy = hashlib.sha256(
            "|".join(["EVEN-0001"] + [f"{f}={record.parsed_fields[f]}"
                                      for f in sorted(["client_ip", "url"])]).encode()
        ).hexdigest()
        ordinary = FingerprintFieldSelection.ordinary(["client_ip", "url"])
        assert ordinary.fingerprint(record) == legacy
        assert ordinary.value_source == ORIGINAL_FIELDS

    def test_selection_falls_back_to_masked_only_for_rule_mode(self):
        """只有规则模式回退脱敏值；普通模式缺字段按空串。"""
        record = make_record(1, client_ip="", url="/a", masked_ip="1.1.*.*")
        ordinary = FingerprintFieldSelection.ordinary(["client_ip"])
        rule = FingerprintFieldSelection.rule(["client_ip"])
        assert ordinary.value_for(record, "client_ip") == ""
        assert rule.value_for(record, "client_ip") == "1.1.*.*"
        assert rule.value_source == ORIGINAL_THEN_MASKED


# ---------- 二、缺少入口凭证 ----------


class TestMissingCredential:
    def test_review_stays_pending_without_credential(self):
        """没有入口凭证时全部条目待核，不产生任何样本。"""
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = new_service(tmp)
            records = [make_record(1, "10.0.0.1", "/a"), make_record(2, "10.0.0.2", "/b")]
            for row in make_hot_rows(records):
                service.set_original_row(row)
            service.register_archive(build_archive("ARCH-NOCRED", records))

            trace = service.start_review("ARCH-NOCRED")
            assert trace["session"]["status"] == "待核"
            assert all(item["status"] == STATUS_PENDING_ENTRY for item in trace["items"])
            assert all(not item["basis"] for item in trace["items"])
            assert trace["samples"] == []

            delivered = service.deliver_verified("RVW-ARCH-NOCRED")
            assert delivered["samples"] == []

    def test_incomplete_credential_is_pending(self):
        """凭证没有行清单视为入口证明不完整，保持待核。"""
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = new_service(tmp)
            records = [make_record(1, "10.0.0.1", "/a")]
            for row in make_hot_rows(records):
                service.set_original_row(row)
            batch = build_archive("ARCH-PART", records)
            service.register_archive(batch)
            service.start_review("ARCH-PART")
            partial = EntryCredential.from_batch(
                batch, credential_id="CRED-PART", row_checksums={})
            trace = service.submit_credential("RVW-ARCH-PART", partial)
            assert trace["session"]["status"] == "待核"
            assert trace["items"][0]["status"] == STATUS_PENDING_ENTRY
            assert "入口证明不完整" in trace["items"][0]["note"]
            assert trace["samples"] == []

    def test_credential_never_guesses_source(self):
        """凭证来源与归档不一致时直接待核，不按名字猜来源。"""
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = new_service(tmp)
            records = [make_record(1, "10.0.0.1", "/a")]
            for row in make_hot_rows(records):
                service.set_original_row(row)
            batch = build_archive("ARCH-SRC", records)
            service.register_archive(batch)
            service.start_review("ARCH-SRC")
            wrong = EntryCredential.from_batch(batch, credential_id="CRED-WRONG")
            wrong = EntryCredential(
                credential_id=wrong.credential_id, batch_id=wrong.batch_id,
                source_id="LOG_-9999", partition=wrong.partition,
                row_checksums=wrong.row_checksums,
            )
            trace = service.submit_credential("RVW-ARCH-SRC", wrong)
            item = trace["items"][0]
            assert item["status"] == STATUS_PENDING_ENTRY
            assert item["basis"] == {}
            assert "来源" in item["note"]


# ---------- 三、凭证完整 / 原行被换 ----------


class TestVerifiedAndMismatch:
    def test_full_credential_recomputes_archive_fingerprint(self):
        """凭证完整时按归档当时字段重算指纹，并与记录指纹一致。"""
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = new_service(tmp)
            records = [make_record(1, "10.0.0.1", "/a", masked_ip="10.0.*.*")]
            for row in make_hot_rows(records):
                service.set_original_row(row)
            batch = build_archive("ARCH-OK", records)
            service.register_archive(batch)
            service.start_review("ARCH-OK")
            credential = EntryCredential.from_batch(batch, credential_id="CRED-OK")
            trace = service.submit_credential("RVW-ARCH-OK", credential)

            item = trace["items"][0]
            assert item["status"] == STATUS_VERIFIED
            assert item["recorded_fingerprint"] == item["recomputed_fingerprint"]
            assert item["basis"]["value_source"] == ORIGINAL_THEN_MASKED
            assert item["basis"]["fingerprint_fields"] == ["client_ip", "url"]
            assert item["basis"]["credential_id"] == "CRED-OK"
            assert trace["session"]["status"] == SESSION_READY

    def test_swapped_original_field_is_isolated_with_location(self):
        """当前原行被换过：隔离失配样本并指出字段与字段序号。"""
        with tempfile.TemporaryDirectory() as tmp:
            service, _ = new_service(tmp)
            records = [
                make_record(1, "10.0.0.1", "/a"),
                make_record(2, "10.0.0.2", "/b"),
                make_record(3, "10.0.0.3", "/c"),
            ]
            for row in make_hot_rows(records):
                service.set_original_row(row)
            batch = build_archive("ARCH-MIS", records)
            service.register_archive(batch)
            service.start_review("ARCH-MIS")
            # 第 2 条当前原行的 url 被换掉
            service.set_original_row(OriginalRow(
                record_id="EVEN-0002", event_type_id="EVEN-0001",
                parsed_fields={"client_ip": "10.0.0.2", "url": "/b-swapped"},
                masked_fields={"client_ip": "10.0.0.2", "url": ""},
                content="line-2-mutated", location="hot/20261101",
            ))
            credential = EntryCredential.from_batch(batch, credential_id="CRED-MIS")
            trace = service.submit_credential("RVW-ARCH-MIS", credential)

            statuses = {item["record_id"]: item["status"] for item in trace["items"]}
            assert statuses["EVEN-0002"] == STATUS_MISMATCH
            assert statuses["EVEN-0001"] == STATUS_VERIFIED
            assert trace["session"]["status"] == SESSION_BLOCKED

            mismatch = trace["mismatches"][0]
            assert mismatch["record_id"] == "EVEN-0002"
            assert mismatch["row_index"] == 1
            assert mismatch["locations"][0]["field"] == "url"
            assert mismatch["locations"][0]["field_index"] == 1
            assert mismatch["locations"][0]["archived_value"] == "/b"
            assert mismatch["locations"][0]["current_value"] == "/b-swapped"

            delivered = service.deliver_verified("RVW-ARCH-MIS")
            delivered_ids = {sample["record_id"] for sample in delivered["samples"]}
            assert "EVEN-0002" not in delivered_ids
            assert delivered_ids == {"EVEN-0001", "EVEN-0003"}
            assert delivered["mismatches"][0]["locations"][0]["field"] == "url"


# ---------- 四、已派出未确认时重启并补送凭证 ----------


class TestRestartAndCorrection:
    def _prepare(self, tmp: str):
        records = [
            make_record(seq, f"10.0.0.{seq}", f"/p{seq}") for seq in range(1, 5)
        ]
        return records

    def test_restart_before_ack_keeps_samples_and_basis(self):
        """样本已派出、复盘端未确认就重启：补送凭证不增发、不重写依据。"""
        with tempfile.TemporaryDirectory() as tmp:
            records = self._prepare(tmp)
            service, _ = new_service(tmp)
            for row in make_hot_rows(records):
                service.set_original_row(row)
            batch = build_archive("ARCH-RST", records)
            service.register_archive(batch)
            service.start_review("ARCH-RST")
            first = EntryCredential.from_batch(batch, credential_id="CRED-RST", revision=1)
            service.submit_credential("RVW-ARCH-RST", first)
            dispatched = service.deliver_verified("RVW-ARCH-RST")
            assert dispatched["session"]["status"] == SESSION_AWAITING_ACK
            original_samples = {
                sample["sample_id"]: dict(sample) for sample in dispatched["samples"]
            }
            assert len(original_samples) == 4

            # 进程中断：重新按落盘状态构造服务（模拟重启）
            restarted, _ = new_service(tmp)
            resumed = restarted.resume_review("RVW-ARCH-RST")
            assert [s["sample_id"] for s in resumed["samples"]] == list(original_samples)
            assert resumed["session"]["pending_record_ids"] == []
            assert resumed["session"]["status"] == SESSION_AWAITING_ACK
            # 已派出的原行不会再次发出
            again = restarted.deliver_verified("RVW-ARCH-RST")
            assert len(again["samples"]) == 4

            # 重启后补来入口更正凭证
            corrected = EntryCredential.from_batch(
                batch, credential_id="CRED-RST", revision=2,
                note="更正入口说明", issued_by="值班员",
            )
            after = restarted.submit_credential("RVW-ARCH-RST", corrected, correction=True)
            assert len(after["samples"]) == 4
            for sample in after["samples"]:
                assert sample["basis"] == original_samples[sample["sample_id"]]["basis"]
                assert sample["basis_locked"] is True
            assert all(item["basis_locked"] for item in after["items"])
            # 依据没有被新版凭证覆盖
            assert all(item["basis"]["credential_revision"] == 1
                       for item in after["items"])

            # 确认到达后状态收口
            for sample in after["samples"]:
                restarted.acknowledge_sample(sample["sample_id"])
            final = restarted.trace("RVW-ARCH-RST")
            assert final["session"]["status"] == SESSION_DONE

    def test_undelivered_rows_continue_after_restart_and_correction(self):
        """上次只覆盖部分行：重启后补凭证，未交付原行继续交付，已发原行不增发。"""
        with tempfile.TemporaryDirectory() as tmp:
            records = self._prepare(tmp)
            service, _ = new_service(tmp)
            for row in make_hot_rows(records):
                service.set_original_row(row)
            batch = build_archive("ARCH-PART2", records)
            service.register_archive(batch)
            service.start_review("ARCH-PART2")

            rows = batch.ordered_rows()
            partial_checksums = {row.record_id: row.checksum() for row in rows[:3]}
            partial = EntryCredential.from_batch(
                batch, credential_id="CRED-P2", revision=1,
                row_checksums=partial_checksums,
            )
            service.submit_credential("RVW-ARCH-PART2", partial)
            first_delivery = service.deliver_verified("RVW-ARCH-PART2")
            first_ids = [s["sample_id"] for s in first_delivery["samples"]]
            assert len(first_ids) == 3
            pending = first_delivery["session"]["pending_record_ids"]
            assert pending == ["EVEN-0004"]

            # 重启，补送覆盖全部行的更正凭证
            restarted, _ = new_service(tmp)
            resumed = restarted.resume_review("RVW-ARCH-PART2")
            assert resumed["session"]["pending_record_ids"] == ["EVEN-0004"]
            assert resumed["session"]["status"] == SESSION_AWAITING_ACK

            complete = EntryCredential.from_batch(
                batch, credential_id="CRED-P2", revision=2,
                row_checksums={row.record_id: row.checksum() for row in rows},
            )
            restarted.submit_credential("RVW-ARCH-PART2", complete, correction=True)
            trace = restarted.trace("RVW-ARCH-PART2")
            assert len(trace["samples"]) == 3  # 更正不增发
            statuses = {item["record_id"]: item["status"] for item in trace["items"]}
            assert statuses["EVEN-0004"] == STATUS_VERIFIED
            assert all(statuses[rid] == STATUS_DISPATCHED for rid in ("EVEN-0001", "EVEN-0002", "EVEN-0003"))

            second = restarted.deliver_verified("RVW-ARCH-PART2")
            assert [s["sample_id"] for s in second["samples"]][:3] == first_ids
            assert len(second["samples"]) == 4
            new_sample = second["samples"][3]
            assert new_sample["record_id"] == "EVEN-0004"
            assert new_sample["status"] == SAMPLE_DISPATCHED
            assert second["session"]["pending_record_ids"] == []

            # 幂等：再次交付不长出第 5 个样本
            third = restarted.deliver_verified("RVW-ARCH-PART2")
            assert len(third["samples"]) == 4

    def test_delivered_row_is_traceable_after_restart(self):
        """复盘编号、指纹依据与实际交付原行在重启后可按编号回查。"""
        with tempfile.TemporaryDirectory() as tmp:
            records = self._prepare(tmp)[:2]
            service, _ = new_service(tmp)
            for row in make_hot_rows(records):
                service.set_original_row(row)
            batch = build_archive("ARCH-TRACE", records)
            service.register_archive(batch)
            service.start_review("ARCH-TRACE")
            service.submit_credential(
                "RVW-ARCH-TRACE",
                EntryCredential.from_batch(batch, credential_id="CRED-T"))
            service.deliver_verified("RVW-ARCH-TRACE")

            restarted, _ = new_service(tmp)
            trace = restarted.trace("RVW-ARCH-TRACE")
            assert trace["session"]["credential_id"] == "CRED-T"
            for delivery in trace["deliveries"]:
                assert delivery["delivered_row"]["record_id"] == delivery["record_id"]
                assert delivery["basis"]["fingerprint_fields"] == ["client_ip", "url"]
                assert delivery["basis"]["archive_fingerprint"]
                assert delivery["delivered_row"]["parsed_fields"]

            events = restarted.audit_events()
            assert any(event["action"] == "复盘交付调查采样" for event in events)

    def test_missing_original_row_stays_recoverable(self):
        """凭证有效但当前原行缺失：保留“待补原行”，不交付也不丢去向。"""
        with tempfile.TemporaryDirectory() as tmp:
            records = [make_record(1, "10.0.0.1", "/a"), make_record(2, "10.0.0.2", "/b")]
            service, _ = new_service(tmp)
            service.set_original_row(make_hot_rows(records)[0])
            batch = build_archive("ARCH-MISSROW", records)
            service.register_archive(batch)
            service.start_review("ARCH-MISSROW")
            trace = service.submit_credential(
                "RVW-ARCH-MISSROW",
                EntryCredential.from_batch(batch, credential_id="CRED-MR"))
            statuses = {item["record_id"]: item["status"] for item in trace["items"]}
            assert statuses["EVEN-0002"] == STATUS_PENDING_ORIGINAL
            # 未交付的条目（含已核验未交付的 EVEN-0001）都还算待办
            assert trace["session"]["pending_record_ids"] == ["EVEN-0001", "EVEN-0002"]
            delivered = service.deliver_verified("RVW-ARCH-MISSROW")
            assert [s["record_id"] for s in delivered["samples"]] == ["EVEN-0001"]
            # 交付后待办只剩缺原行的那条，去向明确
            assert delivered["session"]["pending_record_ids"] == ["EVEN-0002"]
            # 补上原行后，剩余条目可继续处理
            service.set_original_row(make_hot_rows(records)[1])
            resumed = service.resume_review("RVW-ARCH-MISSROW")
            assert resumed["session"]["pending_record_ids"] == ["EVEN-0002"]
            resumed = service.submit_credential(
                "RVW-ARCH-MISSROW",
                EntryCredential.from_batch(batch, credential_id="CRED-MR", revision=2),
                correction=True)
            resumed = service.deliver_verified("RVW-ARCH-MISSROW")
            assert len(resumed["samples"]) == 2

    def test_mismatch_archive_row_checksum_is_not_trusted(self):
        """凭证行校验和与归档不符时，该行保留待核，不拿猜测补齐。"""
        with tempfile.TemporaryDirectory() as tmp:
            records = [make_record(1, "10.0.0.1", "/a")]
            service, _ = new_service(tmp)
            for row in make_hot_rows(records):
                service.set_original_row(row)
            batch = build_archive("ARCH-BADSUM", records)
            service.register_archive(batch)
            service.start_review("ARCH-BADSUM")
            tampered = EntryCredential.from_batch(
                batch, credential_id="CRED-BADSUM",
                row_checksums={"EVEN-0001": "0" * 64})
            trace = service.submit_credential("RVW-ARCH-BADSUM", tampered)
            assert trace["items"][0]["status"] == STATUS_PENDING_ENTRY
            assert "校验和不符" in trace["items"][0]["note"]
            assert trace["samples"] == []


# ---------- 五、归档指纹与归档行自身一致性 ----------


class TestArchiveFingerprintBasis:
    def test_archive_row_freezes_selection(self):
        """归档行冻结当时的指纹字段选择数据，重算结果稳定。"""
        record = make_record(1, "10.0.0.1", "/a", masked_ip="10.0.*.*")
        batch = archive_batch_from_records(
            "ARCH-FREEZE", "LOG_-0001", "20261101", [record],
            ["url", "client_ip"])
        row = batch.rows[0]
        assert row.fingerprint_fields == ("client_ip", "url")
        assert row.value_source == ORIGINAL_THEN_MASKED
        assert row.computed_fingerprint() == row.fingerprint
        # 归档指纹来自归档快照，当前原行怎么换都不影响归档指纹重算
        mutated = OriginalRow(record_id="EVEN-0001", event_type_id="EVEN-0001",
                              parsed_fields={"client_ip": "9.9.9.9", "url": "/z"})
        assert row.computed_fingerprint() == row.fingerprint
        assert row.fingerprint != row.selection().fingerprint_from_fields(
            "EVEN-0001", mutated.parsed_fields)

    def test_archive_row_roundtrip(self):
        """归档行序列化后可原样恢复并复算出同一指纹。"""
        record = make_record(1, "10.0.0.1", "/a")
        batch = build_archive("ARCH-RT", [record])
        payload = batch.to_dict()
        restored = type(batch).from_dict(payload)
        assert restored.manifest_checksum() == batch.manifest_checksum()
        assert restored.rows[0].fingerprint == batch.rows[0].fingerprint


class TestEndToEndDedupReviewSampling:
    """真实去重输出 → 归档 → 复盘核验 → 调查采样全链路。"""

    def _deduped_records(self) -> list[EventRecord]:
        records = [
            make_record(1, "10.0.0.1", "/a"),
            make_record(2, "10.0.0.2", "/b"),
            make_record(3, "10.0.0.1", "/a"),  # 规则去重会命中的重复行
            make_record(4, "10.0.0.3", "/c"),
            make_record(5, "", "", masked_ip="x", masked_url="x"),
            make_record(6, "", "", masked_ip="y", masked_url="y"),
        ]
        rule = DedupFilter(rule_id="DEDU-E2E", event_type_id="EVEN-0001",
                           dedup_fields=["client_ip", "url"])
        pipeline = DedupSamplePipeline(dedup_rules=[rule])
        deduped, hit, ignored = pipeline.deduplicate(records, rule)
        assert hit == 1 and ignored == 1
        return deduped

    def test_dedup_output_flows_into_archive_and_samples(self):
        """去重后的实际原行进入归档、复盘与采样，样本数等于已核验原行数。"""
        with tempfile.TemporaryDirectory() as tmp:
            store = ReviewStore(os.path.join(tmp, "state.json"))
            sampler = InvestigationSampler(store)
            service = ReviewService(store, sampler)

            deduped = self._deduped_records()
            for row in make_hot_rows(deduped):
                service.set_original_row(row)
            batch = build_archive("ARCH-E2E", deduped)
            service.register_archive(batch)
            service.start_review("ARCH-E2E")
            service.submit_credential(
                "RVW-ARCH-E2E",
                EntryCredential.from_batch(batch, credential_id="CRED-E2E"))
            trace = service.deliver_verified("RVW-ARCH-E2E")

            assert len(trace["samples"]) == len(deduped) == 5
            assert [item["status"] for item in trace["items"]] == [STATUS_DISPATCHED] * 5
            for sample in trace["samples"]:
                # 采样端确实经既有采样管道产出样本，且样本指向交付的实际原行
                assert sample["sampled_record_id"] == sample["record_id"]
                assert sample["delivered_row"]["record_id"] == sample["record_id"]
                assert sample["basis"]["archive_fingerprint"]

            # 重启后仍能按复盘编号查出相同的样本集合
            restarted = ReviewService(ReviewStore(os.path.join(tmp, "state.json")),
                                      InvestigationSampler(store))
            again = restarted.trace("RVW-ARCH-E2E")
            assert [s["sample_id"] for s in again["samples"]] == [
                s["sample_id"] for s in trace["samples"]]

    def test_investigation_sampler_uses_sample_job_pipeline(self):
        """调查采样按既有采样管道生成样本，便于与 sample_job 统计对齐。"""
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = DedupSamplePipeline(dedup_rules=[], sample_configs=[])
            sampler = InvestigationSampler(ReviewStore(os.path.join(tmp, "s")))
            sampler.pipeline = pipeline
            item = {"record_id": "EVEN-0001", "basis": {"archive_batch_id": "ARCH-X"}}
            row = make_hot_rows([make_record(1, "10.0.0.1", "/a")])[0]
            sample, created = sampler.receive("RVW-X", item, row)
            assert created is True
            assert sample["sampled_record_id"] == "EVEN-0001"
            assert sample["basis_locked"] is False


class TestIncidentRunbook:
    """按值班场景串起来：真实去重 → 三类归档 → 中断重启 → 补凭证 → 重复确认。"""

    def test_full_incident_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_path = os.path.join(tmp, "state.json")

            # 1) 真实规则去重：留下 4 条（含两条原字段空、脱敏值不同的行）
            raw = [
                make_record(1, "10.0.0.1", "/a"),
                make_record(2, "10.0.0.2", "/b"),
                make_record(3, "10.0.0.1", "/a"),          # 重复
                make_record(4, "10.0.0.3", "/c"),
                make_record(5, "", "", masked_ip="x", masked_url="x"),
                make_record(6, "", "", masked_ip="y", masked_url="y"),
            ]
            rule = DedupFilter(rule_id="DEDU-RUN", event_type_id="EVEN-0001",
                               dedup_fields=["client_ip", "url"])
            dedup_pipeline = DedupSamplePipeline(dedup_rules=[rule])
            deduped, hit, ignored = dedup_pipeline.deduplicate(raw, rule)
            assert (hit, ignored) == (1, 1)
            assert [r.record_id for r in deduped] == [
                "EVEN-0001", "EVEN-0002", "EVEN-0004", "EVEN-0005", "EVEN-0006"]

            store = ReviewStore(state_path)
            service = ReviewService(store, InvestigationSampler(store))
            for row in make_hot_rows(deduped):
                service.set_original_row(row)

            # 2a) 缺入口凭证的归档
            missing_batch = build_archive("ARCH-RUN-MISS", deduped[:1])
            service.register_archive(missing_batch)
            missing_trace = service.start_review("ARCH-RUN-MISS")
            assert missing_trace["session"]["status"] == "待核"
            assert missing_trace["samples"] == []

            # 2b) 凭证完整 + 2c) 原行字段被换过，同一批次里各一条
            batch = build_archive("ARCH-RUN", deduped)
            service.register_archive(batch)
            service.set_original_row(OriginalRow(
                record_id="EVEN-0004", event_type_id="EVEN-0001",
                parsed_fields={"client_ip": "10.0.0.3", "url": "/c-changed"},
                masked_fields={"client_ip": "10.0.0.3", "url": ""},
                content="line-4", location="hot/20261101",
            ))
            rows = batch.ordered_rows()
            # 首次凭证只覆盖前三条（模拟入口凭证不全）
            partial_checksums = {row.record_id: row.checksum() for row in rows[:3]}
            service.start_review("ARCH-RUN")
            service.submit_credential(
                "RVW-ARCH-RUN",
                EntryCredential.from_batch(batch, credential_id="CRED-RUN",
                                           revision=1, row_checksums=partial_checksums))

            first = service.deliver_verified("RVW-ARCH-RUN")
            first_sample_ids = [sample["sample_id"] for sample in first["samples"]]
            assert len(first_sample_ids) == 2          # EVEN-0001/0002
            assert {s["record_id"] for s in first["samples"]} == {"EVEN-0001", "EVEN-0002"}
            # 失配的 EVEN-0004 与凭证未覆盖的 0005/0006 都还在待办里
            assert first["session"]["pending_record_ids"] == [
                "EVEN-0004", "EVEN-0005", "EVEN-0006"]
            assert [m["record_id"] for m in first["mismatches"]] == ["EVEN-0004"]

            # 3) 样本已派出、复盘确认未达时进程中断
            del service

            restarted = ReviewService(ReviewStore(state_path),
                                      InvestigationSampler(ReviewStore(state_path)))
            resumed = restarted.resume_review("RVW-ARCH-RUN")
            assert [s["sample_id"] for s in resumed["samples"]] == first_sample_ids
            assert resumed["session"]["status"] == SESSION_AWAITING_ACK
            assert resumed["session"]["pending_record_ids"] == [
                "EVEN-0004", "EVEN-0005", "EVEN-0006"]
            assert resumed["mismatches"][0]["record_id"] == "EVEN-0004"
            assert resumed["mismatches"][0]["locations"][0]["field"] == "url"

            # 4) 重启后补来入口更正凭证（覆盖全部行）
            corrected = EntryCredential.from_batch(
                batch, credential_id="CRED-RUN", revision=2, issued_by="值班员",
                row_checksums={row.record_id: row.checksum() for row in rows})
            after = restarted.submit_credential("RVW-ARCH-RUN", corrected, correction=True)
            assert [s["sample_id"] for s in after["samples"]] == first_sample_ids
            # 已派出样本的依据没有被新版凭证改写
            for sample in after["samples"]:
                assert sample["basis_locked"] is True
                assert sample["basis"]["credential_revision"] == 1
            # 新覆盖的 0005/0006 变为已核验；失配的 0004 仍是失配，位置可查
            statuses = {item["record_id"]: item["status"] for item in after["items"]}
            assert statuses["EVEN-0005"] == STATUS_VERIFIED
            assert statuses["EVEN-0006"] == STATUS_VERIFIED
            assert statuses["EVEN-0004"] == STATUS_MISMATCH
            assert after["mismatches"][0]["locations"][0]["current_value"] == "/c-changed"

            # 5) 重复确认：同一批样本确认两次不增发
            for sample in after["samples"]:
                first_ack = restarted.acknowledge_sample(sample["sample_id"])
                assert first_ack["first_ack"] is True
                second_ack = restarted.acknowledge_sample(sample["sample_id"])
                assert second_ack["first_ack"] is False
                assert len(second_ack["review"]["samples"]) == 2

            # 6) 未交付原行继续处理，且不重复派出已发原行
            remaining = restarted.deliver_verified("RVW-ARCH-RUN")
            assert [s["sample_id"] for s in remaining["samples"]][:2] == first_sample_ids
            assert {s["record_id"] for s in remaining["samples"]} == {
                "EVEN-0001", "EVEN-0002", "EVEN-0005", "EVEN-0006"}
            # 失配的 EVEN-0004 不会进入采样，去向仍明确可查
            assert remaining["session"]["pending_record_ids"] == ["EVEN-0004"]
            assert remaining["mismatches"][0]["locations"][0]["field"] == "url"

            # 7) 复查：按编号回查，样本与依据都在
            final = ReviewService(ReviewStore(state_path),
                                  InvestigationSampler(ReviewStore(state_path))
                                  ).trace("RVW-ARCH-RUN")
            assert len(final["samples"]) == 4
            assert final["session"]["credential_revision"] == 2
            assert all(sample["delivered_row"]["record_id"] == sample["record_id"]
                       for sample in final["samples"])


class TestSharedStateFile:
    """同一状态文件被多个仓库实例打开时，写入不能互相覆盖。"""

    def test_two_stores_on_same_file_do_not_clobber(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "state.json")
            records = [make_record(1, "10.0.0.1", "/a"), make_record(2, "10.0.0.2", "/b")]
            first_store = ReviewStore(path)
            service = ReviewService(first_store, InvestigationSampler(first_store))
            for row in make_hot_rows(records):
                service.set_original_row(row)
            batch = build_archive("ARCH-SHARED", records)
            service.register_archive(batch)
            service.start_review("ARCH-SHARED")
            service.submit_credential(
                "RVW-ARCH-SHARED",
                EntryCredential.from_batch(batch, credential_id="CRED-SH"))

            # 另一个实例在同样路径上打开并派发样本
            second_store = ReviewStore(path)
            second = ReviewService(second_store, InvestigationSampler(second_store))
            second.deliver_verified("RVW-ARCH-SHARED")
            sample_ids = [s["sample_id"] for s in second.trace("RVW-ARCH-SHARED")["samples"]]
            assert len(sample_ids) == 2

            # 第一个实例继续补凭证：不能把第二个实例写入的样本覆盖掉
            correction = EntryCredential.from_batch(
                batch, credential_id="CRED-SH", revision=2, note="补送")
            first_view = service.submit_credential(
                "RVW-ARCH-SHARED", correction, correction=True)
            assert len(first_view["samples"]) == 2
            assert [s["sample_id"] for s in first_view["samples"]] == sample_ids
            # 会话、条目、归档都在
            assert first_view["session"]["credential_revision"] == 2
            assert len(first_view["items"]) == 2


if __name__ == "__main__":
    import traceback

    tests = {}
    for cls in (TestLegacyDedupCalls, TestMissingCredential, TestVerifiedAndMismatch,
                TestRestartAndCorrection, TestArchiveFingerprintBasis,
                TestEndToEndDedupReviewSampling, TestIncidentRunbook,
                TestSharedStateFile):
        instance = cls()
        for name in dir(instance):
            if name.startswith("test_"):
                tests[f"{cls.__name__}.{name}"] = getattr(instance, name)

    passed = failed = 0
    for name, func in tests.items():
        try:
            func()
            passed += 1
            print(f"  PASS  test_review_investigation: {name}")
        except Exception:
            failed += 1
            print(f"  FAIL  test_review_investigation: {name}")
            traceback.print_exc()
    print(f"\ntest_review_investigation: {passed} passed, {failed} failed, {len(tests)} total")
    sys.exit(1 if failed else 0)
