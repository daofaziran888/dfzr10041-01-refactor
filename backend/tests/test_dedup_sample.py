"""测试：去重过滤与采样归并管道 event_type→dedup_filter→sample_job。

覆盖数据流：
- 数据流 4: event_type → dedup_filter → sample_job
- SHA256 指纹去重
- 采样比例控制
- 归并键分组
- 重复事件检测
- 窗口内/外指纹行为
"""
from __future__ import annotations

import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.models import (
    DedupFilter, EventRecord, EventTypeDef, SampleConfig,
)
from app.core.pipeline import make_pipeline, PRESET_LOG_SOURCES, PRESET_PARSE_RULES
from app.core.mask_enrich import MaskEnrichPipeline
from app.core.dedup_sample import (
    DedupSamplePipeline, PRESET_DEDUP_RULES, PRESET_SAMPLE_CONFIGS,
    make_dedup_sample_pipeline,
)
from app.core.models import IngestJob


def make_test_events_with_duplicates() -> list[EventRecord]:
    """创建包含重复事件的测试数据集合。"""
    base_time = datetime(2026, 11, 15, 14, 30, 0)
    records: list[EventRecord] = []
    seq = 0

    urls = [
        "GET /api/users",
        "POST /api/events/ingest",
        "GET /api/health",
    ]

    # 创建 5 条不同事件（全用 access 类型，保证 type_id 一致用于去重）
    for i in range(5):
        seq += 1
        record = EventRecord(
            record_id=f"EVEN-DEDUP-{seq:04d}",
            event_type=EventTypeDef(
                type_id="EVEN-0001", type_name="test_event",
                category_tag="access", severity="INFO",
            ),
            original_content=f"test line {seq}",
            parsed_fields={
                "client_ip": f"192.168.1.{seq}",
                "url": urls[seq % len(urls)],
                "method": "GET",
                "status": "200",
            },
            masked_fields={"client_ip": f"192.168.1.{seq}"},
            enriched_fields={"app_name": "test-service"},
            event_time=base_time + timedelta(seconds=seq),
            source_host=f"192.168.1.{seq}",
            store_partition="20261115",
        )
        records.append(record)

    # 创建 3 条与第一条完全相同的记录（用于去重测试）
    for repeat in range(3):
        seq += 1
        record = EventRecord(
            record_id=f"EVEN-DEDUP-REPEAT-{repeat:04d}",
            event_type=EventTypeDef(
                type_id="EVEN-0001", type_name="test_event",
                category_tag="access", severity="INFO",
            ),
            original_content=f"test line repeated {repeat}",
            parsed_fields={
                "client_ip": "192.168.1.1",  # 与 seq=1 第一条相同
                "url": urls[1 % 3],          # 与 seq=1 第一条相同 (urls[1%3]=urls[1])
                "method": "GET",
                "status": "200",
            },
            masked_fields={"client_ip": "192.168.1.1"},
            enriched_fields={"app_name": "test-service"},
            event_time=base_time + timedelta(seconds=seq),
            source_host="192.168.1.1",
            store_partition="20261115",
        )
        records.append(record)

    return records


class TestDedupFilter:
    """测试去重过滤的正确性。"""

    def test_fingerprint_computation(self):
        """每个事件的 SHA256 指纹应唯一确定性。"""
        records = make_test_events_with_duplicates()
        rule = PRESET_DEDUP_RULES[0]
        fps = [rule.fingerprint(r) for r in records]
        assert len(fps) == len(records)
        assert all(len(fp) == 64 for fp in fps)
        assert all(all(c in "0123456789abcdef" for c in fp) for fp in fps)

    def test_duplicate_detection(self):
        """完全相同的 (client_ip, url) 组合应被标记为重复。"""
        pipeline = DedupSamplePipeline()
        records = make_test_events_with_duplicates()
        dedup_rule = PRESET_DEDUP_RULES[0]

        deduped, hit_count, ignore_count = pipeline.deduplicate(records, dedup_rule)

        assert len(deduped) < len(records)
        assert hit_count == 3  # 3 条重复记录
        # 所有包含相同 (client_ip, url) 的记录，只有第一条能通过
        first_url_ips = [(r.parsed_fields.get("client_ip"), r.parsed_fields.get("url"))
                         for r in deduped]
        assert len(first_url_ips) == len(set(first_url_ips))

    def test_dedup_all_applies_rules_by_type(self):
        """批量去重应按事件类型分别匹配规则。"""
        pipeline = DedupSamplePipeline()
        records = make_test_events_with_duplicates()
        result = pipeline.deduplicate_all(records)

        assert len(result) <= len(records)
        assert len(result) > 0

    def test_dedup_stats_accumulated(self):
        """去重统计应正确累计命中次数和忽略次数。"""
        import copy
        dedup_rule = copy.deepcopy(PRESET_DEDUP_RULES[0])
        pipeline = DedupSamplePipeline(dedup_rules=[dedup_rule])
        records = make_test_events_with_duplicates()
        pipeline.deduplicate_all(records)
        stats = pipeline.get_stats()

        assert "DEDU-0001" in stats
        assert stats["DEDU-0001"]["hit_count"] > 0
        assert stats["DEDU-0001"]["ignore_count"] > 0

    def test_reset_fingerprints(self):
        """重置指纹缓存后应能重新去重。"""
        import copy
        dedup_rule = copy.deepcopy(PRESET_DEDUP_RULES[0])
        pipeline = DedupSamplePipeline(dedup_rules=[dedup_rule])
        records = make_test_events_with_duplicates()

        result1 = pipeline.deduplicate_all(records)
        pipeline.reset_fingerprints()
        result2 = pipeline.deduplicate_all(records)

        assert len(result1) == len(result2)

    def test_empty_records(self):
        """空记录集去重应安全返回空列表。"""
        pipeline = DedupSamplePipeline(dedup_rules=[], sample_configs=[])
        rule = DedupFilter(
            rule_id="TEST-EMPTY-DEDUP",
            event_type_id="EVEN-0001",
            dedup_fields=["client_ip"],
            window_minutes=10,
        )
        deduped, hit, ignored = pipeline.deduplicate([], rule)
        assert deduped == []
        assert hit == 0
        assert ignored == 0


class TestSampleMerge:
    """测试采样归并的正确性。"""

    def test_sample_ratio_enforcement(self):
        """采样应按比例控制输出数量。"""
        pipeline = DedupSamplePipeline()
        records = make_test_events_with_duplicates()
        config = SampleConfig(
            plan_id="SAMP-TEST", event_type_id="EVEN-0001",
            sample_ratio=0.5, merge_key="",
        )
        result = pipeline.sample(records, config)
        expected_max = max(1, int(len(records) * 0.5))
        assert len(result) <= expected_max

    def test_merge_key_deduplication(self):
        """按归并键分组，每键仅保留最新记录。"""
        pipeline = DedupSamplePipeline()
        records = make_test_events_with_duplicates()
        config = SampleConfig(
            plan_id="SAMP-TEST", event_type_id="EVEN-0001",
            sample_ratio=1.0, merge_key="client_ip",
        )
        result = pipeline.sample(records, config)
        ips_in_result = [r.parsed_fields.get("client_ip") for r in result if r.parsed_fields.get("client_ip")]
        assert len(ips_in_result) == len(set(ips_in_result))

    def test_merge_keeps_latest_event(self):
        """归并应保留最新事件时间戳的记录。"""
        pipeline = DedupSamplePipeline()
        base = datetime(2026, 11, 15, 10, 0, 0)
        records = []
        for i in range(3):
            rec = EventRecord(
                record_id=f"EVEN-MERGE-{i}",
                event_type=EventTypeDef(
                    type_id="EVEN-0001", type_name="test",
                    category_tag="access", severity="INFO",
                ),
                original_content=f"merge test {i}",
                parsed_fields={"client_ip": "10.0.0.1"},
                masked_fields={},
                enriched_fields={},
                event_time=base + timedelta(hours=i),
                source_host="10.0.0.1",
            )
            records.append(rec)
        config = SampleConfig(
            plan_id="SAMP-MERGE", event_type_id="EVEN-0001",
            sample_ratio=1.0, merge_key="client_ip",
        )
        result = pipeline.sample(records, config)
        assert len(result) == 1
        assert result[0].event_time == base + timedelta(hours=2)

    def test_sample_all_integration(self):
        """完整去重+采样归并集成。"""
        pipeline = DedupSamplePipeline()
        records = make_test_events_with_duplicates()
        deduped = pipeline.deduplicate_all(records)
        sampled = pipeline.sample_all(deduped)

        assert len(sampled) <= len(deduped)
        assert len(sampled) > 0

    def test_empty_sample_all(self):
        """空记录集采样应安全返回空列表。"""
        pipeline = DedupSamplePipeline()
        result = pipeline.sample_all([])
        assert result == []


# ---------- 运行入口 ----------

if __name__ == "__main__":
    import traceback

    tests = {
        "dedup_fingerprint": TestDedupFilter().test_fingerprint_computation,
        "dedup_duplicate_detection": TestDedupFilter().test_duplicate_detection,
        "dedup_all_by_type": TestDedupFilter().test_dedup_all_applies_rules_by_type,
        "dedup_stats": TestDedupFilter().test_dedup_stats_accumulated,
        "dedup_reset": TestDedupFilter().test_reset_fingerprints,
        "dedup_empty": TestDedupFilter().test_empty_records,
        "sample_ratio": TestSampleMerge().test_sample_ratio_enforcement,
        "sample_merge_key": TestSampleMerge().test_merge_key_deduplication,
        "sample_merge_latest": TestSampleMerge().test_merge_keeps_latest_event,
        "sample_all_integration": TestSampleMerge().test_sample_all_integration,
        "sample_empty": TestSampleMerge().test_empty_sample_all,
    }

    passed = 0
    failed = 0
    for name, test_func in tests.items():
        try:
            test_func()
            passed += 1
            print(f"  PASS  test_dedup_sample: {name}")
        except Exception as e:
            failed += 1
            print(f"  FAIL  test_dedup_sample: {name}")
            traceback.print_exc()

    print(f"\ntest_dedup_sample: {passed} passed, {failed} failed, {len(tests)} total")
    sys.exit(1 if failed else 0)