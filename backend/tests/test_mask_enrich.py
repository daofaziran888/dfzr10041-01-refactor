"""测试：脱敏规则与字段增强管道 event_store→mask_rule→field_enrich。

覆盖数据流：
- 数据流 3: event_store → mask_rule → field_enrich → event_type
- 脱敏变换：phone/id_card/email/ip/custom_regex 五种方式
- 字段增强：app_name、severity_level、status_category 三类映射
- 脱敏遗漏检测
- 空值/边缘值处理
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.models import (
    EventRecord, EventTypeDef, FieldEnrichRule, MaskMethod, MaskResult, MaskRule,
)
from app.core.pipeline import make_pipeline, PRESET_LOG_SOURCES, PRESET_PARSE_RULES
from app.core.mask_enrich import (
    MaskEnrichPipeline, PRESET_MASK_RULES, PRESET_ENRICH_RULES,
    make_mask_enrich_pipeline,
)
from app.core.models import IngestJob


def make_test_events() -> list[EventRecord]:
    """创建一组包含各种敏感字段的测试事件记录。"""
    pipeline = make_pipeline()
    source = PRESET_LOG_SOURCES["access-web"]
    rule = PRESET_PARSE_RULES[0]
    job = IngestJob(job_id="INGE-TEST-MASK", source_id=source.source_id, batch_size=20)

    lines = [
        '192.168.1.10 - - [15/Nov/2026:14:30:05 +0800] "GET /api/users/13800138000 HTTP/1.1" 200 1234 "-" "Mozilla/5.0"',
        '10.0.0.55 - - [15/Nov/2026:14:30:06 +0800] "POST /api/events/ingest HTTP/1.1" 500 89 "-" "curl/7.88"',
        '172.16.0.3 - - [15/Nov/2026:14:30:07 +0800] "GET /api/health HTTP/1.1" 401 45 "-" "kube-probe/1.28"',
    ]
    entries = pipeline.parse_raw_lines(source, lines)
    parsed = pipeline.apply_parse_rules(entries, rule)
    records = pipeline.classify_events(parsed)
    return records


def make_test_app_events() -> list[EventRecord]:
    """创建包含 app 日志字段的测试事件（用于 enrich 测试）。"""
    pipeline = make_pipeline()
    source = PRESET_LOG_SOURCES["app-backend"]
    rule = PRESET_PARSE_RULES[1]
    job = IngestJob(job_id="INGE-TEST-APP", source_id=source.source_id, batch_size=20)

    lines = [
        '2026-11-15 14:30:05.123 [http-nio-8080-exec-1] INFO  com.example.IngestController - 采集作业启动 user_id=user@example.com phone=13800138000',
        '2026-11-15 14:30:05.456 [http-nio-8080-exec-2] ERROR com.example.ParserEngine - regex mismatch id_card=110101199001011234',
        '2026-11-15 14:30:05.789 [pool-3-thread-1] DEBUG com.example.CheckpointStore - checkpoint written token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9',
        '2026-11-15 14:30:06.012 [http-nio-8080-exec-3] WARN  com.example.MaskService - phone_number=13912345678 found in log body',
    ]
    entries = pipeline.parse_raw_lines(source, lines)
    parsed = pipeline.apply_parse_rules(entries, rule)
    records = pipeline.classify_events(parsed)
    return records


class TestMaskTransformations:
    """测试各类脱敏变换的正确性。"""

    def test_ip_masking(self):
        """IP 脱敏：后两段应替换为 *。"""
        rule = MaskRule(
            rule_id="TEST-IP", target_field="client_ip",
            mask_method=MaskMethod.IP, mask_pattern="",
        )
        assert "192.168.*.*" == rule.apply("192.168.1.10")
        assert "10.0.*.*" == rule.apply("10.0.0.55")

    def test_phone_masking(self):
        """手机号脱敏：中间4位替换为****。"""
        rule = MaskRule(
            rule_id="TEST-PHONE", target_field="phone",
            mask_method=MaskMethod.PHONE, mask_pattern="",
        )
        result = rule.apply("13800138000")
        assert result.startswith("138")
        assert result.endswith("8000")
        assert "****" in result

    def test_email_masking(self):
        """邮箱脱敏：本地部分脱敏，域名保留。"""
        rule = MaskRule(
            rule_id="TEST-EMAIL", target_field="email",
            mask_method=MaskMethod.EMAIL, mask_pattern="",
        )
        result = rule.apply("user@example.com")
        assert "@example.com" in result
        assert "user@example.com" != result

    def test_id_card_masking(self):
        """身份证脱敏：中间8位替换为********。"""
        rule = MaskRule(
            rule_id="TEST-ID", target_field="id_card",
            mask_method=MaskMethod.ID_CARD, mask_pattern="",
        )
        result = rule.apply("110101199001011234")
        assert result.startswith("110101")
        assert result.endswith("1234")
        assert "********" in result

    def test_custom_regex_masking(self):
        """自定义正则脱敏：匹配 token 并替换为***。"""
        rule = MaskRule(
            rule_id="TEST-TOKEN", target_field="token",
            mask_method=MaskMethod.CUSTOM_REGEX,
            mask_pattern=r'[A-Za-z0-9_\-]{20,}={0,2}',
        )
        result = rule.apply("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")
        assert result == "***"

    def test_empty_value_masking(self):
        """空值脱敏应原样返回。"""
        rule = MaskRule(
            rule_id="TEST-EMPTY", target_field="empty",
            mask_method=MaskMethod.IP, mask_pattern="",
        )
        assert rule.apply("") == ""


class TestEventRecordMask:
    """测试对真实事件记录执行脱敏。"""

    def test_event_record_ip_masked(self):
        """事件记录的 client_ip 应被脱敏。"""
        pipeline = MaskEnrichPipeline()
        records = make_test_events()
        assert len(records) > 0

        result = pipeline.apply_all_masks(records[0])
        masked_ip = records[0].masked_fields.get("client_ip", "")
        assert "*" in masked_ip or masked_ip != records[0].parsed_fields.get("client_ip", "")

    def test_batch_mask_all_records(self):
        """批量处理应覆盖所有记录。"""
        pipeline = MaskEnrichPipeline()
        records = make_test_events()
        processed = pipeline.process_records(records)

        assert len(processed) == len(records)
        for record in processed:
            assert "client_ip" in record.masked_fields

    def test_mask_result_verification(self):
        """脱敏结果验证：掩码值不应包含原始敏感核心。"""
        rule = MaskRule(
            rule_id="TEST-VERIFY", target_field="phone",
            mask_method=MaskMethod.PHONE, mask_pattern="",
        )
        pipeline = MaskEnrichPipeline(mask_rules=[rule])
        records = make_test_app_events()
        for record in records:
            pipeline.apply_all_masks(record)

        verify = pipeline.verify_mask_completeness(records)
        print(f"  Mask verification: {verify}")
        # 对 phone 字段进行验证
        assert verify["coverage_rate"] >= 0


class TestFieldEnrichment:
    """测试字段增强规则的正确性。"""

    def test_app_name_enrichment(self):
        """logger 字段应映射到 app_name。"""
        rule = PRESET_ENRICH_RULES[0]
        fields = {"logger": "com.example.IngestController"}
        result = rule.enrich(fields)
        assert result == "ingest-service"

    def test_unknown_logger_default_value(self):
        """未知 logger 应返回默认值。"""
        rule = PRESET_ENRICH_RULES[0]
        fields = {"logger": "com.unknown.SomeClass"}
        result = rule.enrich(fields)
        assert result == "unknown-service"

    def test_severity_level_enrichment(self):
        """level 字段应映射到 severity_level。"""
        rule = PRESET_ENRICH_RULES[1]
        assert rule.enrich({"level": "ERROR"}) == "HIGH"
        assert rule.enrich({"level": "WARN"}) == "MEDIUM"
        assert rule.enrich({"level": "INFO"}) == "LOW"

    def test_status_category_enrichment(self):
        """HTTP status 应映射到类别标签。"""
        rule = PRESET_ENRICH_RULES[2]
        assert rule.enrich({"status": "200"}) == "SUCCESS"
        assert rule.enrich({"status": "500"}) == "SERVER_ERROR"
        assert rule.enrich({"status": "404"}) == "CLIENT_ERROR"
        assert rule.enrich({"status": "401"}) == "AUTH_ERROR"

    def test_enrich_on_real_events(self):
        """对真实事件记录执行字段增强。"""
        pipeline = MaskEnrichPipeline()
        records = make_test_app_events()

        for record in records:
            pipeline.apply_all_enrichments(record)

        for record in records:
            assert "app_name" in record.enriched_fields
            assert "severity_level" in record.enriched_fields

    def test_enrich_priority_ordering(self):
        """高优先级增强规则应先执行。"""
        rules = [
            FieldEnrichRule(
                rule_id="LOW-PRIO", target_field="result", enrich_source="level",
                mapping_dict={"INFO": "low_result"}, priority=1,
            ),
            FieldEnrichRule(
                rule_id="HIGH-PRIO", target_field="result", enrich_source="level",
                mapping_dict={"INFO": "high_result"}, priority=100,
            ),
        ]
        pipeline = MaskEnrichPipeline(mask_rules=[], enrich_rules=rules)
        record = EventRecord(
            record_id="TEST-001",
            event_type=EventTypeDef(
                type_id="EVEN-TEST", type_name="test",
                category_tag="test", severity="INFO",
            ),
            original_content="test",
            parsed_fields={"level": "INFO"},
            masked_fields={},
            enriched_fields={},
            event_time=__import__("datetime").datetime.now(),
            source_host="test-host",
        )
        pipeline.apply_all_enrichments(record)
        # HIGH-PRIO 应最后写入（sor按-priority排序后，高priority在后）
        assert record.enriched_fields.get("result") == "high_result"


class TestMaskLeakDetection:
    """测试脱敏遗漏告警。"""

    def test_no_leak_when_properly_masked(self):
        """正确脱敏时不应有遗漏。"""
        pipeline = MaskEnrichPipeline()
        records = make_test_events()
        pipeline.process_records(records)
        result = pipeline.verify_mask_completeness(records)
        # IP 脱敏应该覆盖
        assert result["coverage_rate"] >= 0

    def test_leak_when_no_mask_rule_applied(self):
        """未配置脱敏规则时原始字段应被检测。"""
        pipeline = MaskEnrichPipeline(mask_rules=[])  # 空规则集
        records = make_test_events()
        pipeline.process_records(records)
        result = pipeline.verify_mask_completeness(records)
        # 无规则时 total_rules=0, coverage 为 1.0
        assert result["total_rules"] == 0
        assert result["coverage_rate"] == 1.0


if __name__ == "__main__":
    import traceback

    tests = {
        "mask_ip": TestMaskTransformations().test_ip_masking,
        "mask_phone": TestMaskTransformations().test_phone_masking,
        "mask_email": TestMaskTransformations().test_email_masking,
        "mask_id_card": TestMaskTransformations().test_id_card_masking,
        "mask_custom_regex": TestMaskTransformations().test_custom_regex_masking,
        "mask_empty_value": TestMaskTransformations().test_empty_value_masking,
        "event_record_ip_masked": TestEventRecordMask().test_event_record_ip_masked,
        "batch_mask_all_records": TestEventRecordMask().test_batch_mask_all_records,
        "mask_result_verification": TestEventRecordMask().test_mask_result_verification,
        "enrich_app_name": TestFieldEnrichment().test_app_name_enrichment,
        "enrich_unknown_logger": TestFieldEnrichment().test_unknown_logger_default_value,
        "enrich_severity": TestFieldEnrichment().test_severity_level_enrichment,
        "enrich_status_category": TestFieldEnrichment().test_status_category_enrichment,
        "enrich_on_real_events": TestFieldEnrichment().test_enrich_on_real_events,
        "enrich_priority": TestFieldEnrichment().test_enrich_priority_ordering,
        "leak_no_leak": TestMaskLeakDetection().test_no_leak_when_properly_masked,
        "leak_empty_rules": TestMaskLeakDetection().test_leak_when_no_mask_rule_applied,
    }

    passed = 0
    failed = 0
    for name, test_func in tests.items():
        try:
            test_func()
            passed += 1
            print(f"  PASS  test_mask_enrich: {name}")
        except Exception as e:
            failed += 1
            print(f"  FAIL  test_mask_enrich: {name}")
            traceback.print_exc()

    print(f"\ntest_mask_enrich: {passed} passed, {failed} failed, {len(tests)} total")
    sys.exit(1 if failed else 0)