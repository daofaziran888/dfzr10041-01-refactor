"""测试：端到端采集解析链路 log_source→parser_rule→ingest_job→event_store。

覆盖数据流：
- 数据流 1: log_source → parser_rule → ingest_job → event_store
- 数据流 2: ingest_job → batch_checkpoint（断点恢复）
- 数据流 8: ingest_job → ingest_metric → quality_report
"""
from __future__ import annotations

import sys
import os
import hashlib
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.models import (
    BatchCheckpoint, IngestJob, IngestMetric, LogSourceConfig, ParseResultStatus,
    ParseRule, QualityReport,
)
from app.core.pipeline import (
    IngestPipeline, PRESET_LOG_SOURCES, PRESET_PARSE_RULES,
    PRESET_EVENT_TYPES, make_sample_raw_lines, make_pipeline,
)


# ---------- 测试数据：模拟文件内容 ----------

MOCK_FILE_PATH = "/var/log/nginx/access.log"


class TestLogSourceValidation:
    """测试 log_source 格式校验。"""

    def test_access_format_validation(self):
        """access 格式的日志应通过校验，非 access 格式应拒绝。"""
        source = PRESET_LOG_SOURCES["access-web"]
        pipeline = make_pipeline()

        valid_line = '192.168.1.10 - - [15/Nov/2026:14:30:05 +0800] "GET /api/users HTTP/1.1" 200 1234 "-" "Mozilla/5.0"'
        entries = pipeline.parse_raw_lines(source, [valid_line])
        assert len(entries) == 1
        assert entries[0].parse_status == ParseResultStatus.OK

    def test_invalid_format_detection(self):
        """脏日志应被标记为 FORMAT_MISMATCH。"""
        source = PRESET_LOG_SOURCES["access-web"]
        pipeline = make_pipeline()

        dirty_lines = [
            "2926-16-15 99:99:99.999 [bad] BAD  malformed - 完全不匹配任何格式",
            "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~",
        ]
        entries = pipeline.parse_raw_lines(source, dirty_lines)
        assert len(entries) == 2
        assert all(e.parse_status == ParseResultStatus.FORMAT_MISMATCH for e in entries)


class TestParseRuleApplication:
    """测试 parser_rule 正则解析。"""

    def test_access_log_parse(self):
        """access 日志解析应提取 8 个字段。"""
        rule = PRESET_PARSE_RULES[0]
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["access-web"]

        line = '192.168.1.10 - - [15/Nov/2026:14:30:05 +0800] "GET /api/users HTTP/1.1" 200 1234 "-" "Mozilla/5.0"'
        entries = pipeline.parse_raw_lines(source, [line])
        result = pipeline.apply_parse_rules(entries, rule)
        assert len(result) == 1
        assert result[0].parse_status == ParseResultStatus.OK
        fields = result[0].parsed_fields
        assert fields is not None
        assert fields["client_ip"] == "192.168.1.10"
        assert fields["method"] == "GET"
        assert fields["status"] == "200"

    def test_app_log_parse(self):
        """app 日志解析应提取 level 和 message。"""
        rule = PRESET_PARSE_RULES[1]
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["app-backend"]

        line = '2026-11-15 14:30:05.123 [http-nio-8080-exec-1] INFO  com.example.IngestController - 采集作业 INGE-0001 启动'
        entries = pipeline.parse_raw_lines(source, [line])
        result = pipeline.apply_parse_rules(entries, rule)
        assert result[0].parse_status == ParseResultStatus.OK
        assert result[0].parsed_fields["level"] == "INFO"
        assert "INGE-0001" in result[0].parsed_fields["message"]

    def test_format_mismatch_stays_unparsed(self):
        """格式不匹配的日志不应被解析，保持原状态。"""
        rule = PRESET_PARSE_RULES[0]
        pipeline = make_pipeline()
        entry = pipeline.parse_raw_lines(
            PRESET_LOG_SOURCES["access-web"],
            ["this is not access log at all"],
        )
        result = pipeline.apply_parse_rules(entry, rule)
        assert result[0].parse_status == ParseResultStatus.FORMAT_MISMATCH
        assert result[0].parsed_fields is None


class TestIngestJobExecution:
    """测试 ingest_job 采集作业执行与批次控制。"""

    def test_batch_size_limit(self):
        """采集作业应按 batch_size 限制处理行数。"""
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["access-web"]

        lines = make_sample_raw_lines()
        entries = pipeline.parse_raw_lines(source, lines)
        job = IngestJob(job_id="INGE-0001", source_id=source.source_id, batch_size=3)
        processed = pipeline.run_ingest_job(entries, job)

        assert job.processed_lines <= 3
        assert job.skipped_lines >= 0

    def test_batch_checkpoint_creation(self):
        """采集作业结束后应创建批次检查点。"""
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["access-web"]
        job = IngestJob(job_id="INGE-0001", source_id=source.source_id, batch_size=5)
        lines = make_sample_raw_lines()
        entries = pipeline.parse_raw_lines(source, lines)
        processed = pipeline.run_ingest_job(entries, job)

        checkpoint = pipeline.create_checkpoint(job, MOCK_FILE_PATH, processed)
        assert isinstance(checkpoint, BatchCheckpoint)
        assert checkpoint.job_id == "INGE-0001"
        assert checkpoint.file_path == MOCK_FILE_PATH
        assert len(checkpoint.content_hash) == 16
        assert checkpoint.state == "已记录"

    def test_checkpoint_resume_offset(self):
        """检查点的 byte_offset 应可供断点恢复使用。"""
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["app-backend"]
        job = IngestJob(job_id="INGE-0002", source_id=source.source_id, batch_size=5)
        rule = PRESET_PARSE_RULES[1]

        lines = make_sample_raw_lines()
        entries = pipeline.parse_raw_lines(source, lines)
        parsed = pipeline.apply_parse_rules(entries, rule)
        pipeline.run_ingest_job(parsed, job)

        checkpoint = pipeline.create_checkpoint(job, MOCK_FILE_PATH, parsed)
        resume_offset = pipeline.resume_from_checkpoint(checkpoint)

        assert resume_offset == job.file_cursor
        assert resume_offset > 0

    def test_multiple_checkpoints_independent(self):
        """多个采集作业的检查点应相互独立，哈希不同。"""
        pipeline1 = make_pipeline()
        pipeline2 = make_pipeline()

        job1 = IngestJob(job_id="INGE-0001", source_id="LOG_-0001", batch_size=3)
        job2 = IngestJob(job_id="INGE-0002", source_id="LOG_-0002", batch_size=3)

        lines = make_sample_raw_lines()
        e1 = pipeline1.parse_raw_lines(PRESET_LOG_SOURCES["access-web"], lines)
        p1 = pipeline1.run_ingest_job(e1, job1)
        cp1 = pipeline1.create_checkpoint(job1, "/var/log/nginx/access.log", p1)

        e2 = pipeline2.parse_raw_lines(PRESET_LOG_SOURCES["app-backend"], lines)
        p2 = pipeline2.run_ingest_job(e2, job2)
        cp2 = pipeline2.create_checkpoint(job2, "/var/log/app/backend.log", p2)

        assert cp1.content_hash != cp2.content_hash


class TestEventClassification:
    """测试事件存储与分类。"""

    def test_log_to_event_store_flow(self):
        """完整的 log_source→parser_rule→ingest_job→event_store 链路。"""
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["app-backend"]
        rule = PRESET_PARSE_RULES[1]
        job = IngestJob(job_id="INGE-0001", source_id=source.source_id, batch_size=20)

        lines = make_sample_raw_lines()
        entries = pipeline.parse_raw_lines(source, lines)
        parsed = pipeline.apply_parse_rules(entries, rule)
        processed = pipeline.run_ingest_job(parsed, job)
        records = pipeline.classify_events(processed)
        store_count = pipeline.store_events(records)

        assert store_count > 0
        assert len(pipeline.get_event_store()) > 0

    def test_error_log_classified_as_error_event(self):
        """ERROR 级别日志应被分类为错误事件。"""
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["app-backend"]
        rule = PRESET_PARSE_RULES[1]
        job = IngestJob(job_id="INGE-0001", source_id=source.source_id, batch_size=20)

        lines = make_sample_raw_lines()
        entries = pipeline.parse_raw_lines(source, lines)
        parsed = pipeline.apply_parse_rules(entries, rule)
        processed = pipeline.run_ingest_job(parsed, job)
        records = pipeline.classify_events(processed)

        error_events = [r for r in records if r.event_type.type_id == "EVEN-0002"]
        assert len(error_events) > 0
        assert error_events[0].event_type.category_tag == "error"

    def test_auth_log_classified_as_auth_event(self):
        """认证相关 sys 日志应被分类为认证事件。"""
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["sys-auth"]
        rule = PRESET_PARSE_RULES[2]
        job = IngestJob(job_id="INGE-0001", source_id=source.source_id, batch_size=20)

        lines = make_sample_raw_lines()
        entries = pipeline.parse_raw_lines(source, lines)
        parsed = pipeline.apply_parse_rules(entries, rule)
        processed = pipeline.run_ingest_job(parsed, job)
        records = pipeline.classify_events(processed)

        auth_events = [r for r in records if r.event_type.type_id == "EVEN-0003"]
        assert len(auth_events) > 0

    def test_dirty_lines_excluded_from_events(self):
        """脏日志行不应生成事件记录。"""
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["access-web"]
        rule = PRESET_PARSE_RULES[0]
        job = IngestJob(job_id="INGE-0001", source_id=source.source_id, batch_size=20)

        dirty_lines = ["this is not valid log format", "just garbage"]
        entries = pipeline.parse_raw_lines(source, dirty_lines)
        parsed = pipeline.apply_parse_rules(entries, rule)
        processed = pipeline.run_ingest_job(parsed, job)
        records = pipeline.classify_events(processed)

        assert len(records) == 0


class TestIngestMetricAndQualityReport:
    """测试采集指标与质量报告数据流 ingest_job→ingest_metric→quality_report。"""

    def test_ingest_metric_calculation(self):
        """采集指标应正确计算速率、错误率和积压。"""
        pipeline = make_pipeline()
        job = IngestJob(job_id="INGE-0001", source_id="LOG_-0001", batch_size=20)
        job.processed_lines = 10
        job.skipped_lines = 3
        total_lines = 15

        metric = pipeline.compute_ingest_metrics(job, 5000.0, total_lines)

        assert metric.ingest_rate > 0
        assert metric.error_rate == 3 / 15
        assert metric.backlog_lines == 2

    def test_quality_report_generation(self):
        """质量报告包含完整率、解析成功率和字段覆盖率。"""
        pipeline = make_pipeline()
        job = IngestJob(job_id="INGE-0001", source_id="LOG_-0001", batch_size=20)
        job.processed_lines = 10
        job.skipped_lines = 2
        total = 15

        metric = pipeline.compute_ingest_metrics(job, 3000.0, total)
        report = pipeline.generate_quality_report(metric, parse_ok=10, parse_total=15,
                                                    fields_coverage=0.875,
                                                    processed=10, skipped=2)

        assert report.completeness_rate > 0
        assert abs(report.parse_success_rate - round(10/15, 4)) < 1e-10
        assert report.field_coverage_rate == 0.875
        assert report.state == "已生成"

    def test_quality_report_zero_lines(self):
        """零日志行时应安全返回零指标。"""
        pipeline = make_pipeline()
        job = IngestJob(job_id="INGE-0001", source_id="LOG_-0001")
        metric = pipeline.compute_ingest_metrics(job, 0.0, 0)
        assert metric.ingest_rate == 0.0
        assert metric.error_rate == 0.0


class TestSchemaChangeDetection:
    """测试 schema_version→parser_rule 兼容变更检测。"""

    def test_compatible_schema_change(self):
        """只新增字段不删除字段应为兼容变更。"""
        pipeline = make_pipeline()
        old_fields = ["client_ip", "method", "url", "status"]
        new_fields = ["client_ip", "method", "url", "status", "request_id"]

        result = pipeline.detect_schema_change(old_fields, new_fields)
        assert result["is_compatible"] is True
        assert "request_id" in result["added_fields"]
        assert result["removed_fields"] == []

    def test_incompatible_schema_change(self):
        """删除字段应为不兼容变更。"""
        pipeline = make_pipeline()
        old_fields = ["client_ip", "method", "url", "status", "body_bytes"]
        new_fields = ["client_ip", "method", "url"]

        result = pipeline.detect_schema_change(old_fields, new_fields)
        assert result["is_compatible"] is False
        assert "body_bytes" in result["removed_fields"]


class TestAnomalyCheckAndForward:
    """测试 anomaly_rule 和 forward_rule 数据流。"""

    def test_anomaly_detection_triggers_on_threshold(self):
        """事件超过阈值时应触发异常告警标记。"""
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["app-backend"]
        rule = PRESET_PARSE_RULES[1]
        job = IngestJob(job_id="INGE-0001", source_id=source.source_id, batch_size=20)

        lines = make_sample_raw_lines()
        entries = pipeline.parse_raw_lines(source, lines)
        parsed = pipeline.apply_parse_rules(entries, rule)
        processed = pipeline.run_ingest_job(parsed, job)
        records = pipeline.classify_events(processed)

        anomalous = pipeline.check_anomaly(records, "EVEN-0002", threshold=0)
        assert len(anomalous) > 0
        assert all("anomaly_alert" in r.parsed_fields for r in anomalous)

    def test_forward_rule_batches(self):
        """转发规则应按目标地址编组分批。"""
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["access-web"]
        rule = PRESET_PARSE_RULES[0]
        job = IngestJob(job_id="INGE-0001", source_id=source.source_id, batch_size=20)

        lines = make_sample_raw_lines()
        entries = pipeline.parse_raw_lines(source, lines)
        parsed = pipeline.apply_parse_rules(entries, rule)
        processed = pipeline.run_ingest_job(parsed, job)
        records = pipeline.classify_events(processed)

        batches = pipeline.apply_forward_rules(records, "webhook://alerts.example.com")
        assert len(batches) == 1
        assert batches[0]["target"] == "webhook://alerts.example.com"
        assert batches[0]["count"] > 0

    def test_archive_policy_marks_old_partitions(self):
        """归档策略应标记超过保留天数的分区。"""
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["access-web"]
        rule = PRESET_PARSE_RULES[0]
        job = IngestJob(job_id="INGE-0001", source_id=source.source_id, batch_size=20)

        lines = make_sample_raw_lines()
        entries = pipeline.parse_raw_lines(source, lines)
        parsed = pipeline.apply_parse_rules(entries, rule)
        processed = pipeline.run_ingest_job(parsed, job)
        records = pipeline.classify_events(processed)
        pipeline.store_events(records)

        # 使用极大的保留天数确保不会意外归档
        archived = pipeline.apply_archive_policy("20261101", 365)
        assert len(archived) == 0

        # 用 0 天保留时间确认归档逻辑可达
        archived2 = pipeline.apply_archive_policy("20200101", 1)
        assert isinstance(archived2, list)


# ---------- 运行入口（确定性测试，不依赖 pytest） ----------

if __name__ == "__main__":
    import traceback

    tests = {
        "log_source_access_format_validation": TestLogSourceValidation().test_access_format_validation,
        "log_source_invalid_format_detection": TestLogSourceValidation().test_invalid_format_detection,
        "parser_access_log_parse": TestParseRuleApplication().test_access_log_parse,
        "parser_app_log_parse": TestParseRuleApplication().test_app_log_parse,
        "parser_format_mismatch": TestParseRuleApplication().test_format_mismatch_stays_unparsed,
        "ingest_batch_size_limit": TestIngestJobExecution().test_batch_size_limit,
        "ingest_checkpoint_creation": TestIngestJobExecution().test_batch_checkpoint_creation,
        "ingest_checkpoint_resume": TestIngestJobExecution().test_checkpoint_resume_offset,
        "ingest_multiple_checkpoints_independent": TestIngestJobExecution().test_multiple_checkpoints_independent,
        "event_log_to_store_flow": TestEventClassification().test_log_to_event_store_flow,
        "event_error_classification": TestEventClassification().test_error_log_classified_as_error_event,
        "event_auth_classification": TestEventClassification().test_auth_log_classified_as_auth_event,
        "event_dirty_lines_excluded": TestEventClassification().test_dirty_lines_excluded_from_events,
        "metric_calculation": TestIngestMetricAndQualityReport().test_ingest_metric_calculation,
        "quality_report_generation": TestIngestMetricAndQualityReport().test_quality_report_generation,
        "quality_report_zero_lines": TestIngestMetricAndQualityReport().test_quality_report_zero_lines,
        "schema_compatible_change": TestSchemaChangeDetection().test_compatible_schema_change,
        "schema_incompatible_change": TestSchemaChangeDetection().test_incompatible_schema_change,
        "anomaly_detection": TestAnomalyCheckAndForward().test_anomaly_detection_triggers_on_threshold,
        "forward_rule_batches": TestAnomalyCheckAndForward().test_forward_rule_batches,
        "archive_policy": TestAnomalyCheckAndForward().test_archive_policy_marks_old_partitions,
    }

    passed = 0
    failed = 0
    for name, test_func in tests.items():
        try:
            test_func()
            passed += 1
            print(f"  PASS  test_ingest_pipeline: {name}")
        except Exception as e:
            failed += 1
            print(f"  FAIL  test_ingest_pipeline: {name}")
            traceback.print_exc()

    print(f"\ntest_ingest_pipeline: {passed} passed, {failed} failed, {len(tests)} total")
    if failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)