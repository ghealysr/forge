"""Tests for forge.safety.audit_agent — data classes and pure logic."""

import time

import pytest

from forge.safety.audit_agent import (
    DEFAULT_AUDIT_INTERVAL,
    DEFAULT_SAMPLE_SIZE,
    FAILURE_THRESHOLD,
    HAIKU_MODEL,
    AuditResult,
    AuditStats,
)

# ---------------------------------------------------------------------------
# Tests: AuditResult
# ---------------------------------------------------------------------------

class TestAuditResult:
    def test_construction(self):
        r = AuditResult(
            business_id="uuid-1",
            business_name="Tampa Dental",
            passed=True,
            issues=[],
            confidence=0.95,
            haiku_feedback="Looks good.",
        )
        assert r.business_id == "uuid-1"
        assert r.passed is True
        assert r.confidence == 0.95
        assert r.timestamp > 0  # Auto-set by __post_init__

    def test_auto_timestamp(self):
        before = time.time()
        r = AuditResult(
            business_id="uuid-1",
            business_name="Test",
            passed=True,
            issues=[],
            confidence=0.9,
            haiku_feedback="OK",
        )
        after = time.time()
        assert before <= r.timestamp <= after

    def test_explicit_timestamp(self):
        r = AuditResult(
            business_id="uuid-1",
            business_name="Test",
            passed=False,
            issues=["bad summary"],
            confidence=0.3,
            haiku_feedback="Poor quality.",
            timestamp=1000.0,
        )
        assert r.timestamp == 1000.0

    def test_failed_result(self):
        r = AuditResult(
            business_id="uuid-2",
            business_name="Bad Biz",
            passed=False,
            issues=["hallucinated_summary", "wrong_industry"],
            confidence=0.2,
            haiku_feedback="Multiple issues found.",
        )
        assert r.passed is False
        assert len(r.issues) == 2


# ---------------------------------------------------------------------------
# Tests: AuditStats
# ---------------------------------------------------------------------------

class TestAuditStats:
    def test_empty_stats(self):
        stats = AuditStats()
        assert stats.total_audited == 0
        assert stats.pass_rate == 1.0  # No audits = 100% pass rate
        assert stats.failure_rate == 0.0

    def test_pass_rate_calculation(self):
        stats = AuditStats(total_audited=10, total_passed=8, total_failed=2)
        assert stats.pass_rate == 0.8
        assert stats.failure_rate == pytest.approx(0.2)

    def test_all_passed(self):
        stats = AuditStats(total_audited=5, total_passed=5, total_failed=0)
        assert stats.pass_rate == 1.0
        assert stats.failure_rate == 0.0

    def test_all_failed(self):
        stats = AuditStats(total_audited=5, total_passed=0, total_failed=5)
        assert stats.pass_rate == 0.0
        assert stats.failure_rate == 1.0

    def test_summary_format(self):
        stats = AuditStats(total_audited=100, total_passed=90, total_failed=10)
        s = stats.summary()
        assert "Audited: 100" in s
        assert "Passed: 90" in s
        assert "Failed: 10" in s
        assert "90.0%" in s

    def test_issues_by_type(self):
        stats = AuditStats()
        stats.issues_by_type["hallucinated_summary"] = 3
        stats.issues_by_type["wrong_industry"] = 2
        assert stats.issues_by_type["hallucinated_summary"] == 3


# ---------------------------------------------------------------------------
# Tests: Constants
# ---------------------------------------------------------------------------

class TestAuditConstants:
    def test_default_audit_interval(self):
        assert DEFAULT_AUDIT_INTERVAL == 50

    def test_default_sample_size(self):
        assert DEFAULT_SAMPLE_SIZE == 3

    def test_failure_threshold(self):
        assert FAILURE_THRESHOLD == 0.20

    def test_haiku_model_defined(self):
        assert "haiku" in HAIKU_MODEL.lower()
