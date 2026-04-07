"""Tests for forge.safety.error_recovery — field validation and batch failure tracking."""


from forge.safety.error_recovery import (
    FIELD_VALIDATORS,
    validate_field,
    validate_updates,
)

# ---------------------------------------------------------------------------
# Tests: validate_field
# ---------------------------------------------------------------------------

class TestValidateField:
    # -- email --
    def test_email_valid(self):
        ok, err = validate_field("email", "info@tampadental.com")
        assert ok is True
        assert err == ""

    def test_email_missing_at(self):
        ok, err = validate_field("email", "notanemail")
        assert ok is False
        assert "pattern" in err.lower()

    def test_email_too_long(self):
        ok, err = validate_field("email", "a" * 250 + "@b.com")
        assert ok is False
        assert "max" in err.lower()

    def test_email_wrong_type(self):
        ok, err = validate_field("email", 12345)
        assert ok is False

    # -- industry --
    def test_industry_whitelisted(self):
        ok, _ = validate_field("industry", "dentist")
        assert ok is True

    def test_industry_not_whitelisted(self):
        ok, err = validate_field("industry", "rocket-science")
        assert ok is False
        assert "whitelist" in err.lower()

    def test_industry_case_insensitive_whitelist(self):
        ok, _ = validate_field("industry", "Dentist")
        assert ok is True

    # -- ai_summary --
    def test_summary_valid(self):
        ok, _ = validate_field("ai_summary", "A great dental practice serving Tampa Bay families.")
        assert ok is True

    def test_summary_too_short(self):
        ok, err = validate_field("ai_summary", "Short")
        assert ok is False
        assert "min" in err.lower()

    def test_summary_too_long(self):
        ok, err = validate_field("ai_summary", "x" * 501)
        assert ok is False
        assert "max" in err.lower()

    # -- health_score --
    def test_health_score_valid(self):
        ok, _ = validate_field("health_score", 75)
        assert ok is True

    def test_health_score_zero(self):
        ok, _ = validate_field("health_score", 0)
        assert ok is True

    def test_health_score_100(self):
        ok, _ = validate_field("health_score", 100)
        assert ok is True

    def test_health_score_negative(self):
        ok, err = validate_field("health_score", -1)
        assert ok is False
        assert "below min" in err

    def test_health_score_over_100(self):
        ok, err = validate_field("health_score", 101)
        assert ok is False
        assert "above max" in err

    def test_health_score_string_convertible(self):
        # String that can be converted to int is accepted
        ok, _ = validate_field("health_score", "75")
        assert ok is True

    def test_health_score_string_not_convertible(self):
        ok, err = validate_field("health_score", "abc")
        assert ok is False

    # -- ssl_valid --
    def test_ssl_valid_bool(self):
        ok, _ = validate_field("ssl_valid", True)
        assert ok is True

    def test_ssl_valid_int_as_bool(self):
        ok, _ = validate_field("ssl_valid", 1)
        assert ok is True

    # -- site_speed_ms --
    def test_site_speed_valid(self):
        ok, _ = validate_field("site_speed_ms", 500)
        assert ok is True

    def test_site_speed_too_high(self):
        ok, err = validate_field("site_speed_ms", 999999)
        assert ok is False

    # -- tech_stack --
    def test_tech_stack_valid(self):
        ok, _ = validate_field("tech_stack", '["wordpress", "google-analytics"]')
        assert ok is True

    def test_tech_stack_too_long(self):
        ok, err = validate_field("tech_stack", "x" * 2001)
        assert ok is False

    # -- cms_detected --
    def test_cms_detected_valid(self):
        ok, _ = validate_field("cms_detected", "wordpress")
        assert ok is True

    # -- pain_points --
    def test_pain_points_list_valid(self):
        ok, _ = validate_field("pain_points", ["no website", "few reviews"])
        assert ok is True

    def test_pain_points_dict_valid(self):
        ok, _ = validate_field("pain_points", {"key": "value"})
        assert ok is True

    # -- unknown field --
    def test_unknown_field_allowed(self):
        ok, _ = validate_field("unknown_field_xyz", "anything")
        assert ok is True
        # No rules = allow


# ---------------------------------------------------------------------------
# Tests: validate_updates
# ---------------------------------------------------------------------------

class TestValidateUpdates:
    def test_all_valid(self):
        updates = {
            "email": "info@dental.com",
            "industry": "dentist",
            "health_score": 75,
        }
        valid, errors = validate_updates(updates)
        assert len(valid) == 3
        assert len(errors) == 0

    def test_mixed_valid_invalid(self):
        updates = {
            "email": "info@dental.com",
            "industry": "rocket-science",  # Not in whitelist
        }
        valid, errors = validate_updates(updates)
        assert "email" in valid
        assert "industry" not in valid
        assert len(errors) == 1

    def test_null_values_rejected(self):
        updates = {
            "email": None,
            "industry": "dentist",
        }
        valid, errors = validate_updates(updates)
        assert "email" not in valid
        assert "industry" in valid
        assert len(errors) == 1
        assert "COALESCE" in errors[0]

    def test_empty_updates(self):
        valid, errors = validate_updates({})
        assert valid == {}
        assert errors == []

    def test_all_invalid(self):
        updates = {
            "email": None,
            "industry": None,
            "health_score": None,
        }
        valid, errors = validate_updates(updates)
        assert len(valid) == 0
        assert len(errors) == 3


# ---------------------------------------------------------------------------
# Tests: FIELD_VALIDATORS structure
# ---------------------------------------------------------------------------

class TestFieldValidators:
    def test_expected_fields_present(self):
        expected = ["email", "industry", "ai_summary", "health_score",
                    "tech_stack", "cms_detected", "ssl_valid", "site_speed_ms",
                    "pain_points"]
        for field in expected:
            assert field in FIELD_VALIDATORS

    def test_all_validators_have_type(self):
        for field, rules in FIELD_VALIDATORS.items():
            assert "type" in rules, f"Missing 'type' in validator for {field}"
