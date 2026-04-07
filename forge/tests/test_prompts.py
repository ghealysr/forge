"""Tests for forge.enrichment.prompts — prompt templates for AI enrichment."""

from forge.enrichment.prompts import (
    INDUSTRY_LIST,
    build_batch_enrichment_prompt,
    build_health_score_prompt,
    build_industry_classification_prompt,
    build_single_enrichment_prompt,
    build_summary_prompt,
)

# ---------------------------------------------------------------------------
# Tests: INDUSTRY_LIST
# ---------------------------------------------------------------------------


class TestIndustryList:
    def test_contains_key_industries(self):
        for industry in ["dentist", "lawyer", "restaurant", "salon", "plumber"]:
            assert industry in INDUSTRY_LIST

    def test_is_comma_separated(self):
        assert ", " in INDUSTRY_LIST

    def test_has_20_industries(self):
        items = [i.strip() for i in INDUSTRY_LIST.split(",")]
        assert len(items) == 20


# ---------------------------------------------------------------------------
# Tests: build_single_enrichment_prompt
# ---------------------------------------------------------------------------


class TestSingleEnrichmentPrompt:
    def test_includes_business_name(self):
        biz = {"name": "Tampa Dental", "city": "Tampa", "state": "FL"}
        prompt = build_single_enrichment_prompt(biz)
        assert "Tampa Dental" in prompt

    def test_includes_city_state(self):
        biz = {"name": "Test", "city": "Austin", "state": "TX"}
        prompt = build_single_enrichment_prompt(biz)
        assert "Austin" in prompt
        assert "TX" in prompt

    def test_includes_json_instructions(self):
        biz = {"name": "Test"}
        prompt = build_single_enrichment_prompt(biz)
        assert "JSON" in prompt
        assert "summary" in prompt
        assert "industry" in prompt
        assert "health_score" in prompt
        assert "pain_points" in prompt

    def test_includes_industry_list(self):
        biz = {"name": "Test"}
        prompt = build_single_enrichment_prompt(biz)
        assert "dentist" in prompt
        assert "lawyer" in prompt

    def test_handles_missing_fields(self):
        biz = {}
        prompt = build_single_enrichment_prompt(biz)
        assert "Unknown" in prompt  # Default name

    def test_includes_website_if_present(self):
        biz = {"name": "Test", "website_url": "https://test.com"}
        prompt = build_single_enrichment_prompt(biz)
        assert "https://test.com" in prompt


# ---------------------------------------------------------------------------
# Tests: build_batch_enrichment_prompt
# ---------------------------------------------------------------------------


class TestBatchEnrichmentPrompt:
    def test_includes_all_businesses(self):
        businesses = [
            {"id": "1", "name": "Biz A", "city": "Tampa", "state": "FL", "zip": "33602"},
            {"id": "2", "name": "Biz B", "city": "Miami", "state": "FL", "zip": "33101"},
        ]
        prompt = build_batch_enrichment_prompt(businesses)
        assert "Biz A" in prompt
        assert "Biz B" in prompt

    def test_includes_json_array_instruction(self):
        businesses = [{"id": "1", "name": "Test", "city": "Tampa", "state": "FL", "zip": "33602"}]
        prompt = build_batch_enrichment_prompt(businesses)
        assert "JSON array" in prompt

    def test_includes_ids(self):
        businesses = [{"id": "abc-123", "name": "Test"}]
        prompt = build_batch_enrichment_prompt(businesses)
        assert "abc-123" in prompt


# ---------------------------------------------------------------------------
# Tests: build_industry_classification_prompt
# ---------------------------------------------------------------------------


class TestIndustryClassificationPrompt:
    def test_includes_business_name(self):
        biz = {"name": "Smith Family Dental"}
        prompt = build_industry_classification_prompt(biz)
        assert "Smith Family Dental" in prompt

    def test_includes_categories(self):
        biz = {"name": "Test"}
        prompt = build_industry_classification_prompt(biz)
        assert "dentist" in prompt
        assert "lawyer" in prompt

    def test_includes_current_category(self):
        biz = {"name": "Test", "industry": "healthcare"}
        prompt = build_industry_classification_prompt(biz)
        assert "healthcare" in prompt

    def test_asks_for_json_output(self):
        biz = {"name": "Test"}
        prompt = build_industry_classification_prompt(biz)
        assert "JSON" in prompt


# ---------------------------------------------------------------------------
# Tests: build_health_score_prompt
# ---------------------------------------------------------------------------


class TestHealthScorePrompt:
    def test_includes_data_fields(self):
        biz = {
            "name": "Test",
            "phone": "8135551234",
            "website_url": "https://test.com",
            "email": "info@test.com",
            "address_line1": "123 Main St",
            "industry": "dentist",
            "ssl_valid": True,
            "tech_stack": '["wordpress"]',
        }
        prompt = build_health_score_prompt(biz)
        assert "phone" in prompt.lower()
        assert "website" in prompt.lower()
        assert "email" in prompt.lower()
        assert "True" in prompt or "true" in prompt.lower()

    def test_scoring_guide_included(self):
        biz = {"name": "Test"}
        prompt = build_health_score_prompt(biz)
        assert "health_score" in prompt
        assert "0-100" in prompt

    def test_handles_empty_business(self):
        biz = {}
        prompt = build_health_score_prompt(biz)
        assert "False" in prompt  # All fields should be False


# ---------------------------------------------------------------------------
# Tests: build_summary_prompt
# ---------------------------------------------------------------------------


class TestSummaryPrompt:
    def test_includes_business_details(self):
        biz = {"name": "Tampa Dental", "industry": "dentist", "city": "Tampa", "state": "FL"}
        prompt = build_summary_prompt(biz)
        assert "Tampa Dental" in prompt
        assert "dentist" in prompt
        assert "Tampa" in prompt

    def test_asks_for_json(self):
        biz = {"name": "Test"}
        prompt = build_summary_prompt(biz)
        assert "summary" in prompt

    def test_length_constraint(self):
        biz = {"name": "Test"}
        prompt = build_summary_prompt(biz)
        assert "10-500" in prompt
