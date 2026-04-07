"""Tests for forge.importers.sam_gov — SAM.gov entity parsing and normalization."""

from forge.importers.sam_gov import (
    _build_params,
    _extract_entity,
    normalize_name,
)

# ---------------------------------------------------------------------------
# Tests: normalize_name
# ---------------------------------------------------------------------------


class TestSamNormalizeName:
    def test_removes_llc(self):
        assert "LLC" not in normalize_name("Acme Solutions LLC")

    def test_removes_inc(self):
        assert "INC" not in normalize_name("Tech Corp Inc")

    def test_removes_corp(self):
        assert "CORP" not in normalize_name("Global Corp")

    def test_uppercase_and_strip(self):
        result = normalize_name("  hello world  ")
        assert result == "HELLO WORLD"

    def test_preserves_core_name(self):
        result = normalize_name("Tampa Bay Dental")
        assert result == "TAMPA BAY DENTAL"


# ---------------------------------------------------------------------------
# Tests: _build_params
# ---------------------------------------------------------------------------


class TestBuildParams:
    def test_basic_params(self):
        params = _build_params(page=0, page_size=100)
        assert params["page"] == "0"
        assert params["size"] == "100"
        assert params["registrationStatus"] == "A"
        assert "stateCode" not in params

    def test_with_state_filter(self):
        params = _build_params(page=1, state_filter="CA")
        assert params["stateCode"] == "CA"

    def test_state_filter_uppercased(self):
        params = _build_params(page=0, state_filter="fl")
        assert params["stateCode"] == "FL"

    def test_includes_sections(self):
        params = _build_params(page=0)
        assert "entityRegistration" in params["includeSections"]
        assert "coreData" in params["includeSections"]
        assert "pointsOfContact" in params["includeSections"]


# ---------------------------------------------------------------------------
# Tests: _extract_entity
# ---------------------------------------------------------------------------


class TestExtractEntity:
    def _make_entity(
        self,
        org_name="ACME CORP",
        state="FL",
        city="TAMPA",
        zip_code="33602",
        email="info@acme.com",
        first="John",
        last="Smith",
    ):
        return {
            "entityRegistration": {
                "legalBusinessName": org_name,
            },
            "coreData": {
                "physicalAddress": {
                    "stateOrProvinceCode": state,
                    "city": city,
                    "zipCode": zip_code,
                },
                "naicsCode": [
                    {"naicsCode": "541511"},
                    {"naicsCode": "541512"},
                ],
            },
            "pointsOfContact": {
                "governmentBusinessPOC": {
                    "email": email,
                    "firstName": first,
                    "lastName": last,
                },
            },
        }

    def test_extracts_valid_entity(self):
        entity = self._make_entity()
        result = _extract_entity(entity)
        assert result is not None
        assert result["org_name"] == "ACME CORP"
        assert result["state"] == "FL"
        assert result["city"] == "TAMPA"
        assert result["poc_email"] == "info@acme.com"
        assert result["poc_name"] == "John Smith"
        assert "541511" in result["naics_codes"]

    def test_normalizes_name(self):
        entity = self._make_entity(org_name="Acme Solutions LLC")
        result = _extract_entity(entity)
        assert result["name_normalized"] == "ACME SOLUTIONS"

    def test_returns_none_without_org_name(self):
        entity = self._make_entity(org_name="")
        result = _extract_entity(entity)
        assert result is None

    def test_returns_none_without_state(self):
        entity = self._make_entity(state="")
        result = _extract_entity(entity)
        assert result is None

    def test_returns_none_without_email(self):
        entity = self._make_entity(email="")
        result = _extract_entity(entity)
        assert result is None

    def test_email_lowercased(self):
        entity = self._make_entity(email="Info@ACME.COM")
        result = _extract_entity(entity)
        assert result["poc_email"] == "info@acme.com"

    def test_city_uppercased(self):
        entity = self._make_entity(city="tampa")
        result = _extract_entity(entity)
        assert result["city"] == "TAMPA"

    def test_zip_truncated_to_5(self):
        entity = self._make_entity(zip_code="33602-1234")
        result = _extract_entity(entity)
        assert result["zip_code"] == "33602"

    def test_fallback_to_electronic_poc(self):
        entity = {
            "entityRegistration": {"legalBusinessName": "Test Corp"},
            "coreData": {
                "physicalAddress": {
                    "stateOrProvinceCode": "CA",
                    "city": "LA",
                    "zipCode": "90001",
                },
                "naicsCode": [],
            },
            "pointsOfContact": {
                "governmentBusinessPOC": {},
                "electronicBusinessPOC": {
                    "email": "electronic@test.com",
                    "firstName": "Jane",
                    "lastName": "Doe",
                },
            },
        }
        result = _extract_entity(entity)
        assert result is not None
        assert result["poc_email"] == "electronic@test.com"
        assert result["poc_name"] == "Jane Doe"

    def test_naics_codes_as_strings(self):
        entity = self._make_entity()
        entity["coreData"]["naicsCode"] = ["541511", "541512"]
        result = _extract_entity(entity)
        assert "541511" in result["naics_codes"]

    def test_naics_codes_as_ints(self):
        entity = self._make_entity()
        entity["coreData"]["naicsCode"] = [541511]
        result = _extract_entity(entity)
        assert "541511" in result["naics_codes"]

    def test_empty_naics(self):
        entity = self._make_entity()
        entity["coreData"]["naicsCode"] = []
        result = _extract_entity(entity)
        assert result["naics_codes"] == []

    def test_poc_with_middle_initial(self):
        entity = self._make_entity()
        entity["pointsOfContact"]["governmentBusinessPOC"]["middleInitial"] = "Q"
        result = _extract_entity(entity)
        assert result["poc_name"] == "John Q Smith"

    def test_poc_email_must_contain_at(self):
        entity = self._make_entity(email="nope")
        result = _extract_entity(entity)
        assert result is None

    def test_empty_entity(self):
        result = _extract_entity({})
        assert result is None

    def test_missing_core_data(self):
        entity = {
            "entityRegistration": {"legalBusinessName": "Test"},
            "coreData": {},
            "pointsOfContact": {
                "governmentBusinessPOC": {"email": "a@b.com"},
            },
        }
        result = _extract_entity(entity)
        # No state -> returns None
        assert result is None
