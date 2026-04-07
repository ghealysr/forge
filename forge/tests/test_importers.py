"""Tests for forge.importers — FCC ULS, NPI Registry, SMTP Verifier."""

from forge.importers.fcc_uls import (
    BUSINESS_TYPES,
    COL_APPLICANT_TYPE,
    COL_EMAIL,
    COL_ENTITY_NAME,
    COL_ENTITY_TYPE,
    COL_PHONE,
    COL_STATE,
    build_name_state_index,
    build_phone_index,
    normalize_name,
    parse_en_file,
)
from forge.importers.fcc_uls import (
    normalize_phone as fcc_normalize_phone,
)
from forge.importers.npi_registry import (
    classify_taxonomy,
)
from forge.importers.npi_registry import (
    normalize_phone as npi_normalize_phone,
)
from forge.importers.smtp_verifier import (
    extract_domain,
)

# ---------------------------------------------------------------------------
# FCC ULS: normalize_phone
# ---------------------------------------------------------------------------


class TestFCCNormalizePhone:
    def test_10_digit_passthrough(self):
        assert fcc_normalize_phone("8135551234") == "8135551234"

    def test_strips_formatting(self):
        assert fcc_normalize_phone("(813) 555-1234") == "8135551234"

    def test_strips_country_code(self):
        assert fcc_normalize_phone("18135551234") == "8135551234"
        assert fcc_normalize_phone("+18135551234") == "8135551234"

    def test_too_short_returns_none(self):
        assert fcc_normalize_phone("555-1234") is None

    def test_too_long_returns_none(self):
        assert fcc_normalize_phone("18135551234999") is None

    def test_empty_returns_none(self):
        assert fcc_normalize_phone("") is None

    def test_none_returns_none(self):
        assert fcc_normalize_phone(None) is None

    def test_international_non_us(self):
        assert fcc_normalize_phone("442071234567") is None


# ---------------------------------------------------------------------------
# FCC ULS: normalize_name
# ---------------------------------------------------------------------------


class TestNormalizeName:
    def test_removes_llc(self):
        assert "LLC" not in normalize_name("ACME Solutions LLC")

    def test_removes_inc(self):
        assert "INC" not in normalize_name("Tech Corp Inc")
        assert "INC." not in normalize_name("Tech Corp Inc.")

    def test_removes_corp(self):
        assert "CORP" not in normalize_name("Global Corp")

    def test_removes_ltd(self):
        assert "LTD" not in normalize_name("Widget Ltd")

    def test_uppercase_and_strips(self):
        result = normalize_name("  acme solutions  ")
        assert result == "ACME SOLUTIONS"

    def test_removes_multiple_suffixes(self):
        result = normalize_name("The Widget Co. LLC")
        assert "CO." not in result
        assert "LLC" not in result
        # " THE" suffix removal only applies when preceded by a space
        # "The" at start of name becomes "THE" after upper(); that's preserved.
        assert result == "THE WIDGET"

    def test_removes_commas_and_dots(self):
        result = normalize_name("Smith, Jones & Associates, Inc.")
        assert "INC." not in result

    def test_preserves_core_name(self):
        result = normalize_name("Tampa Bay Dental")
        assert result == "TAMPA BAY DENTAL"


# ---------------------------------------------------------------------------
# FCC ULS: parse_en_file
# ---------------------------------------------------------------------------


class TestParseENFile:
    """Test parsing pipe-delimited FCC EN.dat files."""

    def _make_en_line(
        self,
        entity_type="L",
        applicant_type="C",
        name="ACME TELECOM",
        phone="8135551234",
        email="info@acmetelecom.com",
        state="FL",
        city="TAMPA",
        zip_code="33602",
    ):
        """Build a pipe-delimited line with the right column positions."""
        # EN.dat has 24+ columns. We need fields at specific positions.
        fields = [""] * 25
        fields[COL_ENTITY_TYPE] = entity_type
        fields[COL_ENTITY_NAME] = name
        fields[COL_PHONE] = phone
        fields[COL_EMAIL] = email
        fields[15] = "123 MAIN ST"  # street
        fields[16] = city
        fields[COL_STATE] = state
        fields[18] = zip_code
        fields[COL_APPLICANT_TYPE] = applicant_type
        return "|".join(fields)

    def test_parses_valid_business_record(self, tmp_path):
        en_file = tmp_path / "EN.dat"
        en_file.write_text(self._make_en_line() + "\n")

        records = parse_en_file(str(en_file))
        assert len(records) == 1
        assert records[0]["name"] == "ACME TELECOM"
        assert records[0]["email"] == "info@acmetelecom.com"
        assert records[0]["state"] == "FL"
        assert records[0]["phone"] == "8135551234"

    def test_skips_non_licensee_entity_type(self, tmp_path):
        en_file = tmp_path / "EN.dat"
        en_file.write_text(self._make_en_line(entity_type="CL") + "\n")
        records = parse_en_file(str(en_file))
        assert len(records) == 0

    def test_skips_individual_applicant_type(self, tmp_path):
        en_file = tmp_path / "EN.dat"
        en_file.write_text(self._make_en_line(applicant_type="I") + "\n")
        records = parse_en_file(str(en_file))
        assert len(records) == 0

    def test_skips_no_email(self, tmp_path):
        en_file = tmp_path / "EN.dat"
        en_file.write_text(self._make_en_line(email="") + "\n")
        records = parse_en_file(str(en_file))
        assert len(records) == 0

    def test_skips_invalid_email(self, tmp_path):
        en_file = tmp_path / "EN.dat"
        en_file.write_text(self._make_en_line(email="nope") + "\n")
        records = parse_en_file(str(en_file))
        assert len(records) == 0

    def test_skips_no_name(self, tmp_path):
        en_file = tmp_path / "EN.dat"
        en_file.write_text(self._make_en_line(name="") + "\n")
        records = parse_en_file(str(en_file))
        assert len(records) == 0

    def test_skips_short_lines(self, tmp_path):
        en_file = tmp_path / "EN.dat"
        en_file.write_text("field1|field2|field3\n")
        records = parse_en_file(str(en_file))
        assert len(records) == 0

    def test_normalizes_name(self, tmp_path):
        en_file = tmp_path / "EN.dat"
        en_file.write_text(self._make_en_line(name="Acme Telecom LLC") + "\n")
        records = parse_en_file(str(en_file))
        assert records[0]["name_normalized"] == "ACME TELECOM"

    def test_lowercases_email(self, tmp_path):
        en_file = tmp_path / "EN.dat"
        en_file.write_text(self._make_en_line(email="Info@ACME.COM") + "\n")
        records = parse_en_file(str(en_file))
        assert records[0]["email"] == "info@acme.com"

    def test_multiple_records(self, tmp_path):
        en_file = tmp_path / "EN.dat"
        lines = [
            self._make_en_line(name="BIZ A", email="a@biz.com", phone="1111111111"),
            self._make_en_line(name="BIZ B", email="b@biz.com", phone="2222222222"),
            self._make_en_line(name="BIZ C", email="c@biz.com", phone="3333333333"),
        ]
        en_file.write_text("\n".join(lines) + "\n")
        records = parse_en_file(str(en_file))
        assert len(records) == 3

    def test_all_valid_business_types(self, tmp_path):
        """All BUSINESS_TYPES should be accepted."""
        en_file = tmp_path / "EN.dat"
        lines = [
            self._make_en_line(applicant_type=bt, name=f"BIZ_{bt}", email=f"x{bt}@biz.com")
            for bt in BUSINESS_TYPES
        ]
        en_file.write_text("\n".join(lines) + "\n")
        records = parse_en_file(str(en_file))
        assert len(records) == len(BUSINESS_TYPES)


# ---------------------------------------------------------------------------
# FCC ULS: index builders
# ---------------------------------------------------------------------------


class TestFCCIndexes:
    def test_build_phone_index(self):
        records = [
            {"phone": "1111111111", "name": "A", "name_normalized": "A", "state": "FL"},
            {"phone": "2222222222", "name": "B", "name_normalized": "B", "state": "CA"},
            {"phone": None, "name": "C", "name_normalized": "C", "state": "TX"},
        ]
        idx = build_phone_index(records)
        assert "1111111111" in idx
        assert "2222222222" in idx
        assert None not in idx

    def test_build_name_state_index(self):
        records = [
            {"name_normalized": "ACME", "state": "FL", "phone": None},
            {"name_normalized": "WIDGET", "state": "CA", "phone": None},
        ]
        idx = build_name_state_index(records)
        assert "ACME|FL" in idx
        assert "WIDGET|CA" in idx


# ---------------------------------------------------------------------------
# NPI Registry: normalize_phone
# ---------------------------------------------------------------------------


class TestNPINormalizePhone:
    def test_10_digit_passthrough(self):
        assert npi_normalize_phone("8135551234") == "8135551234"

    def test_strips_formatting(self):
        assert npi_normalize_phone("(813) 555-1234") == "8135551234"

    def test_strips_country_code(self):
        assert npi_normalize_phone("18135551234") == "8135551234"

    def test_short_returns_none(self):
        assert npi_normalize_phone("555") is None

    def test_empty_returns_none(self):
        assert npi_normalize_phone("") is None

    def test_none_returns_none(self):
        assert npi_normalize_phone(None) is None


# ---------------------------------------------------------------------------
# NPI Registry: classify_taxonomy
# ---------------------------------------------------------------------------


class TestClassifyTaxonomy:
    def test_dentist(self):
        assert classify_taxonomy("General Dentistry") == "dentist"
        assert classify_taxonomy("Dental Hygienist") == "dentist"
        assert classify_taxonomy("Orthodontist") == "dentist"
        assert classify_taxonomy("Periodontist") == "dentist"
        assert classify_taxonomy("Endodontist") == "dentist"

    def test_chiropractor(self):
        assert classify_taxonomy("Chiropractic") == "chiropractor"
        assert classify_taxonomy("Doctor of Chiropractic Medicine") == "chiropractor"

    def test_veterinarian(self):
        assert classify_taxonomy("Veterinary Medicine") == "veterinarian"
        assert classify_taxonomy("Veterinarian, Small Animal") == "veterinarian"

    def test_personal_trainer_from_physical_therapy(self):
        assert classify_taxonomy("Physical Therapist") == "personal-trainer"

    def test_salon(self):
        assert classify_taxonomy("Cosmetology School") == "salon"

    def test_barber(self):
        assert classify_taxonomy("Licensed Barber") == "barber"

    def test_none_for_unmapped(self):
        assert classify_taxonomy("Cardiology") is None
        assert classify_taxonomy("Radiology") is None

    def test_none_for_optometry(self):
        """Optometry is explicitly mapped to None."""
        assert classify_taxonomy("Optometrist") is None

    def test_none_for_empty(self):
        assert classify_taxonomy("") is None
        assert classify_taxonomy(None) is None


# ---------------------------------------------------------------------------
# SMTP Verifier: extract_domain
# ---------------------------------------------------------------------------


class TestExtractDomain:
    def test_simple_url(self):
        assert extract_domain("https://tampadental.com") == "tampadental.com"

    def test_url_with_path(self):
        assert extract_domain("https://tampadental.com/contact") == "tampadental.com"

    def test_url_without_scheme(self):
        assert extract_domain("tampadental.com") == "tampadental.com"

    def test_strips_www(self):
        assert extract_domain("https://www.tampadental.com") == "tampadental.com"

    def test_http_scheme(self):
        assert extract_domain("http://mybiz.net") == "mybiz.net"

    def test_skips_facebook(self):
        assert extract_domain("https://facebook.com/mybiz") is None

    def test_skips_instagram(self):
        assert extract_domain("https://instagram.com/mybiz") is None

    def test_skips_twitter(self):
        assert extract_domain("https://twitter.com/mybiz") is None
        assert extract_domain("https://x.com/mybiz") is None

    def test_skips_linkedin(self):
        assert extract_domain("https://linkedin.com/company/mybiz") is None

    def test_skips_youtube(self):
        assert extract_domain("https://youtube.com/c/mybiz") is None

    def test_skips_yelp(self):
        assert extract_domain("https://yelp.com/biz/mybiz") is None

    def test_skips_wix(self):
        assert extract_domain("https://wix.com") is None

    def test_skips_squarespace(self):
        assert extract_domain("https://squarespace.com") is None

    def test_skips_google(self):
        assert extract_domain("https://google.com") is None

    def test_empty_returns_none(self):
        assert extract_domain("") is None

    def test_whitespace_only_returns_none(self):
        assert extract_domain("   ") is None

    def test_ip_address_returns_none(self):
        assert extract_domain("http://192.168.1.1") is None

    def test_no_dot_returns_none(self):
        assert extract_domain("http://localhost") is None

    def test_trailing_dots_stripped(self):
        result = extract_domain("https://mybiz.com.")
        assert result == "mybiz.com"

    def test_subdomain_preserved(self):
        # Subdomains that aren't www should be kept
        result = extract_domain("https://shop.mybiz.com")
        assert result == "shop.mybiz.com"
