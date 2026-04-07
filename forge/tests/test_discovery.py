"""Tests for forge.discovery."""

import pytest

from forge.discovery.overture import (
    _CATEGORY_TO_INDUSTRY,
    _INDUSTRY_TO_CATEGORIES,
    MILES_TO_DEGREES,
    OvertureDiscoveryError,
)
from forge.discovery.zip_centroids import get_zip_centroid

# ---------------------------------------------------------------------------
# Tests: get_zip_centroid
# ---------------------------------------------------------------------------


class TestGetZipCentroid:
    def test_known_zip_tampa(self):
        result = get_zip_centroid("33602")
        assert result is not None
        assert result["city"] == "Tampa"
        assert result["state"] == "FL"
        assert abs(result["lat"] - 27.9517) < 0.01
        assert abs(result["lon"] - (-82.4588)) < 0.01

    def test_known_zip_nyc(self):
        result = get_zip_centroid("10001")
        assert result is not None
        assert result["city"] == "New York"
        assert result["state"] == "NY"

    def test_known_zip_chicago(self):
        result = get_zip_centroid("60601")
        assert result is not None
        assert result["city"] == "Chicago"
        assert result["state"] == "IL"

    def test_known_zip_la(self):
        result = get_zip_centroid("90001")
        assert result is not None
        assert result["city"] == "Los Angeles"
        assert result["state"] == "CA"

    def test_known_zip_sf(self):
        result = get_zip_centroid("94102")
        assert result is not None
        assert result["city"] == "San Francisco"
        assert result["state"] == "CA"

    def test_known_zip_houston(self):
        result = get_zip_centroid("77001")
        assert result is not None
        assert result["city"] == "Houston"
        assert result["state"] == "TX"

    def test_known_zip_phoenix(self):
        result = get_zip_centroid("85001")
        assert result is not None
        assert result["city"] == "Phoenix"
        assert result["state"] == "AZ"

    def test_known_zip_miami(self):
        result = get_zip_centroid("33101")
        assert result is not None
        assert result["city"] == "Miami"
        assert result["state"] == "FL"

    def test_known_zip_dc(self):
        result = get_zip_centroid("20001")
        assert result is not None
        assert result["city"] == "Washington"
        assert result["state"] == "DC"

    def test_known_zip_atlanta(self):
        result = get_zip_centroid("30301")
        assert result is not None
        assert result["city"] == "Atlanta"
        assert result["state"] == "GA"

    def test_unknown_zip_returns_none(self):
        result = get_zip_centroid("00000")
        assert result is None

    def test_unknown_rural_zip(self):
        result = get_zip_centroid("99999")
        assert result is None

    def test_zero_padded(self):
        # Should pad to 5 digits
        result = get_zip_centroid("10001")
        assert result is not None

    def test_numeric_input(self):
        # Accepts numeric string
        result = get_zip_centroid("33602")
        assert result is not None

    def test_result_keys(self):
        result = get_zip_centroid("33602")
        assert set(result.keys()) == {"lat", "lon", "city", "state"}


# ---------------------------------------------------------------------------
# Tests: Overture Maps category mapping
# ---------------------------------------------------------------------------


class TestCategoryMapping:
    def test_restaurant_maps_to_restaurant(self):
        assert _CATEGORY_TO_INDUSTRY["restaurant"] == "restaurant"

    def test_dentist_maps_to_healthcare(self):
        assert _CATEGORY_TO_INDUSTRY["dentist"] == "healthcare"

    def test_hair_salon_maps_to_beauty(self):
        assert _CATEGORY_TO_INDUSTRY["hair_salon"] == "beauty"

    def test_gym_maps_to_fitness(self):
        assert _CATEGORY_TO_INDUSTRY["gym"] == "fitness"

    def test_lawyer_maps_to_legal(self):
        assert _CATEGORY_TO_INDUSTRY["lawyer"] == "legal"

    def test_plumber_maps_to_home_services(self):
        assert _CATEGORY_TO_INDUSTRY["plumber"] == "home_services"

    def test_hotel_maps_to_hospitality(self):
        assert _CATEGORY_TO_INDUSTRY["hotel"] == "hospitality"

    def test_reverse_mapping_completeness(self):
        """Every FORGE industry should have at least one category."""
        all_industries = set(_CATEGORY_TO_INDUSTRY.values())
        for ind in all_industries:
            assert ind in _INDUSTRY_TO_CATEGORIES
            assert len(_INDUSTRY_TO_CATEGORIES[ind]) >= 1

    def test_category_count(self):
        """Should have a substantial number of category mappings."""
        assert len(_CATEGORY_TO_INDUSTRY) > 40


# ---------------------------------------------------------------------------
# Tests: Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_miles_to_degrees_reasonable(self):
        # At mid-latitudes, 1 mile ~ 0.0145 degrees
        assert 0.01 < MILES_TO_DEGREES < 0.02

    def test_overture_error_is_exception(self):
        with pytest.raises(OvertureDiscoveryError):
            raise OvertureDiscoveryError("test error")
