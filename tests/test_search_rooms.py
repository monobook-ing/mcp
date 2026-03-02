import os
import unittest
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

import sys
from unittest.mock import patch, MagicMock

# Mock psycopg2 pool before db/server imports
sys.modules['psycopg2'] = MagicMock()
sys.modules['psycopg2.pool'] = MagicMock()
sys.modules['psycopg2.extras'] = MagicMock()

import server


import db

def fake_fetch_all(responses):
    def fetch_all(sql, params=None):
        if not responses:
            raise AssertionError("No fake responses left for fetch_all()")
        return responses.pop(0)
    return fetch_all


def make_unit(
    *,
    unit_id="u1",
    name="Sova House",
    unit_type="Home",
    description="Cozy mountain stay",
    amenities=None,
    city="Volosianka",
    state="Lviv",
    country="Ukraine",
):
    return {
        "id": unit_id,
        "name": name,
        "type": unit_type,
        "description": description,
        "price_per_night": "180.00",
        "currency_code": "USD",
        "max_guests": 4,
        "bed_config": "2 beds",
        "images": ["https://example.com/1.jpg"],
        "amenities": amenities if amenities is not None else ["Pool", "Hot tub", "Sauna"],
        "p_name": "Vysota 890",
        "p_city": city,
        "p_state": state,
        "p_country": country,
        "p_rating": "4.9",
        "p_image_url": "https://example.com/hotel.jpg",
        "p_lat": 49.0,
        "p_lng": 23.0,
    }


class SearchRoomsTests(unittest.TestCase):
    def run_search(self, responses, **kwargs):
        kwargs.setdefault("show_occupied", True)
        with patch.object(server, "fetch_all", side_effect=fake_fetch_all(list(responses))):
            return server.search_rooms(**kwargs)

    def test_regression_no_jsonb_contains_error(self):
        result = self.run_search(
            responses=[[make_unit()]],
            amenity="Hot tub",
            query="hot",
        )
        self.assertEqual(result["structuredContent"]["count"], 1)

    def test_amenity_fuzzy_match(self):
        result = self.run_search(
            responses=[[make_unit(amenities=["Wifi", "Hot tub"])]],
            amenity="hot",
        )
        self.assertEqual(result["structuredContent"]["count"], 1)

    def test_query_matches_hotel_location_fields(self):
        result = self.run_search(
            responses=[[make_unit(city="Volosianka")]],
            query="volosianka",
        )
        self.assertEqual(result["structuredContent"]["count"], 1)

    def test_query_matches_room_fields(self):
        result = self.run_search(
            responses=[[make_unit(name="Presidential Penthouse", unit_type="Penthouse")]],
            query="penthouse",
        )
        self.assertEqual(result["structuredContent"]["count"], 1)

    def test_combined_structured_and_text_filters_use_and_logic(self):
        first = make_unit(unit_id="u1", city="Volosianka", amenities=["Pool"])
        second = make_unit(unit_id="u2", city="Volosianka", amenities=["Sauna"])
        result = self.run_search(
            responses=[[first, second]],
            city="Volosianka",
            query="pool",
        )
        units = result["structuredContent"]["units"]
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]["id"], "u1")

    def test_relaxed_country_filter_with_text_match(self):
        relaxed_match = make_unit(country="Ukraine")
        result = self.run_search(
            responses=[[], [relaxed_match]],
            city="Volosianka",
            country="Poland",
            query="volosianka",
        )
        self.assertEqual(result["structuredContent"]["count"], 1)
        self.assertTrue(result["structuredContent"]["relaxed_country_filter"])

    def test_backward_compatible_structured_only_search(self):
        result = self.run_search(
            responses=[[make_unit()]],
            city="Volosianka",
            country="Ukraine",
        )
        self.assertIn("structuredContent", result)
        self.assertEqual(result["structuredContent"]["count"], 1)
        self.assertIn("units", result["structuredContent"])

    def test_cyrillic_query_falls_back_to_sql_results(self):
        first = make_unit(unit_id="u1", unit_type="Cottage")
        second = make_unit(unit_id="u2", unit_type="Cottage")
        result = self.run_search(
            responses=[[first, second]],
            city="Волосянка",
            country="Ukraine",
            unit_type="cottage",
            query="котедж",
            show_occupied=True,
        )
        self.assertEqual(result["structuredContent"]["count"], 2)

    def test_cyrillic_query_with_amenity_still_filters(self):
        with_hot_tub = make_unit(unit_id="u1", unit_type="Cottage", amenities=["Wifi", "Hot tub"])
        no_hot_tub = make_unit(unit_id="u2", unit_type="Cottage", amenities=["Wifi"])
        result = self.run_search(
            responses=[[with_hot_tub, no_hot_tub]],
            city="Волосянка",
            country="Ukraine",
            unit_type="cottage",
            query="котедж",
            amenity="hot tub",
            show_occupied=True,
        )
        units = result["structuredContent"]["units"]
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]["id"], "u1")

    def test_latin_query_still_filters_normally(self):
        cottage = make_unit(unit_id="u1", unit_type="Cottage", name="Forest Cottage")
        apartment = make_unit(unit_id="u2", unit_type="Apartment", name="City Apartment")
        result = self.run_search(
            responses=[[cottage, apartment]],
            city="Volosianka",
            country="Ukraine",
            query="cottage",
            show_occupied=True,
        )
        units = result["structuredContent"]["units"]
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]["id"], "u1")

    def test_cyrillic_query_relaxed_country_fallback(self):
        relaxed_match = make_unit(unit_id="u1", unit_type="Cottage", country="Ukraine")
        result = self.run_search(
            responses=[[], [relaxed_match]],
            city="Волосянка",
            country="Poland",
            unit_type="cottage",
            query="котедж",
            show_occupied=True,
        )
        self.assertEqual(result["structuredContent"]["count"], 1)
        self.assertTrue(result["structuredContent"]["relaxed_country_filter"])


if __name__ == "__main__":
    unittest.main()
