import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")

import server


class FakeResponse:
    def __init__(self, data):
        self.data = data


class FakeQuery:
    def __init__(self, responses):
        self._responses = responses

    def select(self, _):
        return self

    def ilike(self, *_):
        return self

    def lte(self, *_):
        return self

    def gte(self, *_):
        return self

    def contains(self, *_):
        raise AssertionError("contains() should not be used for amenities filtering")

    def execute(self):
        if not self._responses:
            raise AssertionError("No fake responses left for execute()")
        return FakeResponse(self._responses.pop(0))


class FakeSupabase:
    def __init__(self, responses):
        self._responses = list(responses)

    def table(self, _):
        return FakeQuery(self._responses)


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
        "mvp_accommodation": {
            "name": "Vysota 890",
            "city": city,
            "state": state,
            "country": country,
            "rating": "4.9",
            "image_url": "https://example.com/hotel.jpg",
            "lat": 49.0,
            "lng": 23.0,
        },
    }


class SearchRoomsTests(unittest.TestCase):
    def run_search(self, responses, **kwargs):
        fake = FakeSupabase(responses=responses)
        with patch.object(server, "supabase", fake):
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


if __name__ == "__main__":
    unittest.main()
