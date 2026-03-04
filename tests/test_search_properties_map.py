import os
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

import sys

# Mock psycopg2 pool before db/server imports
sys.modules["psycopg2"] = MagicMock()
sys.modules["psycopg2.pool"] = MagicMock()
sys.modules["psycopg2.extras"] = MagicMock()

import server

EXPECTED_MAP_FOLLOW_UP_INSTRUCTIONS = (
    "Very important: DO NOT LIST ANY HOTEL in your answer, "
    "its all filtered by availability and shown to the user."
)


def make_unit_row(
    *,
    unit_id: str = "u1",
    property_id: str = "p1",
    name: str = "JO&JOE - Private room",
    unit_type: str = "Hostel",
    description: str = "Hostel stay in Vienna",
    amenities=None,
    city: str = "Vienna",
    state: str = "Vienna",
    country: str = "Austria",
    lat=None,
    lng=None,
):
    return {
        "id": unit_id,
        "property_id": property_id,
        "name": name,
        "type": unit_type,
        "description": description,
        "images": ["https://example.com/room.jpg"],
        "price_per_night": "25.00",
        "currency_code": "USD",
        "max_guests": 8,
        "bed_config": "8 beds",
        "amenities": amenities if amenities is not None else ["Wifi", "Hot tub"],
        "p_name": "JO&JOE Vienna",
        "p_city": city,
        "p_state": state,
        "p_country": country,
        "p_rating": "4.6",
        "p_image_url": "https://example.com/hotel.jpg",
        "p_lat": lat,
        "p_lng": lng,
        "p_street": "Mariahilfer Strasse 1",
    }


class SearchPropertiesMapTests(unittest.TestCase):
    def test_coordinates_required_for_map_but_not_rooms(self):
        map_sql: list[str] = []

        def map_fetch_all(sql, params=None):
            map_sql.append(sql)
            return []

        with patch.object(server, "fetch_all", side_effect=map_fetch_all):
            map_result = server.search_properties_map(
                city="Vienna",
                country="Austria",
                unit_type="hostel",
                query="hostel",
            )

        self.assertEqual(map_result["structuredContent"]["count"], 0)
        self.assertTrue(map_result["structuredContent"]["coordinates_required"])
        self.assertIn("coordinates", map_result["content"][0]["text"].lower())
        self.assertEqual(
            map_result["follow_up_instructions"],
            EXPECTED_MAP_FOLLOW_UP_INSTRUCTIONS,
        )
        self.assertTrue(any("p.lat IS NOT NULL" in sql for sql in map_sql))
        self.assertTrue(any("p.lng IS NOT NULL" in sql for sql in map_sql))

        rooms_sql: list[str] = []
        row_without_coords = make_unit_row(lat=None, lng=None)

        def rooms_fetch_all(sql, params=None):
            rooms_sql.append(sql)
            return [row_without_coords]

        with patch.object(server, "fetch_all", side_effect=rooms_fetch_all):
            rooms_result = server.search_rooms(
                city="Vienna",
                country="Austria",
                unit_type="hostel",
                query="hostel",
                show_occupied=True,
            )

        self.assertEqual(rooms_result["structuredContent"]["count"], 1)
        self.assertFalse(any("p.lat IS NOT NULL" in sql for sql in rooms_sql))
        self.assertFalse(any("p.lng IS NOT NULL" in sql for sql in rooms_sql))

    def test_parity_with_search_rooms_when_coordinates_exist(self):
        matching = make_unit_row(
            unit_id="u1",
            property_id="p1",
            unit_type="Hostel",
            lat=48.2082,
            lng=16.3738,
        )
        non_matching = make_unit_row(
            unit_id="u2",
            property_id="p1",
            name="City Apartment",
            unit_type="Apartment",
            description="Apartment in Vienna",
            amenities=["Wifi"],
            lat=48.2082,
            lng=16.3738,
        )

        with patch.object(server, "fetch_all", side_effect=[[matching, non_matching]]):
            rooms_result = server.search_rooms(
                city="Vienna",
                country="Austria",
                query="hostel",
                show_occupied=True,
            )

        with patch.object(server, "fetch_all", side_effect=[[matching, non_matching], []]):
            map_result = server.search_properties_map(
                city="Vienna",
                country="Austria",
                query="hostel",
            )

        self.assertEqual(rooms_result["structuredContent"]["count"], 1)
        self.assertEqual(map_result["structuredContent"]["count"], 1)
        self.assertEqual(
            map_result["follow_up_instructions"],
            EXPECTED_MAP_FOLLOW_UP_INSTRUCTIONS,
        )
        self.assertFalse(map_result["structuredContent"]["relaxed_country_filter"])
        map_rooms = map_result["structuredContent"]["properties"][0]["rooms"]
        self.assertEqual(len(map_rooms), 1)
        self.assertEqual(map_rooms[0]["id"], "u1")
        self.assertEqual(rooms_result["structuredContent"]["units"][0]["id"], "u1")

    def test_map_relaxed_country_fallback(self):
        relaxed_match = make_unit_row(
            unit_id="u1",
            property_id="p1",
            city="Vienna",
            country="Austria",
            lat=48.2082,
            lng=16.3738,
        )

        with patch.object(server, "fetch_all", side_effect=[[], [relaxed_match], []]):
            result = server.search_properties_map(
                city="Vienna",
                country="Wrong Country",
                query="hostel",
            )

        self.assertEqual(result["structuredContent"]["count"], 1)
        self.assertTrue(result["structuredContent"]["relaxed_country_filter"])

    def test_query_and_amenity_filter_parity(self):
        with_hot_tub = make_unit_row(
            unit_id="u1",
            property_id="p1",
            unit_type="Cottage",
            amenities=["Wifi", "Hot tub"],
            lat=48.2082,
            lng=16.3738,
        )
        without_hot_tub = make_unit_row(
            unit_id="u2",
            property_id="p1",
            unit_type="Cottage",
            amenities=["Wifi"],
            lat=48.2082,
            lng=16.3738,
        )
        rows = [with_hot_tub, without_hot_tub]

        with patch.object(server, "fetch_all", side_effect=[rows]):
            rooms_result = server.search_rooms(
                city="Vienna",
                country="Austria",
                unit_type="cottage",
                query="котедж",
                amenity="hot tub",
                show_occupied=True,
            )

        with patch.object(server, "fetch_all", side_effect=[rows, []]):
            map_result = server.search_properties_map(
                city="Vienna",
                country="Austria",
                unit_type="cottage",
                query="котедж",
                amenity="hot tub",
            )

        self.assertEqual(rooms_result["structuredContent"]["count"], 1)
        self.assertEqual(rooms_result["structuredContent"]["units"][0]["id"], "u1")
        self.assertEqual(map_result["structuredContent"]["count"], 1)
        self.assertEqual(map_result["structuredContent"]["properties"][0]["rooms"][0]["id"], "u1")


if __name__ == "__main__":
    unittest.main()
