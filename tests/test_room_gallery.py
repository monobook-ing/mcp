import os
import unittest
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")

import server


class FakeResponse:
    def __init__(self, data):
        self.data = data


class FakeSingleQuery:
    """Simulates .single() returning a single dict."""
    def __init__(self, data):
        self._data = data

    def execute(self):
        return FakeResponse(self._data)


class FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def select(self, _):
        return self

    def eq(self, *_):
        return self

    def ilike(self, *_):
        return self

    def single(self):
        if self._rows:
            return FakeSingleQuery(self._rows[0])
        return FakeSingleQuery(None)

    def execute(self):
        return FakeResponse(self._rows)


class FakeSupabase:
    def __init__(self, rows):
        self._rows = rows

    def table(self, _):
        return FakeQuery(self._rows)


def make_room(
    *,
    room_id="1685a254-bdf7-4b57-882b-9d9691b10fad",
    name="Sova House - Height 890",
    room_type="Cottage",
    images=None,
    city="Tirol",
    state="Lvivska",
    country="Ukraine",
):
    return {
        "id": room_id,
        "name": name,
        "type": room_type,
        "description": "A cozy cottage",
        "price_per_night": "180.00",
        "currency_code": "USD",
        "max_guests": 6,
        "bed_config": "2 bedrooms · 2 beds · 2 baths",
        "images": images if images is not None else [
            "https://example.com/img1.jpg",
            "https://example.com/img2.jpg",
            "https://example.com/img3.jpg",
        ],
        "amenities": ["Kitchen", "Wifi"],
        "properties": {
            "city": city,
            "state": state,
            "country": country,
        },
    }


class RoomGalleryTests(unittest.TestCase):
    def run_gallery(self, rows, **kwargs):
        fake = FakeSupabase(rows=rows)
        with patch.object(server, "supabase", fake):
            return server.room_gallery(**kwargs)

    def test_lookup_by_uuid(self):
        room = make_room()
        result = self.run_gallery([room], room_id=room["id"])
        sc = result["structuredContent"]
        self.assertEqual(sc["room_id"], room["id"])
        self.assertEqual(sc["room_name"], "Sova House - Height 890")
        self.assertEqual(sc["image_count"], 3)
        self.assertEqual(len(sc["images"]), 3)

    def test_lookup_by_name(self):
        room = make_room()
        result = self.run_gallery([room], room_name="Sova House - Height 890")
        sc = result["structuredContent"]
        self.assertEqual(sc["room_name"], "Sova House - Height 890")
        self.assertEqual(sc["images"], room["images"])

    def test_returns_widget_meta(self):
        room = make_room()
        result = self.run_gallery([room], room_id=room["id"])
        meta = result["_meta"]
        self.assertEqual(meta["ui"]["resourceUri"], server.ROOM_GALLERY_WIDGET_URI)
        self.assertEqual(meta["openai/outputTemplate"], server.ROOM_GALLERY_WIDGET_URI)
        self.assertTrue(meta["openai/widgetAccessible"])

    def test_images_in_structured_content(self):
        imgs = ["https://example.com/a.jpg", "https://example.com/b.jpg"]
        room = make_room(images=imgs)
        result = self.run_gallery([room], room_id=room["id"])
        sc = result["structuredContent"]
        self.assertEqual(sc["images"], imgs)
        self.assertEqual(sc["image_count"], 2)

    def test_empty_images(self):
        room = make_room(images=[])
        result = self.run_gallery([room], room_id=room["id"])
        sc = result["structuredContent"]
        self.assertEqual(sc["images"], [])
        self.assertEqual(sc["image_count"], 0)

    def test_error_when_no_id_or_name(self):
        with self.assertRaises(ValueError):
            self.run_gallery([], room_id="", room_name="")

    def test_error_when_room_not_found(self):
        with self.assertRaises(ValueError):
            self.run_gallery([], room_name="Nonexistent Room")

    def test_location_in_structured_content(self):
        room = make_room(city="Tirol", state="Lvivska", country="Ukraine")
        result = self.run_gallery([room], room_id=room["id"])
        loc = result["structuredContent"]["location"]
        self.assertEqual(loc["city"], "Tirol")
        self.assertEqual(loc["state"], "Lvivska")
        self.assertEqual(loc["country"], "Ukraine")


if __name__ == "__main__":
    unittest.main()
