import os
import unittest
from datetime import date
from unittest.mock import patch

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")

import server


class FakeResponse:
    def __init__(self, data):
        self.data = data


class FakeTableQuery:
    def __init__(self, table, state):
        self.table = table
        self.state = state
        self.payload = None
        self.filters = {}

    def select(self, _):
        return self

    def eq(self, key, value):
        self.filters[key] = value
        return self

    def maybe_single(self):
        self.state["guest_lookup_email"] = self.filters.get("email")
        return self

    def single(self):
        self.state["unit_lookup_id"] = self.filters.get("id")
        return self

    def update(self, payload):
        self.payload = payload
        return self

    def insert(self, payload):
        self.payload = payload
        if self.table == "mvp_reservation":
            self.state["reservation_insert_payload"] = payload
        return self

    def execute(self):
        if self.table == "mvp_guest":
            if self.payload is None:
                data = {"id": self.state["existing_guest_id"]} if self.state["existing_guest_id"] else None
                return FakeResponse(data)
            if "email" in self.payload:
                self.state["guest_insert_payload"] = self.payload
                return FakeResponse([{"id": self.state["inserted_guest_id"]}])
            self.state["guest_update_payload"] = self.payload
            self.state["guest_update_id"] = self.filters.get("id")
            return FakeResponse([])

        if self.table == "mvp_unit":
            return FakeResponse(self.state["unit"])

        if self.table == "mvp_reservation":
            return FakeResponse([{"ok": True}])

        raise AssertionError(f"Unexpected table {self.table}")


class FakeSupabase:
    def __init__(self, *, existing_guest_id=None):
        self.state = {
            "existing_guest_id": existing_guest_id,
            "inserted_guest_id": "guest-new-1",
            "unit": {
                "id": "unit-1",
                "name": "Sova House",
                "property_id": "acc-1",
                "images": ["https://example.com/cover.jpg"],
                "mvp_accommodation": {"name": "Vysota 890"},
            },
        }

    def table(self, name):
        return FakeTableQuery(name, self.state)


class BookConfirmEmailTests(unittest.TestCase):
    def call_book_confirm(self):
        return server.book_confirm(
            unit_id="unit-1",
            check_in=date(2026, 3, 10),
            check_out=date(2026, 3, 12),
            guests=3,
            guest_name="John Doe",
            guest_email="john@example.com",
            guest_phone="+1234567890",
            total_price=450.0,
            currency_code="USD",
            unit_name="Sova House",
        )

    def test_email_success_path_and_args(self):
        fake = FakeSupabase()
        with patch.object(server, "supabase", fake), patch.object(
            server, "_send_booking_confirmation_email"
        ) as send_email:
            result = self.call_book_confirm()

        self.assertEqual(result["structuredContent"]["status"], "confirmed")
        send_email.assert_called_once()
        kwargs = send_email.call_args.kwargs
        self.assertEqual(kwargs["guest_email"], "john@example.com")
        self.assertEqual(kwargs["hotel_name"], "Vysota 890")
        self.assertEqual(kwargs["unit_name"], "Sova House")
        self.assertEqual(kwargs["guests"], 3)
        self.assertEqual(kwargs["check_in"], date(2026, 3, 10))
        self.assertEqual(kwargs["check_out"], date(2026, 3, 12))

    def test_email_failure_is_non_blocking(self):
        fake = FakeSupabase()
        with patch.object(server, "supabase", fake), patch.object(
            server, "_send_booking_confirmation_email", side_effect=RuntimeError("email down")
        ):
            result = self.call_book_confirm()

        self.assertEqual(result["structuredContent"]["status"], "confirmed")
        self.assertTrue(result["structuredContent"]["confirmation_code"].startswith("BK-"))

    def test_template_payload_mapping(self):
        payload = server._build_monosend_payload(
            guest_email="alice@example.com",
            hotel_name="Vysota 890",
            unit_name="Leleka",
            confirmation_code="BK-ABC123",
            guest_name="Alice Smith",
            guest_phone="+1987654321",
            guests=2,
            check_in=date(2026, 4, 1),
            check_out=date(2026, 4, 4),
            total_price=399.9,
            currency_code="USD",
        )

        self.assertEqual(payload["to"], ["alice@example.com"])
        self.assertEqual(payload["subject"], "Thanks! Your booking is confirmed at Vysota 890")
        vars_ = payload["template"]["variables"]
        self.assertEqual(vars_["hotel_unit_title"], "Leleka")
        self.assertEqual(vars_["bookingNumber"], "BK-ABC123")
        self.assertEqual(vars_["firstName"], "Alice")
        self.assertEqual(vars_["email"], "alice@example.com")
        self.assertEqual(vars_["phoneNumber"], "+1987654321")
        self.assertEqual(vars_["guestCount"], "2")
        self.assertEqual(vars_["checkIn"], "2026-04-01")
        self.assertEqual(vars_["checkOut"], "2026-04-04")
        self.assertEqual(vars_["total"], "399.90 USD")
        self.assertEqual(vars_["companyName"], "Vysota 890")

    def test_reservation_insert_and_response_regression(self):
        fake = FakeSupabase(existing_guest_id="guest-existing-1")
        with patch.object(server, "supabase", fake), patch.object(
            server, "_send_booking_confirmation_email"
        ):
            result = self.call_book_confirm()

        reservation_payload = fake.state["reservation_insert_payload"]
        self.assertEqual(reservation_payload["unit_id"], "unit-1")
        self.assertEqual(reservation_payload["guests_count"], 3)
        self.assertEqual(reservation_payload["total_price"], 450.0)
        self.assertEqual(reservation_payload["currency_code"], "USD")
        self.assertEqual(reservation_payload["status"], "confirmed")

        structured = result["structuredContent"]
        self.assertEqual(structured["unit_name"], "Sova House")
        self.assertEqual(structured["guest_name"], "John Doe")
        self.assertEqual(structured["guest_email"], "john@example.com")
        self.assertEqual(structured["guest_phone"], "+1234567890")
        self.assertEqual(structured["check_in"], "2026-03-10")
        self.assertEqual(structured["check_out"], "2026-03-12")
        self.assertEqual(structured["total_price"], 450.0)


if __name__ == "__main__":
    unittest.main()
