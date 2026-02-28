import os
import re
import uuid
import json
import logging
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Optional
from urllib import request, error

from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles

load_dotenv()

from fastmcp import FastMCP

from db import supabase

# ── Server ──────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

mcp = FastMCP(
    name="vysota890-booking",
)

# ── Widget loader ───────────────────────────────────────────────────────────

WIDGETS_DIR = Path(__file__).parent / "widgets"
RESOURCE_MIME = "text/html;profile=mcp-app"


@lru_cache(maxsize=None)
def load_widget(name: str) -> str:
    return (WIDGETS_DIR / f"{name}.html").read_text(encoding="utf-8")


def create_app():
    app = mcp.http_app(stateless_http=True)
    app.mount("/widgets", StaticFiles(directory=str(WIDGETS_DIR)), name="widgets")
    return app


app = create_app()

UNIT_CARD_WIDGET_URI = "ui://widget/unit-card.html"
BOOKING_FORM_WIDGET_URI = "ui://widget/booking-form.html"
BOOKING_CONFIRMATION_WIDGET_URI = "ui://widget/booking-confirmation.html"

MONOSEND_EMAILS_URL = "https://api.monosend.io/emails"
MONOSEND_TEMPLATE_ID = "7bc5aec5-cf40-4bec-88c5-d1c00b611fde"
MONOSEND_FROM_EMAIL = "noreply@monosend.email"
MONOSEND_API_KEY = os.getenv("API_KEY", "mono_bYguadb30GKyZ49gv0MxnJdgzG2xpHupQNw3Szbf87o")
MONOSEND_TIMEOUT_SECONDS = 10


def _build_monosend_payload(
    *,
    guest_email: str,
    hotel_name: str,
    unit_name: str,
    confirmation_code: str,
    guest_name: str,
    guest_phone: str,
    guests: int,
    check_in: date,
    check_out: date,
    total_price: float,
    currency_code: str,
) -> dict:
    first_name = guest_name.strip().split(" ", 1)[0] if guest_name.strip() else guest_name
    return {
        "to": [guest_email],
        "from": MONOSEND_FROM_EMAIL,
        "subject": f"Thanks! Your booking is confirmed at {hotel_name}",
        "template": {
            "id": MONOSEND_TEMPLATE_ID,
            "variables": {
                "hotel_unit_title": unit_name,
                "bookingNumber": confirmation_code,
                "firstName": first_name or guest_name,
                "email": guest_email,
                "phoneNumber": guest_phone,
                "guestCount": str(guests),
                "checkIn": check_in.isoformat(),
                "checkOut": check_out.isoformat(),
                "total": f"{total_price:.2f} {currency_code}",
                "companyName": hotel_name,
            },
        },
    }


def _send_booking_confirmation_email(
    *,
    guest_email: str,
    hotel_name: str,
    unit_name: str,
    confirmation_code: str,
    guest_name: str,
    guest_phone: str,
    guests: int,
    check_in: date,
    check_out: date,
    total_price: float,
    currency_code: str,
) -> None:
    payload = _build_monosend_payload(
        guest_email=guest_email,
        hotel_name=hotel_name,
        unit_name=unit_name,
        confirmation_code=confirmation_code,
        guest_name=guest_name,
        guest_phone=guest_phone,
        guests=guests,
        check_in=check_in,
        check_out=check_out,
        total_price=total_price,
        currency_code=currency_code,
    )
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        MONOSEND_EMAILS_URL,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {MONOSEND_API_KEY}",
            "Content-Type": "application/json",
        },
    )

    try:
        with request.urlopen(req, timeout=MONOSEND_TIMEOUT_SECONDS) as resp:
            status = getattr(resp, "status", resp.getcode())
            response_body = resp.read().decode("utf-8", errors="replace")
            if status < 200 or status >= 300:
                raise RuntimeError(
                    f"Monosend returned HTTP {status}: {response_body[:500]}"
                )
    except error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Monosend returned HTTP {exc.code}: {err_body[:500]}"
        ) from exc
    except error.URLError as exc:
        raise RuntimeError(f"Monosend request failed: {exc}") from exc


# ── Resources (UI templates) ───────────────────────────────────────────────

@mcp.resource(
    uri=UNIT_CARD_WIDGET_URI,
    name="Unit Card Widget",
    description="Displays hotel room/unit cards with image, amenities, price, and Reserve button",
    mime_type=RESOURCE_MIME,
    meta={
        "openai/widgetDescription": "Interactive room cards with a Reserve button. User-triggered booking actions happen inside this widget; assistant should not invoke booking tools directly.",
        "openai/widgetPrefersBorder": False,
    },
)
def unit_card_resource() -> str:
    return load_widget("unit_card")


@mcp.resource(
    uri=BOOKING_FORM_WIDGET_URI,
    name="Booking Form Widget",
    description="Booking form with guest details fields and price breakdown",
    mime_type=RESOURCE_MIME,
    meta={
        "openai/widgetDescription": "Booking form to collect guest details before confirmation. User-triggered booking actions happen inside this widget; assistant should not invoke booking tools directly.",
        "openai/widgetPrefersBorder": False,
    },
)
def booking_form_resource() -> str:
    return load_widget("booking_form")


@mcp.resource(
    uri=BOOKING_CONFIRMATION_WIDGET_URI,
    name="Booking Confirmation Widget",
    description="Confirmation page with booking details and calendar integration",
    mime_type=RESOURCE_MIME,
    meta={
        "openai/widgetDescription": "Booking confirmation details with reservation code and stay summary.",
        "openai/widgetPrefersBorder": False,
    },
)
def booking_confirmation_resource() -> str:
    return load_widget("booking_confirmation")


# ── Tool 1: search_hotels ──────────────────────────────────────────────────

@mcp.tool(
    annotations={"readOnlyHint": True, "openWorldHint": False},
)
def search_hotels(
    hotel_name: str = "",
    city: str = "",
    country: str = "",
    lat: Optional[float] = None,
    lng: Optional[float] = None,
) -> dict:
    """Search accommodations by hotel name, city, coordinates, or country.
    Returns matching hotels with basic info."""

    query = supabase.table("properties").select("*")

    if hotel_name:
        query = query.ilike("name", f"%{hotel_name}%")
    if city:
        query = query.ilike("city", f"%{city}%")
    if country:
        query = query.ilike("country", f"%{country}%")
    if lat is not None and lng is not None:
        delta = 0.5
        query = query.gte("lat", lat - delta).lte("lat", lat + delta)
        query = query.gte("lng", lng - delta).lte("lng", lng + delta)

    result = query.execute()
    hotels = result.data

    return {
        "hotels": hotels,
        "count": len(hotels),
    }


# ── Tool 2: search_rooms ──────────────────────────────────────────────────

@mcp.tool(
    annotations={"readOnlyHint": True, "openWorldHint": False},
    app={"resourceUri": UNIT_CARD_WIDGET_URI},
    meta={
        "openai/outputTemplate": UNIT_CARD_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def search_rooms(
    hotel_name: str = "",
    city: str = "",
    country: str = "",
    unit_type: str = "",
    max_price: Optional[float] = None,
    min_guests: Optional[int] = None,
    amenity: str = "",
    query: str = "",
    check_in: str = "",
    check_out: str = "",
) -> dict:
    """Search available rooms/units by hotel name, city, country, type, price,
    guest capacity, amenities like 'Hot tub', 'Sauna', 'Pool', or free-text query.
    Query matches unit name/description/type, hotel name, city/state/country, and amenities.
    Returns interactive room cards with Reserve button.
    Use check_in/check_out in YYYY-MM-DD format if dates are known."""

    normalized_query = " ".join(re.split(r"\s+", str(query or "").strip().lower()))
    normalized_amenity = " ".join(re.split(r"\s+", str(amenity or "").strip().lower()))

    def normalize_text(value: object) -> str:
        if value is None:
            return ""
        return " ".join(re.split(r"\s+", str(value).strip().lower()))

    def normalize_amenities(raw_amenities: object) -> list[str]:
        if isinstance(raw_amenities, list):
            return [str(item) for item in raw_amenities if item is not None]
        if raw_amenities is None:
            return []
        return [str(raw_amenities)]

    def unit_matches_text_filters(unit: dict) -> bool:
        accommodation = unit.get("properties")
        if not isinstance(accommodation, dict):
            accommodation = {}

        amenities_list = normalize_amenities(unit.get("amenities"))
        amenities_blob = normalize_text(" ".join(amenities_list))
        searchable_blob = normalize_text(
            " ".join(
                [
                    str(unit.get("name") or ""),
                    str(unit.get("description") or ""),
                    str(unit.get("type") or ""),
                    str(accommodation.get("name") or ""),
                    str(accommodation.get("city") or ""),
                    str(accommodation.get("state") or ""),
                    str(accommodation.get("country") or ""),
                    " ".join(amenities_list),
                ]
            )
        )

        if normalized_query and normalized_query not in searchable_blob:
            return False
        if normalized_amenity and normalized_amenity not in amenities_blob:
            return False
        return True

    def build_query(include_country: bool):
        q = supabase.table("rooms").select(
            "*, properties!inner(city, state, country, rating, image_url, lat, lng)"
        )

        if hotel_name:
            q = q.ilike("properties.city", f"%{hotel_name}%")
        if city:
            q = q.ilike("properties.city", f"%{city}%")
        if include_country and country:
            q = q.ilike("properties.country", f"%{country}%")
        if unit_type:
            q = q.ilike("type", f"%{unit_type}%")
        if max_price is not None:
            q = q.lte("price_per_night", max_price)
        if min_guests is not None:
            q = q.gte("max_guests", min_guests)

        return q

    result = build_query(include_country=True).execute()
    units = [u for u in (result.data or []) if unit_matches_text_filters(u)]
    relaxed_country_filter = False

    # If city+country yields nothing, retry city-only for region ambiguities
    if not units and city and country:
        relaxed_units = build_query(include_country=False).execute().data or []
        relaxed_units = [u for u in relaxed_units if unit_matches_text_filters(u)]
        if relaxed_units:
            units = relaxed_units
            relaxed_country_filter = True

    def parse_rating(value):
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def get_cancel_date_text(raw_check_in: str) -> str:
        if not raw_check_in:
            return "14 days before check-in"
        try:
            check_in_date = date.fromisoformat(raw_check_in)
            cancel_date = check_in_date - timedelta(days=14)
            return f"{cancel_date.strftime('%B')} {cancel_date.day}"
        except ValueError:
            return "14 days before check-in"

    def derive_safety(amenities: list[str]) -> str:
        amenity_blob = " ".join(str(a).lower() for a in amenities)
        lines = []
        if "smoke alarm not reported" in amenity_blob:
            lines.append("Smoke alarm not reported")
        elif "smoke alarm" in amenity_blob:
            lines.append("Smoke alarm available")

        if "carbon monoxide alarm" in amenity_blob:
            lines.append("Carbon monoxide alarm available")
        if "camera" in amenity_blob or "security camera" in amenity_blob:
            lines.append("Exterior security cameras on property")
        if "fire extinguisher" in amenity_blob:
            lines.append("Fire extinguisher available")
        if "first aid kit" in amenity_blob:
            lines.append("First aid kit available")

        if not lines:
            return "No special safety notes provided by host."
        return "\n".join(lines)

    structured_units = []
    for u in units:
        accommodation = u.get("properties")
        if not isinstance(accommodation, dict):
            accommodation = {}

        amenities_list = normalize_amenities(u.get("amenities"))
        images = u.get("images")
        if not isinstance(images, list):
            images = [str(images)] if images else []

        rating = parse_rating(accommodation.get("rating"))
        structured_units.append(
            {
                "id": u.get("id"),
                "name": u.get("name"),
                "type": u.get("type"),
                "description": u.get("description"),
                "price_per_night": float(u.get("price_per_night") or 0),
                "currency_code": u.get("currency_code"),
                "max_guests": u.get("max_guests"),
                "bed_config": u.get("bed_config"),
                "image_url": images[0] if images else "",
                "images": images,
                "amenities": amenities_list,
                "hotel_name": accommodation.get("name"),
                "hotel_rating": accommodation.get("rating"),
                "location": {
                    "city": accommodation.get("city"),
                    "state": accommodation.get("state"),
                    "country": accommodation.get("country"),
                    "lat": accommodation.get("lat"),
                    "lng": accommodation.get("lng"),
                },
                "review_summary": {
                    "rating": rating,
                    "review_count": 0,
                    "text": f"{rating:.2f}" if rating is not None else "No reviews yet",
                },
                "host": {
                    "name": accommodation.get("name") or "Host",
                    "years_hosting": 1,
                    "response_time": "Responds within a few days or more",
                    "is_superhost": bool((rating or 0) >= 4.8),
                    "avatar_url": accommodation.get("image_url") or (images[0] if images else ""),
                },
                "things_to_know": {
                    "cancellation": (
                        "Free cancellation until "
                        f"{get_cancel_date_text(check_in)} (local time). "
                        "After that, cancellation may be non-refundable depending on host rules."
                    ),
                    "house_rules": (
                        "Check-in after 3:00 PM\n"
                        "Checkout before 11:00 AM\n"
                        f"Maximum guests: {u.get('max_guests') or 1}"
                    ),
                    "safety": derive_safety(amenities_list),
                },
                "extended_reviews": [],
                "map": {
                    "lat": accommodation.get("lat"),
                    "lng": accommodation.get("lng"),
                    "label": ", ".join(
                        [
                            p
                            for p in [
                                accommodation.get("city"),
                                accommodation.get("state"),
                                accommodation.get("country"),
                            ]
                            if p
                        ]
                    ),
                },
            }
        )

    structured = {
        "units": structured_units,
        "count": len(units),
        "check_in": check_in,
        "check_out": check_out,
        "relaxed_country_filter": relaxed_country_filter,
    }

    return {
        "content": [
            {
                "type": "text",
                "text": f"Found {len(units)} room(s). If the user asks to reserve, call the book tool with the appropriate unit and dates.",
            }
        ],
        "structuredContent": structured,
        "_meta": {
            "ui": {"resourceUri": UNIT_CARD_WIDGET_URI},
            "openai/outputTemplate": UNIT_CARD_WIDGET_URI,
            "openai/widgetAccessible": True,
        },
    }


# ── Tool 3: book ──────────────────────────────────────────────────────────

@mcp.tool(
    annotations={
        "title": "Open booking form",
        "readOnlyHint": True,
        "openWorldHint": False,
    },
    app={"resourceUri": BOOKING_FORM_WIDGET_URI},
    meta={
        "openai/outputTemplate": BOOKING_FORM_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def book(
    check_in: date,
    check_out: date,
    unit_id: str = "",
    unit_name: str = "",
    hotel_name: str = "",
    guests: int = 2,
) -> dict:
    """Open a booking form for the given unit/room.
    Can be called when the user requests a reservation from chat,
    or when they click Reserve in the room card."""

    unit = None
    looks_like_uuid = bool(
        unit_id
        and len(unit_id) == 36
        and unit_id.count("-") == 4
    )

    # Backward-compatible lookup path for older clients that still send unit_id.
    if looks_like_uuid:
        unit_result = (
            supabase.table("rooms")
            .select("*, properties!inner(*)")
            .eq("id", unit_id)
            .single()
            .execute()
        )
        unit = unit_result.data
    else:
        lookup_name = (unit_name or unit_id).strip()
        if not lookup_name:
            raise ValueError("Either unit_name or unit_id is required.")

        unit_query = (
            supabase.table("rooms")
            .select("*, properties!inner(*)")
            .eq("name", lookup_name)
        )
        if hotel_name:
            unit_query = unit_query.eq("properties.city", hotel_name)

        unit_rows = unit_query.execute().data or []
        if not unit_rows:
            raise ValueError(
                f"Unable to find unit '{lookup_name}'"
                + (f" at hotel '{hotel_name}'." if hotel_name else ".")
            )
        unit = unit_rows[0]

    check_in_date = check_in
    check_out_date = check_out
    nights = (check_out_date - check_in_date).days
    total = float(unit["price_per_night"]) * nights

    structured = {
        "unit_id": unit["id"],
        "unit_name": unit["name"],
        "hotel_name": unit["properties"].get("city", ""),
        "check_in": check_in_date.isoformat(),
        "check_out": check_out_date.isoformat(),
        "nights": nights,
        "guests": guests,
        "price_per_night": float(unit["price_per_night"]),
        "currency_code": unit["currency_code"],
        "total_price": total,
        "image_url": unit["images"][0] if unit.get("images") else "",
        "rating": unit["properties"]["rating"],
    }

    return {
        "content": [
            {
                "type": "text",
                "text": "Booking form opened. User should complete details and confirm inside the widget (click Confirm or press Enter in a form field).",
            }
        ],
        "structuredContent": structured,
        "_meta": {
            "ui": {"resourceUri": BOOKING_FORM_WIDGET_URI},
            "openai/outputTemplate": BOOKING_FORM_WIDGET_URI,
            "openai/widgetAccessible": True,
        },
    }


# ── Tool 4: book_confirm ─────────────────────────────────────────────────

@mcp.tool(
    annotations={
        "title": "Confirm booking (widget submit only)",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False,
    },
    app={"resourceUri": BOOKING_CONFIRMATION_WIDGET_URI},
    meta={
        "openai/outputTemplate": BOOKING_CONFIRMATION_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def book_confirm(
    unit_id: str,
    check_in: date,
    check_out: date,
    guests: int,
    guest_name: str,
    guest_email: str,
    guest_phone: str,
    total_price: float,
    currency_code: str = "USD",
    unit_name: str = "",
) -> dict:
    """Confirm a booking with guest details and finalize the reservation.
    Can be called from the widget form submission or directly from chat."""

    # Get unit info (moved up so property_id is available for guest upsert)
    unit = (
        supabase.table("rooms")
        .select("*, properties!inner(*)")
        .eq("id", unit_id)
        .single()
        .execute()
        .data
    )
    if unit_name and unit_name.strip() != unit["name"]:
        raise ValueError(
            f"Unit name mismatch for unit_id '{unit_id}': expected '{unit['name']}', got '{unit_name.strip()}'."
        )

    property_id = unit["property_id"]

    # Upsert guest. Supabase may return None or a result object with dict/list data.
    existing = (
        supabase.table("guests")
        .select("id")
        .eq("email", guest_email)
        .eq("property_id", property_id)
        .maybe_single()
        .execute()
    )
    existing_data = existing.data if existing is not None else None
    if isinstance(existing_data, list):
        existing_data = existing_data[0] if existing_data else None

    if existing_data:
        guest_id = existing_data["id"]
        supabase.table("guests").update(
            {"name": guest_name, "phone": guest_phone, "updated_at": "now()"}
        ).eq("id", guest_id).execute()
    else:
        guest_insert = (
            supabase.table("guests")
            .insert({"property_id": property_id, "name": guest_name, "email": guest_email, "phone": guest_phone})
            .execute()
        )
        inserted_rows = guest_insert.data if guest_insert is not None else None
        if not inserted_rows:
            raise RuntimeError("Failed to create guest record for booking confirmation.")
        guest_id = inserted_rows[0]["id"]

    # Generate confirmation code
    confirmation_code = f"BK-{uuid.uuid4().hex[:6].upper()}"

    # Create reservation
    check_in_date = check_in
    check_out_date = check_out
    nights = (check_out_date - check_in_date).days

    supabase.table("bookings").insert(
        {
            "property_id": property_id,
            "room_id": unit_id,
            "guest_id": guest_id,
            "check_in": check_in_date.isoformat(),
            "check_out": check_out_date.isoformat(),
            "guests_count": guests,
            "total_price": total_price,
            "currency_code": currency_code,
            "status": "confirmed",
            "ai_handled": True,
            "source": "chatgpt",
        }
    ).execute()

    structured = {
        "confirmation_code": confirmation_code,
        "status": "confirmed",
        "unit_name": unit["name"],
        "hotel_name": unit["properties"].get("city", ""),
        "check_in": check_in_date.isoformat(),
        "check_out": check_out_date.isoformat(),
        "check_in_time": "15:00",
        "check_out_time": "11:00",
        "nights": nights,
        "guests": guests,
        "total_price": total_price,
        "currency_code": currency_code,
        "guest_name": guest_name,
        "guest_email": guest_email,
        "guest_phone": guest_phone,
        "image_url": unit["images"][0] if unit.get("images") else "",
    }

    try:
        _send_booking_confirmation_email(
            guest_email=guest_email,
            hotel_name=unit["properties"].get("city", ""),
            unit_name=unit["name"],
            confirmation_code=confirmation_code,
            guest_name=guest_name,
            guest_phone=guest_phone,
            guests=guests,
            check_in=check_in_date,
            check_out=check_out_date,
            total_price=total_price,
            currency_code=currency_code,
        )
        logger.info(
            "Monosend booking confirmation email sent for %s to %s.",
            confirmation_code,
            guest_email,
        )
    except Exception as exc:
        logger.warning(
            "Monosend booking confirmation email failed for %s to %s: %s",
            confirmation_code,
            guest_email,
            exc,
        )

    return {
        "content": [
            {
                "type": "text",
                "text": f"Booking confirmed. Code: {confirmation_code}.",
            }
        ],
        "structuredContent": structured,
        "_meta": {
            "ui": {"resourceUri": BOOKING_CONFIRMATION_WIDGET_URI},
            "openai/outputTemplate": BOOKING_CONFIRMATION_WIDGET_URI,
            "openai/widgetAccessible": True,
        },
    }


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
