import os
import uuid
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles

load_dotenv()

from fastmcp import FastMCP

from db import supabase

# ── Server ──────────────────────────────────────────────────────────────────

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

    query = supabase.table("mvp_accommodation").select("*")

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
    check_in: str = "",
    check_out: str = "",
) -> dict:
    """Search available rooms/units by hotel name, city, country, type, price,
    guest capacity, or amenities like 'Hot tub', 'Sauna', 'Pool'.
    Returns interactive room cards with Reserve button.
    Use check_in/check_out in YYYY-MM-DD format if dates are known."""

    def build_query(include_country: bool):
        q = supabase.table("mvp_unit").select(
            "*, mvp_accommodation!inner(name, city, state, country, rating, image_url, lat, lng)"
        )

        if hotel_name:
            q = q.ilike("mvp_accommodation.name", f"%{hotel_name}%")
        if city:
            q = q.ilike("mvp_accommodation.city", f"%{city}%")
        if include_country and country:
            q = q.ilike("mvp_accommodation.country", f"%{country}%")
        if unit_type:
            q = q.ilike("type", f"%{unit_type}%")
        if max_price is not None:
            q = q.lte("price_per_night", max_price)
        if min_guests is not None:
            q = q.gte("max_guests", min_guests)
        if amenity:
            q = q.contains("amenities", [amenity])

        return q

    result = build_query(include_country=True).execute()
    units = result.data
    relaxed_country_filter = False

    # If city+country yields nothing, retry city-only for region ambiguities
    if not units and city and country:
        relaxed_units = build_query(include_country=False).execute().data
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

    structured = {
        "units": [
            {
                "id": u["id"],
                "name": u["name"],
                "type": u["type"],
                "description": u["description"],
                "price_per_night": float(u["price_per_night"]),
                "currency_code": u["currency_code"],
                "max_guests": u["max_guests"],
                "bed_config": u["bed_config"],
                "image_url": u["images"][0] if u.get("images") else "",
                "images": u.get("images", []),
                "amenities": u.get("amenities", []),
                "hotel_name": u["mvp_accommodation"]["name"],
                "hotel_rating": u["mvp_accommodation"]["rating"],
                "location": {
                    "city": u["mvp_accommodation"].get("city"),
                    "state": u["mvp_accommodation"].get("state"),
                    "country": u["mvp_accommodation"].get("country"),
                    "lat": u["mvp_accommodation"].get("lat"),
                    "lng": u["mvp_accommodation"].get("lng"),
                },
                "review_summary": {
                    "rating": parse_rating(u["mvp_accommodation"].get("rating")),
                    "review_count": 0,
                    "text": (
                        f"{parse_rating(u['mvp_accommodation'].get('rating')):.2f}"
                        if parse_rating(u["mvp_accommodation"].get("rating")) is not None
                        else "No reviews yet"
                    ),
                },
                "host": {
                    "name": u["mvp_accommodation"].get("name") or "Host",
                    "years_hosting": 1,
                    "response_time": "Responds within a few days or more",
                    "is_superhost": bool(
                        (parse_rating(u["mvp_accommodation"].get("rating")) or 0) >= 4.8
                    ),
                    "avatar_url": (
                        u["mvp_accommodation"].get("image_url")
                        or (u["images"][0] if u.get("images") else "")
                    ),
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
                    "safety": derive_safety(u.get("amenities", [])),
                },
                "extended_reviews": [],
                "map": {
                    "lat": u["mvp_accommodation"].get("lat"),
                    "lng": u["mvp_accommodation"].get("lng"),
                    "label": ", ".join(
                        [
                            p
                            for p in [
                                u["mvp_accommodation"].get("city"),
                                u["mvp_accommodation"].get("state"),
                                u["mvp_accommodation"].get("country"),
                            ]
                            if p
                        ]
                    ),
                },
            }
            for u in units
        ],
        "count": len(units),
        "check_in": check_in,
        "check_out": check_out,
        "relaxed_country_filter": relaxed_country_filter,
    }

    return {
        "content": [
            {
                "type": "text",
                "text": f"Found {len(units)} room(s). To reserve, ask the user to click Reserve in the room card. Do not call booking tools directly.",
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
        "title": "Open booking form (widget click only)",
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
    """Widget-only tool. Do not call directly from assistant chat.
    Use only after user clicks Reserve in the room card."""

    unit = None
    looks_like_uuid = bool(
        unit_id
        and len(unit_id) == 36
        and unit_id.count("-") == 4
    )

    # Backward-compatible lookup path for older clients that still send unit_id.
    if looks_like_uuid:
        unit_result = (
            supabase.table("mvp_unit")
            .select("*, mvp_accommodation!inner(*)")
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
            supabase.table("mvp_unit")
            .select("*, mvp_accommodation!inner(*)")
            .eq("name", lookup_name)
        )
        if hotel_name:
            unit_query = unit_query.eq("mvp_accommodation.name", hotel_name)

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
        "hotel_name": unit["mvp_accommodation"]["name"],
        "check_in": check_in_date.isoformat(),
        "check_out": check_out_date.isoformat(),
        "nights": nights,
        "guests": guests,
        "price_per_night": float(unit["price_per_night"]),
        "currency_code": unit["currency_code"],
        "total_price": total,
        "image_url": unit["images"][0] if unit.get("images") else "",
        "rating": unit["mvp_accommodation"]["rating"],
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
    """Widget-only tool. Do not call directly from assistant chat.
    Use only after user submits the booking form."""

    # Upsert guest. Supabase may return None or a result object with dict/list data.
    existing = (
        supabase.table("mvp_guest")
        .select("id")
        .eq("email", guest_email)
        .maybe_single()
        .execute()
    )
    existing_data = existing.data if existing is not None else None
    if isinstance(existing_data, list):
        existing_data = existing_data[0] if existing_data else None

    if existing_data:
        guest_id = existing_data["id"]
        supabase.table("mvp_guest").update(
            {"name": guest_name, "phone": guest_phone}
        ).eq("id", guest_id).execute()
    else:
        guest_insert = (
            supabase.table("mvp_guest")
            .insert({"name": guest_name, "email": guest_email, "phone": guest_phone})
            .execute()
        )
        inserted_rows = guest_insert.data if guest_insert is not None else None
        if not inserted_rows:
            raise RuntimeError("Failed to create guest record for booking confirmation.")
        guest_id = inserted_rows[0]["id"]

    # Get unit info
    unit = (
        supabase.table("mvp_unit")
        .select("*, mvp_accommodation!inner(*)")
        .eq("id", unit_id)
        .single()
        .execute()
        .data
    )
    if unit_name and unit_name.strip() != unit["name"]:
        raise ValueError(
            f"Unit name mismatch for unit_id '{unit_id}': expected '{unit['name']}', got '{unit_name.strip()}'."
        )

    # Generate confirmation code
    confirmation_code = f"BK-{uuid.uuid4().hex[:6].upper()}"

    # Create reservation
    check_in_date = check_in
    check_out_date = check_out
    nights = (check_out_date - check_in_date).days

    supabase.table("mvp_reservation").insert(
        {
            "confirmation_code": confirmation_code,
            "guest_id": guest_id,
            "unit_id": unit_id,
            "accommodation_id": unit["property_id"],
            "check_in": check_in_date.isoformat(),
            "check_out": check_out_date.isoformat(),
            "guests_count": guests,
            "total_price": total_price,
            "currency_code": currency_code,
            "status": "confirmed",
        }
    ).execute()

    structured = {
        "confirmation_code": confirmation_code,
        "status": "confirmed",
        "unit_name": unit["name"],
        "hotel_name": unit["mvp_accommodation"]["name"],
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
