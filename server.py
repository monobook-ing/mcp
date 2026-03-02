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

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

load_dotenv()

from fastmcp import FastMCP

from db import fetch_all, fetch_one, execute, execute_returning

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
ROOM_GALLERY_WIDGET_URI = "ui://widget/room-gallery.html"

MONOSEND_EMAILS_URL = "https://api.monosend.io/emails"
MONOSEND_TEMPLATE_ID = "7bc5aec5-cf40-4bec-88c5-d1c00b611fde"
MONOSEND_FROM_EMAIL = "noreply@monosend.email"
MONOSEND_API_KEY = os.getenv("API_KEY", "mono_bYguadb30GKyZ49gv0MxnJdgzG2xpHupQNw3Szbf87o")
MONOSEND_TIMEOUT_SECONDS = 10


def log_tool_call(
    tool_name: str,
    description: str,
    status: str = "success",
    source: str = "chatgpt",
    property_id: str | None = None,
    request_payload: dict | None = None,
    response_payload: dict | None = None,
) -> None:
    """Persist an audit_log row for every tool invocation. Fire-and-forget."""
    try:
        execute(
            """INSERT INTO audit_log
               (property_id, source, tool_name, description, status,
                request_payload, response_payload)
               VALUES (%s, %s::audit_source_type, %s, %s, %s::audit_entry_status, %s::jsonb, %s::jsonb)""",
            [
                property_id,
                source,
                tool_name,
                description,
                status,
                json.dumps(request_payload) if request_payload else None,
                json.dumps(response_payload) if response_payload else None,
            ],
        )
    except Exception as e:
        logger.warning("Failed to log tool call: %s", e)


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


@mcp.resource(
    uri=ROOM_GALLERY_WIDGET_URI,
    name="Room Gallery Widget",
    description="Displays a photo gallery for a hotel room with lightbox and full-screen view",
    mime_type=RESOURCE_MIME,
    meta={
        "openai/widgetDescription": "Interactive photo gallery for a specific room. Shows images in a grid with lightbox navigation.",
        "openai/widgetPrefersBorder": False,
    },
)
def room_gallery_resource() -> str:
    return load_widget("room_gallery")


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

    conditions = []
    params = []

    if hotel_name:
        conditions.append("a.name ILIKE %s")
        params.append(f"%{hotel_name}%")
    if city:
        conditions.append("p.city ILIKE %s")
        params.append(f"%{city}%")
    if country:
        conditions.append("p.country ILIKE %s")
        params.append(f"%{country}%")
    if lat is not None and lng is not None:
        delta = 0.5
        conditions.append("p.lat >= %s AND p.lat <= %s")
        params.extend([lat - delta, lat + delta])
        conditions.append("p.lng >= %s AND p.lng <= %s")
        params.extend([lng - delta, lng + delta])

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = (
        "SELECT p.*, a.name AS account_name "
        "FROM properties p JOIN accounts a ON p.account_id = a.id "
        f"{where}"
    )
    hotels = fetch_all(sql, params or None)

    first_hotel = hotels[0] if hotels else {}
    log_tool_call(
        "search_hotels",
        f"Searched hotels: name={hotel_name!r} city={city!r} country={country!r}",
        property_id=str(first_hotel["id"]) if first_hotel else None,
        request_payload={
            "hotel_name": hotel_name,
            "city": city,
            "country": country,
            "lat": lat,
            "lng": lng,
        },
        response_payload={"count": len(hotels)},
    )

    return {
        "hotels": hotels,
        "count": len(hotels),
    }


# ── Tool 2: search_rooms ──────────────────────────────────────────────────

def _get_occupied_room_ids(room_ids: list[str], check_in_str: str, check_out_str: str) -> set[str]:
    if not room_ids:
        return set()
    placeholders = ", ".join(["%s"] * len(room_ids))
    sql = f"""
        SELECT DISTINCT room_id FROM bookings
        WHERE room_id IN ({placeholders})
          AND status != 'cancelled'
          AND check_in < %s AND check_out > %s
    """
    rows = fetch_all(sql, room_ids + [check_out_str, check_in_str])
    return {str(r["room_id"]) for r in rows}


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
    show_occupied: bool = False,
) -> dict:
    """Search available rooms/units by hotel name, city, country, type, price,
    guest capacity, amenities like 'Hot tub', 'Sauna', 'Pool', or free-text query.
    Query matches unit name/description/type, hotel name, city/state/country, and amenities.
    Returns interactive room cards with Reserve button.
    Use check_in/check_out in YYYY-MM-DD format if dates are known.
    By default only available rooms returned. Set show_occupied=True to include occupied rooms."""

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

        if normalized_query:
            query_terms = [t for t in normalized_query.split() if len(t) > 2]
            if not query_terms:
                query_terms = normalized_query.split()
            if not any(term in searchable_blob for term in query_terms):
                return False

        if normalized_amenity and normalized_amenity not in amenities_blob:
            return False
        return True

    def unit_matches_amenity_filter(unit: dict) -> bool:
        if not normalized_amenity:
            return True
        amenities_list = normalize_amenities(unit.get("amenities"))
        amenities_blob = normalize_text(" ".join(amenities_list))
        return normalized_amenity in amenities_blob

    def _row_to_unit(row):
        """Convert a flat SQL row into the nested dict structure expected downstream."""
        d = dict(row)
        d["properties"] = {
            "city": d.pop("p_city", None),
            "state": d.pop("p_state", None),
            "country": d.pop("p_country", None),
            "rating": d.pop("p_rating", None),
            "image_url": d.pop("p_image_url", None),
            "lat": d.pop("p_lat", None),
            "lng": d.pop("p_lng", None),
            "name": d.pop("p_name", None),
        }
        return d

    def run_room_query(include_country: bool):
        conditions = []
        params = []

        if hotel_name:
            conditions.append("(p.description ILIKE %s OR p.city ILIKE %s)")
            params.extend([f"%{hotel_name}%", f"%{hotel_name}%"])
        if city:
            conditions.append("p.city ILIKE %s")
            params.append(f"%{city}%")
        if include_country and country:
            conditions.append("p.country ILIKE %s")
            params.append(f"%{country}%")
        if unit_type:
            conditions.append("r.type ILIKE %s")
            params.append(f"%{unit_type}%")
        if max_price is not None:
            conditions.append("r.price_per_night <= %s")
            params.append(max_price)
        if min_guests is not None:
            conditions.append("r.max_guests >= %s")
            params.append(min_guests)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"""
            SELECT r.*,
                   p.city AS p_city, p.state AS p_state, p.country AS p_country,
                   p.rating AS p_rating, p.image_url AS p_image_url,
                   p.lat AS p_lat, p.lng AS p_lng, p.description AS p_name
            FROM rooms r
            JOIN properties p ON r.property_id = p.id
            {where}
        """
        rows = fetch_all(sql, params or None)
        return [_row_to_unit(r) for r in rows]

    sql_results = run_room_query(include_country=True)
    units = [u for u in sql_results if unit_matches_text_filters(u)]
    if not units and sql_results and normalized_query:
        units = [u for u in sql_results if unit_matches_amenity_filter(u)]
    relaxed_country_filter = False

    # If city+country yields nothing, retry city-only for region ambiguities
    if not units and city and country:
        relaxed_sql = run_room_query(include_country=False)
        relaxed_units = [u for u in relaxed_sql if unit_matches_text_filters(u)]
        if not relaxed_units and relaxed_sql and normalized_query:
            relaxed_units = [u for u in relaxed_sql if unit_matches_amenity_filter(u)]
        if relaxed_units:
            units = relaxed_units
            relaxed_country_filter = True

    if not show_occupied and units:
        today = date.today().isoformat()
        eff_in = check_in if check_in else today
        eff_out = check_out if check_out else (date.today() + timedelta(days=1)).isoformat()
        occupied = _get_occupied_room_ids([str(u["id"]) for u in units], eff_in, eff_out)
        units = [u for u in units if str(u["id"]) not in occupied]

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

    first_unit = units[0] if units else {}
    log_tool_call(
        "search_rooms",
        f"Searched rooms: query={query!r} city={city!r}",
        property_id=str(first_unit.get("property_id")) if first_unit.get("property_id") else None,
        request_payload={
            "hotel_name": hotel_name,
            "city": city,
            "country": country,
            "query": query,
            "check_in": check_in,
            "check_out": check_out,
            "show_occupied": show_occupied,
        },
        response_payload={"count": len(units)},
    )

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

    def _row_to_unit(row):
        d = dict(row)
        d["properties"] = {
            "city": d.pop("p_city", None),
            "state": d.pop("p_state", None),
            "country": d.pop("p_country", None),
            "rating": d.pop("p_rating", None),
            "image_url": d.pop("p_image_url", None),
            "lat": d.pop("p_lat", None),
            "lng": d.pop("p_lng", None),
            "name": d.pop("p_name", None),
        }
        return d

    unit = None
    looks_like_uuid = bool(
        unit_id
        and len(unit_id) == 36
        and unit_id.count("-") == 4
    )

    base_sql = """
        SELECT r.*,
               p.city AS p_city, p.state AS p_state, p.country AS p_country,
               p.rating AS p_rating, p.image_url AS p_image_url,
               p.lat AS p_lat, p.lng AS p_lng, p.description AS p_name
        FROM rooms r
        JOIN properties p ON r.property_id = p.id
    """

    # Backward-compatible lookup path for older clients that still send unit_id.
    if looks_like_uuid:
        sql = base_sql + " WHERE r.id = %s"
        row = fetch_one(sql, [unit_id])
        if row:
            unit = _row_to_unit(row)
    else:
        lookup_name = (unit_name or unit_id).strip()
        if not lookup_name:
            raise ValueError("Either unit_name or unit_id is required.")

        params = [lookup_name]
        sql = base_sql + " WHERE r.name = %s"
        
        if hotel_name:
            sql += " AND p.city = %s"
            params.append(hotel_name)

        rows = fetch_all(sql, params)
        if not rows:
            raise ValueError(
                f"Unable to find unit '{lookup_name}'"
                + (f" at hotel '{hotel_name}'." if hotel_name else ".")
            )
        unit = _row_to_unit(rows[0])

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

    log_tool_call(
        "book",
        f"Opened booking form: {unit['name']}",
        property_id=str(unit["property_id"]),
        request_payload={
            "unit_id": unit_id,
            "unit_name": unit_name,
            "hotel_name": hotel_name,
            "check_in": check_in.isoformat(),
            "check_out": check_out.isoformat(),
            "guests": guests,
        },
        response_payload={"unit_id": str(unit["id"]), "nights": nights, "total": total},
    )

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

    def _row_to_unit(row):
        d = dict(row)
        d["properties"] = {
            "city": d.pop("p_city", None),
            "state": d.pop("p_state", None),
            "country": d.pop("p_country", None),
            "rating": d.pop("p_rating", None),
            "image_url": d.pop("p_image_url", None),
            "lat": d.pop("p_lat", None),
            "lng": d.pop("p_lng", None),
            "name": d.pop("p_name", None),
        }
        return d

    # Get unit info (moved up so property_id is available for guest upsert)
    sql = """
        SELECT r.*,
               p.city AS p_city, p.state AS p_state, p.country AS p_country,
               p.rating AS p_rating, p.image_url AS p_image_url,
               p.lat AS p_lat, p.lng AS p_lng, p.description AS p_name
        FROM rooms r
        JOIN properties p ON r.property_id = p.id
        WHERE r.id = %s
    """
    row = fetch_one(sql, [unit_id])
    if not row:
        raise ValueError(f"Unable to find unit with id '{unit_id}'")
    
    unit = _row_to_unit(row)
    
    if unit_name and unit_name.strip() != unit["name"]:
        raise ValueError(
            f"Unit name mismatch for unit_id '{unit_id}': expected '{unit['name']}', got '{unit_name.strip()}'."
        )

    property_id = unit["property_id"]

    # Upsert guest
    existing_data = fetch_one(
        "SELECT id FROM guests WHERE email = %s AND property_id = %s LIMIT 1",
        [guest_email, property_id]
    )

    if existing_data:
        guest_id = existing_data["id"]
        execute(
            "UPDATE guests SET name = %s, phone = %s, updated_at = now() WHERE id = %s",
            [guest_name, guest_phone, guest_id]
        )
    else:
        inserted_row = execute_returning(
            "INSERT INTO guests (property_id, name, email, phone) VALUES (%s, %s, %s, %s) RETURNING id",
            [property_id, guest_name, guest_email, guest_phone]
        )
        if not inserted_row:
            raise RuntimeError("Failed to create guest record for booking confirmation.")
        guest_id = inserted_row["id"]

    # Generate confirmation code
    confirmation_code = f"BK-{uuid.uuid4().hex[:6].upper()}"

    # Create reservation
    check_in_date = check_in
    check_out_date = check_out
    nights = (check_out_date - check_in_date).days

    execute(
        """
        INSERT INTO bookings 
        (property_id, room_id, guest_id, check_in, check_out, guests_count, 
         total_price, currency_code, status, ai_handled, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            property_id, unit_id, guest_id, check_in_date.isoformat(), 
            check_out_date.isoformat(), guests, total_price, currency_code, 
            "confirmed", True, "chatgpt"
        ]
    )

    log_tool_call(
        "book_confirm",
        f"Booking confirmed: {confirmation_code} for {guest_name}",
        property_id=str(property_id),
        request_payload={
            "unit_id": unit_id,
            "check_in": check_in.isoformat(),
            "check_out": check_out.isoformat(),
            "guests": guests,
            "guest_email": guest_email,
        },
        response_payload={"confirmation_code": confirmation_code, "total_price": total_price},
    )

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


# ── Tool 5: room_gallery ─────────────────────────────────────────────────

@mcp.tool(
    annotations={"readOnlyHint": True, "openWorldHint": False},
    app={"resourceUri": ROOM_GALLERY_WIDGET_URI},
    meta={
        "openai/outputTemplate": ROOM_GALLERY_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def room_gallery(
    room_id: str = "",
    room_name: str = "",
) -> dict:
    """Show the photo gallery for a room/unit.
    Returns interactive gallery widget with all room images."""

    def _row_to_unit(row):
        d = dict(row)
        d["properties"] = {
            "city": d.pop("p_city", None),
            "state": d.pop("p_state", None),
            "country": d.pop("p_country", None),
        }
        return d

    unit = None
    looks_like_uuid = bool(
        room_id
        and len(room_id) == 36
        and room_id.count("-") == 4
    )

    base_sql = """
        SELECT r.*,
               p.city AS p_city, p.state AS p_state, p.country AS p_country
        FROM rooms r
        JOIN properties p ON r.property_id = p.id
    """

    if looks_like_uuid:
        row = fetch_one(base_sql + " WHERE r.id = %s", [room_id])
        if row:
            unit = _row_to_unit(row)
    else:
        lookup_name = (room_name or room_id).strip()
        if not lookup_name:
            raise ValueError("Either room_name or room_id is required.")

        rows = fetch_all(base_sql + " WHERE r.name = %s", [lookup_name])
        
        if not rows:
            raise ValueError(f"Unable to find room '{lookup_name}'.")
        unit = _row_to_unit(rows[0])

    images = unit.get("images")
    if not isinstance(images, list):
        images = [str(images)] if images else []

    accommodation = unit.get("properties") or {}

    structured = {
        "room_id": unit["id"],
        "room_name": unit.get("name", ""),
        "room_type": unit.get("type", ""),
        "images": images,
        "image_count": len(images),
        "location": {
            "city": accommodation.get("city"),
            "state": accommodation.get("state"),
            "country": accommodation.get("country"),
        },
    }

    log_tool_call(
        "room_gallery",
        f"Viewed gallery: {unit.get('name', '')}",
        property_id=str(unit["property_id"]),
        request_payload={"room_id": room_id, "room_name": room_name},
        response_payload={"image_count": len(images)},
    )

    return {
        "content": [
            {
                "type": "text",
                "text": f"Showing {len(images)} photo(s) for {unit.get('name', 'room')}.",
            }
        ],
        "structuredContent": structured,
        "_meta": {
            "ui": {"resourceUri": ROOM_GALLERY_WIDGET_URI},
            "openai/outputTemplate": ROOM_GALLERY_WIDGET_URI,
            "openai/widgetAccessible": True,
        },
    }


# ── Ping endpoint ──────────────────────────────────────────────────────────

from starlette.requests import Request
from starlette.responses import JSONResponse, Response as StarletteResponse
from starlette.routing import Route


async def ping_get(request: Request):
    """Health-check endpoint (GET)."""
    return JSONResponse({"status": "ok"})


async def ping_head(request: Request):
    """Health-check endpoint (HEAD)."""
    return StarletteResponse(status_code=200)


async def ping_options(request: Request):
    """Health-check endpoint (OPTIONS) – returns allowed methods."""
    return StarletteResponse(
        status_code=200,
        headers={"Allow": "GET, HEAD, OPTIONS"},
    )


app.routes.insert(0, Route("/ping", endpoint=ping_get, methods=["GET", "HEAD", "OPTIONS"]))


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
