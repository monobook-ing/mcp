import os
import re
import uuid
import json
import logging
import math
from time import perf_counter
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional
from urllib import request, error
from urllib.parse import quote

from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

load_dotenv()

from fastmcp import FastMCP

from db import fetch_all, fetch_one, execute, execute_returning

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - environment-dependent
    OpenAI = None  # type: ignore[assignment]

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
KNOWLEDGE_ANSWER_WIDGET_URI = "ui://widget/knowledge-answer.html"

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


@lru_cache(maxsize=1)
def _get_openai_client() -> Any:
    if OpenAI is None:
        raise RuntimeError("OpenAI client unavailable. Install openai>=1.60.0")

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured")
    return OpenAI(api_key=api_key)


def _embed_query(text: str) -> list[float]:
    client = _get_openai_client()
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=text,
    )
    return list(response.data[0].embedding)


def _search_chunks(
    property_id: str,
    embedding: list[float],
    language: str | None = None,
    limit: int = 8,
) -> list[dict]:
    query_vector = "[" + ",".join(f"{value:.8f}" for value in embedding) + "]"
    sql = """
        SELECT
          e.id,
          e.source_id AS file_id,
          e.chunk_index,
          e.content,
          e.metadata,
          (1 - (e.embedding <=> %s::vector))::float AS similarity
        FROM embeddings e
        WHERE e.property_id = %s::uuid
          AND e.source_type = 'knowledge_chunk'
          AND (%s::text IS NULL OR COALESCE(e.metadata->>'language', '') = %s::text)
          AND (1 - (e.embedding <=> %s::vector)) > 0.5
        ORDER BY e.embedding <=> %s::vector
        LIMIT %s
    """
    rows = fetch_all(
        sql,
        [
            query_vector,
            property_id,
            language,
            language,
            query_vector,
            query_vector,
            limit,
        ],
    )
    return rows


def _build_rag_answer(question: str, chunks: list[dict]) -> str:
    client = _get_openai_client()
    context_lines = []
    for index, chunk in enumerate(chunks, start=1):
        metadata = chunk.get("metadata") or {}
        context_lines.append(
            "\n".join(
                [
                    f"[Source {index}]",
                    f"file_name: {metadata.get('file_name', 'Unknown')}",
                    f"doc_type: {metadata.get('doc_type', 'general')}",
                    f"section: {metadata.get('section', 'General')}",
                    f"content: {str(chunk.get('content') or '')[:1400]}",
                ]
            )
        )

    system_prompt = (
        "You are a hotel concierge assistant. "
        "Answer only with information from the provided sources. "
        "If the answer is not in the sources, say that you do not have enough information."
    )
    user_prompt = (
        "Use the context below to answer the user question.\n\n"
        f"{'\n'.join(context_lines)}\n\n"
        f"Question: {question}"
    )

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0.3,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    return (response.choices[0].message.content or "").strip()


def _log_rag_query(
    property_id: str,
    question: str,
    answer: str,
    chunks_used: list[dict],
    language: str | None,
    latency_ms: int,
) -> None:
    try:
        execute(
            """INSERT INTO rag_query_logs
               (property_id, question, answer, chunks_used, source, language, latency_ms)
               VALUES (%s::uuid, %s, %s, %s::jsonb, %s, %s, %s)""",
            [
                property_id,
                question,
                answer,
                json.dumps(chunks_used),
                "chatgpt",
                language or "en",
                latency_ms,
            ],
        )
    except Exception as exc:
        logger.warning("Failed to persist rag_query_logs: %s", exc)


def _resolve_property_id(property_id: str, room_id: str) -> str:
    normalized_room_id = str(room_id or "").strip()
    if normalized_room_id:
        room = fetch_one(
            "SELECT property_id FROM rooms WHERE id = %s LIMIT 1",
            [normalized_room_id],
        )
        if not room or not room.get("property_id"):
            raise ValueError("room_id not found")
        return str(room["property_id"])

    normalized_property_id = str(property_id or "").strip()
    if normalized_property_id:
        return normalized_property_id

    raise ValueError("Either property_id or room_id is required.")


GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
EXPERIENCES_CARD_WIDGET_URI = "ui://widget/experiences-card.html"

_GOOGLE_SEARCH_TEXT_URL = "https://places.googleapis.com/v1/places:searchText"
_PRICE_ENUM_BY_LEVEL = {
    0: "PRICE_LEVEL_FREE",
    1: "PRICE_LEVEL_INEXPENSIVE",
    2: "PRICE_LEVEL_MODERATE",
    3: "PRICE_LEVEL_EXPENSIVE",
    4: "PRICE_LEVEL_VERY_EXPENSIVE",
}
_PRICE_LEVEL_BY_ENUM = {v: k for k, v in _PRICE_ENUM_BY_LEVEL.items()}
_IGNORED_TYPES = {"point_of_interest", "establishment", "food", "restaurant"}
_FIELD_MASK_SEARCH = ",".join(
    [
        "places.id",
        "places.name",
        "places.displayName",
        "places.formattedAddress",
        "places.location",
        "places.rating",
        "places.userRatingCount",
        "places.priceLevel",
        "places.types",
        "places.internationalPhoneNumber",
        "places.nationalPhoneNumber",
        "places.websiteUri",
        "places.photos",
        "places.regularOpeningHours",
        "places.currentOpeningHours",
        "places.googleMapsUri",
    ]
)


def _to_float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _curated_maps_url(
    google_place_id: str | None,
    lat: float | None,
    lng: float | None,
) -> str | None:
    if google_place_id:
        return (
            "https://www.google.com/maps/search/?api=1"
            f"&query_place_id={google_place_id}"
        )
    if lat is not None and lng is not None:
        return f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"
    return None


def _normalize_curated_place(row: dict[str, Any]) -> dict[str, Any]:
    lat = _to_float_or_none(row.get("lat")) if row.get("lat") is not None else None
    lng = _to_float_or_none(row.get("lng")) if row.get("lng") is not None else None
    photo_urls = _to_text_list(row.get("photo_urls"))
    return {
        "place_id": str(row.get("id", "")),
        "source": "curated",
        "name": row.get("name", ""),
        "address": row.get("address"),
        "lat": lat,
        "lng": lng,
        "rating": _to_float_or_none(row.get("rating"))
        if row.get("rating") is not None
        else None,
        "review_count": _to_int_or_none(row.get("review_count")),
        "price_level": _to_int_or_none(row.get("price_level")),
        "cuisine": _to_text_list(row.get("cuisine")),
        "phone": row.get("phone"),
        "website": row.get("website"),
        "photo_url": photo_urls[0] if photo_urls else None,
        "opening_hours": row.get("opening_hours"),
        "is_open_now": None,
        "walking_minutes": _to_int_or_none(row.get("walking_minutes")),
        "distance_m": None,
        "best_for": _to_text_list(row.get("best_for")),
        "meal_types": _to_text_list(row.get("meal_types")),
        "is_curated": True,
        "is_sponsored": bool(row.get("sponsored", False)),
        "maps_url": _curated_maps_url(row.get("google_place_id"), lat, lng),
    }


def _search_google_places(
    lat: float,
    lng: float,
    query: str,
    cuisine: str,
    price_level: int,
    open_now: bool,
    limit: int,
    radius_m: int = 5000,
) -> list[dict[str, Any]]:
    if not GOOGLE_PLACES_API_KEY:
        return []

    safe_limit = max(1, min(int(limit or 8), 20))
    radius = max(100, min(int(radius_m or 5000), 50000))

    text_terms = [query.strip() or "restaurant"]
    if cuisine.strip():
        text_terms.append(cuisine.strip())
    payload: dict[str, Any] = {
        "textQuery": " ".join(text_terms),
        "includedType": "restaurant",
        "maxResultCount": safe_limit,
        "locationBias": {
            "circle": {
                "center": {"latitude": lat, "longitude": lng},
                "radius": float(radius),
            }
        },
        "rankPreference": "DISTANCE",
        "openNow": bool(open_now),
    }
    if price_level > 0 and price_level in _PRICE_ENUM_BY_LEVEL:
        payload["priceLevels"] = [_PRICE_ENUM_BY_LEVEL[price_level]]

    req = request.Request(
        _GOOGLE_SEARCH_TEXT_URL,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
            "X-Goog-FieldMask": _FIELD_MASK_SEARCH,
        },
    )
    try:
        with request.urlopen(req, timeout=12) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body)
    except (error.HTTPError, error.URLError, TimeoutError, ValueError) as exc:
        logger.warning("Google Places search failed: %s", exc)
        return []

    places = data.get("places")
    if not isinstance(places, list):
        return []
    return [place for place in places if isinstance(place, dict)]


def _extract_place_id(raw: dict[str, Any]) -> str:
    if isinstance(raw.get("id"), str):
        return raw["id"]
    name = raw.get("name")
    if isinstance(name, str) and "/" in name:
        return name.split("/")[-1]
    return ""


def _display_place_name(raw: dict[str, Any]) -> str:
    display_name = raw.get("displayName")
    if isinstance(display_name, dict):
        text = display_name.get("text")
        if isinstance(text, str):
            return text
    if isinstance(display_name, str):
        return display_name
    return "Unknown place"


def _normalize_price_level(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value if 0 <= value <= 4 else None
    if isinstance(value, str):
        return _PRICE_LEVEL_BY_ENUM.get(value)
    return None


def _extract_cuisine(types: Any) -> list[str]:
    if not isinstance(types, list):
        return []
    labels: list[str] = []
    for place_type in types:
        if not isinstance(place_type, str) or place_type in _IGNORED_TYPES:
            continue
        label = place_type.replace("_", " ").strip().title()
        if label and label not in labels:
            labels.append(label)
        if len(labels) >= 4:
            break
    return labels


def _normalize_opening_hours(raw: dict[str, Any]) -> dict[str, Any] | None:
    source = raw.get("regularOpeningHours")
    if not isinstance(source, dict):
        current = raw.get("currentOpeningHours")
        source = current if isinstance(current, dict) else None
    if not isinstance(source, dict):
        return None

    weekday = source.get("weekdayDescriptions")
    if not isinstance(weekday, list):
        return None

    normalized: dict[str, Any] = {}
    for row in weekday:
        if not isinstance(row, str) or ":" not in row:
            continue
        day, value = row.split(":", 1)
        normalized[day.strip().lower()] = value.strip()
    return normalized or None


def _extract_open_now(raw: dict[str, Any]) -> bool | None:
    current = raw.get("currentOpeningHours")
    if isinstance(current, dict) and isinstance(current.get("openNow"), bool):
        return current.get("openNow")
    regular = raw.get("regularOpeningHours")
    if isinstance(regular, dict) and isinstance(regular.get("openNow"), bool):
        return regular.get("openNow")
    return None


def _photo_url(photos: Any) -> str | None:
    if not isinstance(photos, list) or not photos:
        return None
    first = photos[0] if isinstance(photos[0], dict) else None
    if not isinstance(first, dict):
        return None
    photo_name = first.get("name")
    if not isinstance(photo_name, str) or not photo_name:
        return None
    return (
        f"https://places.googleapis.com/v1/{photo_name}/media"
        f"?maxHeightPx=600&key={quote(GOOGLE_PLACES_API_KEY, safe='')}"
    )


def _normalize_google_place(raw: dict[str, Any]) -> dict[str, Any]:
    place_id = _extract_place_id(raw)
    location = raw.get("location") if isinstance(raw.get("location"), dict) else {}
    lat = _to_float_or_none(location.get("latitude"))
    lng = _to_float_or_none(location.get("longitude"))
    price_level = _normalize_price_level(raw.get("priceLevel"))

    maps_url = raw.get("googleMapsUri")
    if not maps_url:
        if place_id:
            maps_url = (
                "https://www.google.com/maps/search/?api=1"
                f"&query_place_id={place_id}"
            )
        elif lat is not None and lng is not None:
            maps_url = f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"

    return {
        "place_id": place_id,
        "source": "google",
        "name": _display_place_name(raw),
        "address": raw.get("formattedAddress"),
        "lat": lat,
        "lng": lng,
        "rating": _to_float_or_none(raw.get("rating")),
        "review_count": _to_int_or_none(raw.get("userRatingCount")),
        "price_level": price_level,
        "cuisine": _extract_cuisine(raw.get("types")),
        "phone": raw.get("internationalPhoneNumber") or raw.get("nationalPhoneNumber"),
        "website": raw.get("websiteUri"),
        "photo_url": _photo_url(raw.get("photos")),
        "opening_hours": _normalize_opening_hours(raw),
        "is_open_now": _extract_open_now(raw),
        "walking_minutes": None,
        "distance_m": None,
        "best_for": [],
        "meal_types": [],
        "is_curated": False,
        "is_sponsored": False,
        "maps_url": maps_url,
    }


def _haversine_distance_km(
    origin_lat: float,
    origin_lng: float,
    target_lat: float,
    target_lng: float,
) -> float:
    earth_radius_km = 6371.0
    d_lat = math.radians(target_lat - origin_lat)
    d_lng = math.radians(target_lng - origin_lng)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(origin_lat))
        * math.cos(math.radians(target_lat))
        * math.sin(d_lng / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return earth_radius_km * c


def _with_walking_distance(
    place: dict[str, Any],
    property_lat: float | None,
    property_lng: float | None,
) -> dict[str, Any]:
    if property_lat is None or property_lng is None:
        return place
    place_lat = _to_float_or_none(place.get("lat"))
    place_lng = _to_float_or_none(place.get("lng"))
    if place_lat is None or place_lng is None:
        return place

    distance_km = _haversine_distance_km(property_lat, property_lng, place_lat, place_lng)
    distance_m = int(round(distance_km * 1000))
    updated = dict(place)
    updated["distance_m"] = distance_m
    if not updated.get("walking_minutes"):
        updated["walking_minutes"] = max(1, int(round(distance_m / 80)))
    return updated


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


@mcp.resource(
    uri=KNOWLEDGE_ANSWER_WIDGET_URI,
    name="Knowledge Answer Widget",
    description="Displays grounded RAG answer with source citations and expandable chunks",
    mime_type=RESOURCE_MIME,
    meta={
        "openai/widgetDescription": "Grounded answer card with source citations from property knowledge documents.",
        "openai/widgetPrefersBorder": False,
    },
)
def knowledge_answer_resource() -> str:
    return load_widget("knowledge_answer")


@mcp.resource(
    uri=EXPERIENCES_CARD_WIDGET_URI,
    name="Experiences Card Widget",
    description="Displays nearby places/restaurants as cards with photos, ratings, and directions",
    mime_type=RESOURCE_MIME,
    meta={
        "openai/widgetDescription": "Interactive experience/restaurant cards with directions and booking links.",
        "openai/widgetPrefersBorder": False,
    },
)
def experiences_card_resource() -> str:
    return load_widget("experiences_card")


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


# ── Tool 3: search_knowledge ───────────────────────────────────────────────

@mcp.tool(
    annotations={"readOnlyHint": True, "openWorldHint": False},
    app={"resourceUri": KNOWLEDGE_ANSWER_WIDGET_URI},
    meta={
        "openai/outputTemplate": KNOWLEDGE_ANSWER_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def search_knowledge(
    question: str,
    property_id: str = "",
    room_id: str = "",
    language: str = "",
) -> dict:
    """Answer guest questions from indexed property knowledge files (RAG).
    Requires property_id or room_id. If room_id is provided, property_id is resolved from room."""
    started_at = perf_counter()
    normalized_question = str(question or "").strip()
    normalized_property_input = str(property_id or "").strip()
    normalized_room_id = str(room_id or "").strip()
    if not normalized_property_input and not normalized_room_id:
        raise ValueError("Either property_id or room_id is required for search_knowledge.")
    normalized_property_id = _resolve_property_id(normalized_property_input, normalized_room_id)
    normalized_language = str(language or "").strip().lower() or None

    if not normalized_question:
        raise ValueError("question is required.")

    try:
        query_embedding = _embed_query(normalized_question)
        chunks = _search_chunks(
            property_id=normalized_property_id,
            embedding=query_embedding,
            language=normalized_language,
            limit=8,
        )

        if not chunks:
            answer = (
                "I couldn't find that in the uploaded knowledge documents. "
                "Please ask the property owner to upload or update the relevant policy."
            )
            latency_ms = int((perf_counter() - started_at) * 1000)
            _log_rag_query(
                property_id=normalized_property_id,
                question=normalized_question,
                answer=answer,
                chunks_used=[],
                language=normalized_language,
                latency_ms=latency_ms,
            )
            log_tool_call(
                "search_knowledge",
                "No matching knowledge chunks found.",
                property_id=normalized_property_id,
                request_payload={
                    "question": normalized_question,
                    "room_id": normalized_room_id or None,
                    "language": normalized_language,
                    "property_id_input": normalized_property_input or None,
                    "property_id_resolved": normalized_property_id,
                    "chunks_used_count": 0,
                    "chunks_used": [],
                },
                response_payload={"answer": answer, "chunks_used": []},
            )
            return {
                "content": [{"type": "text", "text": answer}],
                "structuredContent": {
                    "question": normalized_question,
                    "answer": answer,
                    "sources": [],
                },
                "_meta": {
                    "ui": {"resourceUri": KNOWLEDGE_ANSWER_WIDGET_URI},
                    "openai/outputTemplate": KNOWLEDGE_ANSWER_WIDGET_URI,
                    "openai/widgetAccessible": True,
                },
            }

        answer = _build_rag_answer(normalized_question, chunks)
        sources = []
        for chunk in chunks:
            metadata = chunk.get("metadata") or {}
            sources.append(
                {
                    "id": str(chunk.get("id")),
                    "file_id": str(chunk.get("file_id") or ""),
                    "file_name": metadata.get("file_name") or "Unknown",
                    "doc_type": metadata.get("doc_type") or "general",
                    "section": metadata.get("section") or "General",
                    "language": metadata.get("language") or "en",
                    "chunk_index": chunk.get("chunk_index"),
                    "similarity": chunk.get("similarity"),
                    "chunk_text": chunk.get("content") or "",
                }
            )

        latency_ms = int((perf_counter() - started_at) * 1000)
        _log_rag_query(
            property_id=normalized_property_id,
            question=normalized_question,
            answer=answer,
            chunks_used=sources,
            language=normalized_language,
            latency_ms=latency_ms,
        )

        log_tool_call(
            "search_knowledge",
            f"Answered knowledge query using {len(sources)} chunk(s).",
            property_id=normalized_property_id,
            request_payload={
                "question": normalized_question,
                "room_id": normalized_room_id or None,
                "language": normalized_language,
                "property_id_input": normalized_property_input or None,
                "property_id_resolved": normalized_property_id,
                "chunks_used_count": len(sources),
                "chunks_used": [source["id"] for source in sources],
            },
            response_payload={"answer": answer, "chunks_used": sources},
        )

        return {
            "content": [{"type": "text", "text": answer}],
            "structuredContent": {
                "question": normalized_question,
                "answer": answer,
                "sources": sources,
            },
            "_meta": {
                "ui": {"resourceUri": KNOWLEDGE_ANSWER_WIDGET_URI},
                "openai/outputTemplate": KNOWLEDGE_ANSWER_WIDGET_URI,
                "openai/widgetAccessible": True,
            },
        }
    except Exception as exc:
        latency_ms = int((perf_counter() - started_at) * 1000)
        error_message = str(exc)
        _log_rag_query(
            property_id=normalized_property_id,
            question=normalized_question,
            answer=f"ERROR: {error_message}",
            chunks_used=[],
            language=normalized_language,
            latency_ms=latency_ms,
        )
        log_tool_call(
            "search_knowledge",
            f"Knowledge query failed: {error_message}",
            status="error",
            property_id=normalized_property_id,
            request_payload={
                "question": normalized_question,
                "room_id": normalized_room_id or None,
                "language": normalized_language,
                "property_id_input": normalized_property_input or None,
                "property_id_resolved": normalized_property_id,
            },
            response_payload={"error": error_message},
        )
        raise


# ── Tool 4: book ──────────────────────────────────────────────────────────

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


# ── Tool 5: book_confirm ─────────────────────────────────────────────────

def _sanitize(value: str) -> str:
    """Strip common prompt-injection patterns from user-provided text fields."""
    text = str(value or "")
    patterns = [
        r"(?is)```.*?```",
        r"(?is)<\s*(script|style)[^>]*>.*?<\s*/\s*\1\s*>",
        r"(?i)\bignore\s+(all\s+)?(previous|prior)\s+instructions?\b",
        r"(?i)\b(system|developer|assistant)\s*:\s*",
        r"(?i)\b(prompt\s+injection|jailbreak)\b",
    ]
    for pattern in patterns:
        text = re.sub(pattern, " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

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

    # a) Date validation
    today = date.today()
    if check_in < today:
        raise ValueError("Check-in date cannot be in the past.")
    if check_out <= check_in:
        raise ValueError("Check-out date must be after check-in date.")
    nights = (check_out - check_in).days
    if nights > 30:
        raise ValueError("Maximum stay is 30 nights.")
    if check_in > (today + timedelta(days=365)):
        raise ValueError("Check-in date cannot be more than 1 year in advance.")

    # b) Guest count validation
    if guests < 1 or guests > 20:
        raise ValueError("Guests must be between 1 and 20.")
    unit_capacity = int(unit.get("max_guests") or 0)
    if unit_capacity > 0 and guests > unit_capacity:
        raise ValueError(f"Guest count exceeds room capacity ({unit_capacity}).")

    # c) Room availability validation
    occupied = _get_occupied_room_ids([str(unit["id"])], check_in.isoformat(), check_out.isoformat())
    if str(unit["id"]) in occupied:
        raise ValueError("Room is not available for the selected dates.")

    # d) Price validation
    expected_total = float(unit["price_per_night"]) * nights
    if abs(float(total_price) - expected_total) > 0.01:
        raise ValueError(
            f"Total price mismatch: expected {expected_total:.2f}, got {float(total_price):.2f}."
        )

    # e) Input sanitization
    guest_name = _sanitize(guest_name)
    guest_email = _sanitize(guest_email)
    guest_phone = _sanitize(guest_phone)

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


# ── Tool 6: room_gallery ─────────────────────────────────────────────────

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


# ── Tool 7: search_nearby_places ───────────────────────────────────────────

@mcp.tool(
    annotations={"readOnlyHint": True, "openWorldHint": True},
    app={"resourceUri": EXPERIENCES_CARD_WIDGET_URI},
    meta={
        "openai/outputTemplate": EXPERIENCES_CARD_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def search_nearby_places(
    property_id: str = "",
    room_id: str = "",
    query: str = "restaurant",
    cuisine: str = "",
    price_level: int = 0,
    open_now: bool = False,
    limit: int = 8,
) -> dict:
    """Search nearby places for a property.
    Returns curated recommendations plus additional nearby Google Places results."""

    normalized_query = str(query or "").strip() or "restaurant"
    normalized_cuisine = str(cuisine or "").strip()
    normalized_property_input = str(property_id or "").strip()
    normalized_room_id = str(room_id or "").strip()
    safe_limit = max(1, min(int(limit or 8), 20))
    if not normalized_property_input and not normalized_room_id:
        raise ValueError("Either property_id or room_id is required for search_nearby_places.")
    resolved_property_id = _resolve_property_id(normalized_property_input, normalized_room_id)
    property_row = fetch_one(
        "SELECT id, lat, lng FROM properties WHERE id = %s LIMIT 1",
        [resolved_property_id],
    )
    if not property_row:
        raise ValueError("property_id not found")

    property_lat = _to_float_or_none(property_row.get("lat"))
    property_lng = _to_float_or_none(property_row.get("lng"))

    curated_rows = fetch_all(
        """
        SELECT *
        FROM curated_places
        WHERE property_id = %s
          AND deleted_at IS NULL
        ORDER BY sponsored DESC, sort_order ASC, created_at DESC
        LIMIT %s
        """,
        [resolved_property_id, max(safe_limit, 12)],
    )

    filtered_curated: list[dict[str, Any]] = []
    requested_cuisine = normalized_cuisine.lower()
    for row in curated_rows:
        row_price = _to_int_or_none(row.get("price_level"))
        if price_level and row_price not in (None, price_level):
            continue
        if requested_cuisine:
            cuisines = [str(item).lower() for item in _to_text_list(row.get("cuisine"))]
            if requested_cuisine not in cuisines:
                continue
        filtered_curated.append(row)

    curated = [
        _with_walking_distance(_normalize_curated_place(row), property_lat, property_lng)
        for row in filtered_curated[:safe_limit]
    ]

    nearby: list[dict[str, Any]] = []
    if property_lat is not None and property_lng is not None:
        google_raw = _search_google_places(
            lat=property_lat,
            lng=property_lng,
            query=normalized_query,
            cuisine=normalized_cuisine,
            price_level=price_level,
            open_now=open_now,
            limit=safe_limit + len(curated),
        )
        google_places = [_normalize_google_place(raw) for raw in google_raw]
        curated_google_ids = {
            str(row.get("google_place_id")).strip()
            for row in filtered_curated
            if row.get("google_place_id")
        }
        deduped_google = [
            place
            for place in google_places
            if place.get("place_id") and place.get("place_id") not in curated_google_ids
        ]
        nearby = [
            _with_walking_distance(place, property_lat, property_lng)
            for place in deduped_google[:safe_limit]
        ]

    structured = {
        "property_id": resolved_property_id,
        "curated": curated,
        "nearby": nearby,
        "count_curated": len(curated),
        "count_nearby": len(nearby),
    }

    log_tool_call(
        "search_nearby_places",
        f"Searched nearby places: '{normalized_query}'",
        property_id=resolved_property_id,
        request_payload={
            "room_id": normalized_room_id or None,
            "property_id_input": normalized_property_input or None,
            "property_id_resolved": resolved_property_id,
            "query": normalized_query,
            "cuisine": normalized_cuisine,
            "price_level": price_level,
            "open_now": open_now,
            "limit": safe_limit,
        },
        response_payload={
            "count_curated": len(curated),
            "count_nearby": len(nearby),
        },
    )

    return {
        "content": [
            {
                "type": "text",
                "text": (
                    f"Found {len(curated)} curated and {len(nearby)} nearby place(s) "
                    f"for '{normalized_query}'."
                ),
            }
        ],
        "structuredContent": structured,
        "_meta": {
            "ui": {"resourceUri": EXPERIENCES_CARD_WIDGET_URI},
            "openai/outputTemplate": EXPERIENCES_CARD_WIDGET_URI,
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
