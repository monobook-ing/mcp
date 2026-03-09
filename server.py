import os
import re
import uuid
import json
import logging
import math
from time import perf_counter
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional
from urllib import request, error
from urllib.parse import quote, urlparse

from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

load_dotenv()

from fastmcp import FastMCP

from account_context import get_account_property_ids, require_property_ownership
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
HOTEL_MAP_API_KEY_PLACEHOLDER = "__HOTEL_MAP_API_KEY__"


@lru_cache(maxsize=None)
def load_widget(name: str) -> str:
    return (WIDGETS_DIR / f"{name}.html").read_text(encoding="utf-8")


def create_app():
    mcp_app = mcp.http_app(stateless_http=True)
    mcp_app.mount("/widgets", StaticFiles(directory=str(WIDGETS_DIR)), name="widgets")

    try:
        from starlette.applications import Starlette
        from starlette.middleware import Middleware
        from starlette.responses import Response as StarletteResponse
        from starlette.routing import Mount, Route
        from account_middleware import AccountGatewayMiddleware
    except (ImportError, AttributeError):
        return mcp_app

    async def mcp_probe(_: Any):
        return StarletteResponse(
            status_code=200,
            headers={"Allow": "GET, HEAD, OPTIONS, POST"},
        )

    # Some connector UIs probe with GET before opening the MCP session.
    # Keep POST handled by FastMCP's built-in /mcp route.
    if hasattr(mcp_app, "routes"):
        mcp_app.routes.insert(
            0,
            Route("/mcp", endpoint=mcp_probe, methods=["GET", "HEAD", "OPTIONS"]),
        )

    outer_kwargs: dict[str, Any] = {}
    mcp_lifespan = getattr(mcp_app, "lifespan", None)
    if mcp_lifespan is not None:
        outer_kwargs["lifespan"] = mcp_lifespan

    outer = Starlette(
        routes=[
            Mount(
                "/v1/{account_id}",
                app=mcp_app,
                middleware=[Middleware(AccountGatewayMiddleware)],
            ),
            Mount("/", app=mcp_app),
        ],
        **outer_kwargs,
    )
    return outer


app = create_app()

UNIT_CARD_WIDGET_URI = "ui://widget/unit-card.html"
BOOKING_FORM_WIDGET_URI = "ui://widget/booking-form.html"
BOOKING_CONFIRMATION_WIDGET_URI = "ui://widget/booking-confirmation.html"
ROOM_GALLERY_WIDGET_URI = "ui://widget/room-gallery.html"
KNOWLEDGE_ANSWER_WIDGET_URI = "ui://widget/knowledge-answer.html"
SERVICES_CARD_WIDGET_URI = "ui://widget/services-card.html"
HOTEL_MAP_WIDGET_URI = "ui://widget/hotel-map.html"

MONOSEND_EMAILS_URL = "https://api.monosend.io/emails"
MONOSEND_TEMPLATE_ID = "7bc5aec5-cf40-4bec-88c5-d1c00b611fde"
MONOSEND_FROM_EMAIL = "noreply@monosend.email"
MONOSEND_API_KEY = os.getenv("API_KEY", "mono_bYguadb30GKyZ49gv0MxnJdgzG2xpHupQNw3Szbf87o")
MONOSEND_TIMEOUT_SECONDS = 10


def _normalize_https_origin(raw: str) -> str | None:
    value = raw.strip()
    if not value:
        return None

    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc:
        return None
    if parsed.params or parsed.query or parsed.fragment:
        return None
    if parsed.path not in ("", "/"):
        return None

    return f"https://{parsed.netloc}"


PUBLIC_WIDGET_ORIGIN = _normalize_https_origin(os.getenv("PUBLIC_WIDGET_ORIGIN", ""))


def _build_hotel_map_resource_meta() -> dict[str, Any]:
    meta: dict[str, Any] = {
        "openai/widgetDescription": "Interactive hotel map showing properties as pins with prices. Supports fullscreen mode with detail panels.",
        "openai/widgetPrefersBorder": False,
    }
    if PUBLIC_WIDGET_ORIGIN:
        meta["openai/widgetDomain"] = PUBLIC_WIDGET_ORIGIN
    return meta


def _build_hotel_map_resource_app_config() -> dict[str, Any] | None:
    if not PUBLIC_WIDGET_ORIGIN:
        return None
    return {
        "domain": PUBLIC_WIDGET_ORIGIN,
        "prefersBorder": False,
    }


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
    context_block = "\n".join(context_lines)
    user_prompt = (
        "Use the context below to answer the user question.\n\n"
        f"{context_block}\n\n"
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
        resolved_id = str(room["property_id"])
        require_property_ownership(resolved_id)
        return resolved_id

    normalized_property_id = str(property_id or "").strip()
    if normalized_property_id:
        require_property_ownership(normalized_property_id)
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


@mcp.resource(
    uri=SERVICES_CARD_WIDGET_URI,
    name="Services Card Widget",
    description="Displays property services/add-ons as cards with pricing, availability, and booking action",
    mime_type=RESOURCE_MIME,
    meta={
        "openai/widgetDescription": "Interactive service cards for browsing and booking property services and add-ons.",
        "openai/widgetPrefersBorder": False,
    },
)
def services_card_resource() -> str:
    return load_widget("services_card")


@mcp.resource(
    uri=HOTEL_MAP_WIDGET_URI,
    name="Hotel Map Widget",
    description="Interactive map with hotel pins, card list, fullscreen view, and detail panel",
    mime_type=RESOURCE_MIME,
    meta=_build_hotel_map_resource_meta(),
    app=_build_hotel_map_resource_app_config(),
)
def hotel_map_resource() -> str:
    return load_widget("hotel_map").replace(
        HOTEL_MAP_API_KEY_PLACEHOLDER,
        json.dumps(GOOGLE_PLACES_API_KEY or ""),
    )


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
    account_pids = get_account_property_ids()

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
    if account_pids is not None:
        if not account_pids:
            return {"hotels": [], "count": 0}
        scoped_pids = sorted(account_pids)
        placeholders = ", ".join(["%s"] * len(scoped_pids))
        conditions.append(f"p.id IN ({placeholders})")
        params.extend(scoped_pids)

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


def _normalize_search_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(re.split(r"\s+", str(value).strip().lower()))


def _normalize_search_amenities(raw_amenities: object) -> list[str]:
    if isinstance(raw_amenities, list):
        return [str(item) for item in raw_amenities if item is not None]
    if raw_amenities is None:
        return []
    return [str(raw_amenities)]


def _room_matches_text_filters(
    unit: dict[str, Any],
    normalized_query: str,
    normalized_amenity: str,
) -> bool:
    accommodation = unit.get("properties")
    if not isinstance(accommodation, dict):
        accommodation = {}

    amenities_list = _normalize_search_amenities(unit.get("amenities"))
    amenities_blob = _normalize_search_text(" ".join(amenities_list))
    searchable_blob = _normalize_search_text(
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
        query_terms = [term for term in normalized_query.split() if len(term) > 2]
        if not query_terms:
            query_terms = normalized_query.split()
        if not any(term in searchable_blob for term in query_terms):
            return False

    if normalized_amenity and normalized_amenity not in amenities_blob:
        return False
    return True


def _room_matches_amenity_filter(unit: dict[str, Any], normalized_amenity: str) -> bool:
    if not normalized_amenity:
        return True
    amenities_list = _normalize_search_amenities(unit.get("amenities"))
    amenities_blob = _normalize_search_text(" ".join(amenities_list))
    return normalized_amenity in amenities_blob


def _row_to_search_unit(row: dict[str, Any]) -> dict[str, Any]:
    """Convert a flat SQL row into the nested dict structure expected downstream."""
    data = dict(row)
    data["properties"] = {
        "city": data.pop("p_city", None),
        "state": data.pop("p_state", None),
        "country": data.pop("p_country", None),
        "rating": data.pop("p_rating", None),
        "image_url": data.pop("p_image_url", None),
        "lat": data.pop("p_lat", None),
        "lng": data.pop("p_lng", None),
        "name": data.pop("p_name", None),
        "street": data.pop("p_street", None),
        "account_name": data.pop("p_account_name", None),
        "account_logo_url": data.pop("p_account_logo_url", None),
        "account_external_url": data.pop("p_account_external_url", None),
    }
    return data


def _run_room_candidate_query(
    *,
    hotel_name: str,
    city: str,
    country: str,
    unit_type: str,
    max_price: Optional[float],
    min_guests: Optional[int],
    include_country: bool,
    require_coordinates: bool,
) -> list[dict[str, Any]]:
    conditions = []
    params = []
    account_pids = get_account_property_ids()

    if require_coordinates:
        conditions.extend(["p.lat IS NOT NULL", "p.lng IS NOT NULL"])
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
    if account_pids is not None:
        if not account_pids:
            return []
        scoped_pids = sorted(account_pids)
        placeholders = ", ".join(["%s"] * len(scoped_pids))
        conditions.append(f"r.property_id IN ({placeholders})")
        params.extend(scoped_pids)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"""
        SELECT r.*,
               p.city AS p_city, p.state AS p_state, p.country AS p_country,
               p.rating AS p_rating, p.image_url AS p_image_url,
               p.lat AS p_lat, p.lng AS p_lng, p.name AS p_name,
               p.street AS p_street,
               a.name AS p_account_name,
               a.logo_url AS p_account_logo_url,
               a.external_url AS p_account_external_url
        FROM rooms r
        JOIN properties p ON r.property_id = p.id
        JOIN accounts a ON p.account_id = a.id
        {where}
    """
    rows = fetch_all(sql, params or None)
    return [_row_to_search_unit(row) for row in rows]


def _search_room_candidates(
    *,
    hotel_name: str,
    city: str,
    country: str,
    unit_type: str,
    max_price: Optional[float],
    min_guests: Optional[int],
    amenity: str,
    query: str,
    require_coordinates: bool,
) -> tuple[list[dict[str, Any]], bool]:
    normalized_query = _normalize_search_text(query)
    normalized_amenity = _normalize_search_text(amenity)

    sql_results = _run_room_candidate_query(
        hotel_name=hotel_name,
        city=city,
        country=country,
        unit_type=unit_type,
        max_price=max_price,
        min_guests=min_guests,
        include_country=True,
        require_coordinates=require_coordinates,
    )
    units = [
        unit
        for unit in sql_results
        if _room_matches_text_filters(unit, normalized_query, normalized_amenity)
    ]
    if not units and sql_results and normalized_query:
        units = [
            unit
            for unit in sql_results
            if _room_matches_amenity_filter(unit, normalized_amenity)
        ]
    relaxed_country_filter = False

    # If city+country yields nothing, retry city-only for region ambiguities.
    if not units and city and country:
        relaxed_sql = _run_room_candidate_query(
            hotel_name=hotel_name,
            city=city,
            country=country,
            unit_type=unit_type,
            max_price=max_price,
            min_guests=min_guests,
            include_country=False,
            require_coordinates=require_coordinates,
        )
        relaxed_units = [
            unit
            for unit in relaxed_sql
            if _room_matches_text_filters(unit, normalized_query, normalized_amenity)
        ]
        if not relaxed_units and relaxed_sql and normalized_query:
            relaxed_units = [
                unit
                for unit in relaxed_sql
                if _room_matches_amenity_filter(unit, normalized_amenity)
            ]
        if relaxed_units:
            units = relaxed_units
            relaxed_country_filter = True

    return units, relaxed_country_filter


# ── Tool 2: search_properties_map ───────────────────────────────────────────
SEARCH_PROPERTIES_MAP_FOLLOW_UP_INSTRUCTIONS = (
    "Very important: DO NOT LIST ANY HOTEL in your answer, "
    "its all filtered by availability and shown to the user."
)

@mcp.tool(
    annotations={"readOnlyHint": True, "openWorldHint": False},
    app={"resourceUri": HOTEL_MAP_WIDGET_URI},
    meta={
        "openai/outputTemplate": HOTEL_MAP_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def search_properties_map(
    city: str = "",
    country: str = "",
    hotel_name: str = "",
    unit_type: str = "",
    max_price: Optional[float] = None,
    min_guests: Optional[int] = None,
    amenity: str = "",
    query: str = "",
    check_in: str = "",
    check_out: str = "",
) -> dict:
    """Search hotels/properties and display results on an interactive map with pins and cards.
    Best for browsing multiple properties in a city or region.
    Returns a map view with property locations, prices, and details."""

    def _build_response(properties: list[dict[str, Any]], relaxed_country_filter: bool = False) -> dict:
        structured = {
            "properties": properties,
            "count": len(properties),
            "check_in": check_in,
            "check_out": check_out,
            "query_context": {
                "city": city,
                "country": country,
                "hotel_name": hotel_name,
                "unit_type": unit_type,
                "max_price": max_price,
                "min_guests": min_guests,
                "amenity": amenity,
                "query": query,
            },
            "relaxed_country_filter": relaxed_country_filter,
            "coordinates_required": True,
        }
        return {
            "content": [
                {
                    "type": "text",
                    "text": (
                        f"Found {len(properties)} properties on the map. "
                        "Only properties with valid coordinates (lat/lng) are shown."
                    ),
                }
            ],
            "follow_up_instructions": SEARCH_PROPERTIES_MAP_FOLLOW_UP_INSTRUCTIONS,
            "structuredContent": structured,
            "_meta": {
                "ui": {"resourceUri": HOTEL_MAP_WIDGET_URI},
                "openai/outputTemplate": HOTEL_MAP_WIDGET_URI,
                "openai/widgetAccessible": True,
            },
        }

    units, relaxed_country_filter = _search_room_candidates(
        hotel_name=hotel_name,
        city=city,
        country=country,
        unit_type=unit_type,
        max_price=max_price,
        min_guests=min_guests,
        amenity=amenity,
        query=query,
        require_coordinates=True,
    )
    if not units:
        return _build_response([], relaxed_country_filter=relaxed_country_filter)

    today = date.today().isoformat()
    eff_in = check_in if check_in else today
    eff_out = check_out if check_out else (date.today() + timedelta(days=1)).isoformat()
    occupied_room_ids = _get_occupied_room_ids([str(unit["id"]) for unit in units], eff_in, eff_out)
    available_units = [unit for unit in units if str(unit["id"]) not in occupied_room_ids]

    if not available_units:
        return _build_response([], relaxed_country_filter=relaxed_country_filter)

    grouped: dict[str, dict[str, Any]] = {}

    for unit in available_units:
        property_id = str(unit.get("property_id") or "")
        if not property_id:
            continue

        accommodation = unit.get("properties")
        if not isinstance(accommodation, dict):
            accommodation = {}

        room_images = unit.get("images")
        if not isinstance(room_images, list):
            room_images = [str(room_images)] if room_images else []

        room_price = float(unit.get("price_per_night") or 0)
        room_image = room_images[0] if room_images else ""
        room_payload = {
            "id": str(unit.get("id") or ""),
            "name": unit.get("name"),
            "type": unit.get("type"),
            "price_per_night": room_price,
            "currency_code": unit.get("currency_code"),
            "max_guests": unit.get("max_guests"),
            "image_url": room_image,
            "bed_config": unit.get("bed_config"),
        }

        if property_id not in grouped:
            grouped[property_id] = {
                "id": property_id,
                "name": accommodation.get("name") or "Hotel",
                "image_url": accommodation.get("image_url") or "",
                "room_image": room_image,
                "rating": accommodation.get("rating"),
                "city": accommodation.get("city"),
                "state": accommodation.get("state"),
                "country": accommodation.get("country"),
                "street": accommodation.get("street"),
                "account_name": accommodation.get("account_name") or "",
                "account_logo_url": accommodation.get("account_logo_url") or "",
                "account_external_url": accommodation.get("account_external_url") or "",
                "lat": float(accommodation.get("lat") or 0),
                "lng": float(accommodation.get("lng") or 0),
                "rooms": [room_payload],
            }
            continue

        grouped[property_id]["rooms"].append(room_payload)
        if not grouped[property_id]["room_image"] and room_image:
            grouped[property_id]["room_image"] = room_image

    properties: list[dict[str, Any]] = []
    for prop in grouped.values():
        rooms = prop.pop("rooms")
        rooms.sort(key=lambda r: float(r.get("price_per_night") or 0))
        cheapest = rooms[0] if rooms else {}

        properties.append(
            {
                **prop,
                "room_image": prop.get("room_image") or cheapest.get("image_url") or "",
                "min_price": float(cheapest.get("price_per_night") or 0),
                "currency_code": cheapest.get("currency_code") or "USD",
                "room_count": len(rooms),
                "rooms": rooms[:3],
            }
        )

    properties.sort(key=lambda p: float(p.get("min_price") or 0))

    first_property = properties[0] if properties else {}
    log_tool_call(
        "search_properties_map",
        f"Searched properties on map: city={city!r} country={country!r} unit_type={unit_type!r}",
        property_id=str(first_property.get("id")) if first_property.get("id") else None,
        request_payload={
            "city": city,
            "country": country,
            "hotel_name": hotel_name,
            "unit_type": unit_type,
            "max_price": max_price,
            "min_guests": min_guests,
            "amenity": amenity,
            "query": query,
            "check_in": check_in,
            "check_out": check_out,
        },
        response_payload={
            "count": len(properties),
            "relaxed_country_filter": relaxed_country_filter,
            "coordinates_required": True,
        },
    )

    return _build_response(properties, relaxed_country_filter=relaxed_country_filter)


# ── Tool 3: search_rooms ──────────────────────────────────────────────────

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

    units, relaxed_country_filter = _search_room_candidates(
        hotel_name=hotel_name,
        city=city,
        country=country,
        unit_type=unit_type,
        max_price=max_price,
        min_guests=min_guests,
        amenity=amenity,
        query=query,
        require_coordinates=False,
    )

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

        amenities_list = _normalize_search_amenities(u.get("amenities"))
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
            require_property_ownership(str(unit["property_id"]))
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
        require_property_ownership(str(unit["property_id"]))

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
    require_property_ownership(str(unit["property_id"]))
    
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
            require_property_ownership(str(unit["property_id"]))
    else:
        lookup_name = (room_name or room_id).strip()
        if not lookup_name:
            raise ValueError("Either room_name or room_id is required.")

        rows = fetch_all(base_sql + " WHERE r.name = %s", [lookup_name])
        
        if not rows:
            raise ValueError(f"Unable to find room '{lookup_name}'.")
        unit = _row_to_unit(rows[0])
        require_property_ownership(str(unit["property_id"]))

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


# ── Services helpers ────────────────────────────────────────────────────────

def _looks_like_uuid(value: str) -> bool:
    normalized = str(value or "").strip()
    return (
        len(normalized) == 36
        and normalized.count("-") == 4
        and all(ch in "0123456789abcdef-" for ch in normalized.lower())
    )


def _normalize_currency_code(value: Any) -> str:
    return str(value or "USD").strip().upper() or "USD"


def _fetch_currency_display_map(codes: list[str]) -> dict[str, str]:
    unique_codes = sorted(
        {
            _normalize_currency_code(code)
            for code in codes
            if str(code or "").strip()
        }
    )
    if not unique_codes:
        return {}

    placeholders = ", ".join(["%s"] * len(unique_codes))
    rows = fetch_all(
        f"SELECT code, display FROM currencies WHERE code IN ({placeholders})",
        unique_codes,
    )
    return {
        _normalize_currency_code(row.get("code")): str(row.get("display") or "")
        for row in rows
    }


def _normalize_slot_time_key(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "T" in raw:
        raw = raw.split("T", 1)[-1]
    if "+" in raw:
        raw = raw.split("+", 1)[0]
    if "." in raw:
        raw = raw.split(".", 1)[0]
    pieces = raw.split(":")
    if len(pieces) >= 2:
        return f"{pieces[0].zfill(2)}:{pieces[1].zfill(2)}"
    return raw


def _service_is_slot_based(service: dict[str, Any]) -> bool:
    capacity_mode = str(service.get("capacity_mode") or "").lower()
    availability_type = str(service.get("availability_type") or "").lower()
    return capacity_mode == "per_hour_limit" or availability_type == "time_slot"


def _service_public_and_active(service: dict[str, Any]) -> bool:
    status = str(service.get("status") or "").lower()
    visibility = str(service.get("visibility") or "").lower()
    return status == "active" and visibility == "public"


def _as_service_slot(row: dict[str, Any]) -> dict[str, Any]:
    capacity = _to_int_or_none(row.get("capacity")) or 0
    booked = _to_int_or_none(row.get("booked")) or 0
    slot_time = _normalize_slot_time_key(row.get("slot_time"))
    return {
        "id": row.get("id"),
        "service_id": row.get("service_id"),
        "time": slot_time,
        "slot_time": slot_time,
        "capacity": capacity,
        "booked": booked,
        "sort_order": _to_int_or_none(row.get("sort_order")) or 0,
    }


def _load_service_slots_for_ids(service_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not service_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(service_ids))
    rows = fetch_all(
        f"""
        SELECT id, service_id, slot_time, capacity, booked, sort_order
        FROM service_time_slots
        WHERE service_id IN ({placeholders})
        ORDER BY service_id, sort_order ASC, slot_time ASC
        """,
        service_ids,
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        service_id = str(row.get("service_id"))
        grouped.setdefault(service_id, []).append(_as_service_slot(row))
    return grouped


def _normalize_service_record(
    row: dict[str, Any],
    slots_by_service_id: dict[str, list[dict[str, Any]]],
    currency_map: dict[str, str],
) -> dict[str, Any]:
    service_id = str(row.get("id"))
    image_urls_raw = row.get("image_urls")
    if isinstance(image_urls_raw, (list, tuple)):
        image_urls = [str(item) for item in image_urls_raw if item]
    else:
        image_urls = [str(image_urls_raw)] if image_urls_raw else []

    currency_code = _normalize_currency_code(row.get("currency_code"))
    currency_display = (
        str(row.get("currency_display") or "").strip()
        or currency_map.get(currency_code)
        or currency_code
    )
    slots = slots_by_service_id.get(service_id, [])
    slot_based = _service_is_slot_based(row) and bool(slots)
    has_available_slots = any(
        (_to_int_or_none(slot.get("capacity")) or 0) > (_to_int_or_none(slot.get("booked")) or 0)
        for slot in slots
    )

    return {
        "id": service_id,
        "property_id": str(row.get("property_id") or ""),
        "account_id": row.get("account_id"),
        "category_id": row.get("category_id"),
        "category_name": row.get("category_name"),
        "partner_id": row.get("partner_id"),
        "partner_name": row.get("partner_name"),
        "slug": row.get("slug"),
        "name": row.get("name"),
        "short_description": row.get("short_description") or "",
        "full_description": row.get("full_description") or "",
        "image_urls": image_urls,
        "type": row.get("type") or "internal",
        "status": row.get("status") or "draft",
        "visibility": row.get("visibility") or "public",
        "pricing_type": row.get("pricing_type") or "fixed",
        "price": _to_float_or_none(row.get("price")) or 0.0,
        "currency_code": currency_code,
        "currency_display": currency_display,
        "vat_percent": _to_float_or_none(row.get("vat_percent")) or 0.0,
        "availability_type": row.get("availability_type") or "always",
        "capacity_mode": row.get("capacity_mode") or "unlimited",
        "capacity_limit": _to_int_or_none(row.get("capacity_limit")),
        "available_before_booking": bool(row.get("available_before_booking", True)),
        "available_during_booking": bool(row.get("available_during_booking", True)),
        "post_booking_upsell": bool(row.get("post_booking_upsell", False)),
        "knowledge_language": row.get("knowledge_language") or "en",
        "slots": slots,
        "slot_based": slot_based,
        "has_available_slots": has_available_slots,
    }


def _fetch_services_for_property(
    property_id: str,
    category: str = "",
    search: str = "",
) -> list[dict[str, Any]]:
    normalized_category = str(category or "").strip()
    normalized_search = str(search or "").strip()

    sql = """
        SELECT
          s.*,
          sc.name AS category_name,
          sp.name AS partner_name,
          c.display AS currency_display
        FROM services s
        LEFT JOIN service_categories sc ON sc.id = s.category_id
        LEFT JOIN service_partners sp ON sp.id = s.partner_id
        LEFT JOIN currencies c ON c.code = s.currency_code
        WHERE s.property_id = %s::uuid
          AND s.status = 'active'
          AND s.visibility = 'public'
    """
    params: list[Any] = [property_id]

    if normalized_category:
        if _looks_like_uuid(normalized_category):
            sql += " AND s.category_id = %s::uuid"
            params.append(normalized_category)
        else:
            sql += " AND sc.name ILIKE %s"
            params.append(f"%{normalized_category}%")

    if normalized_search:
        sql += " AND (s.name ILIKE %s OR s.short_description ILIKE %s)"
        search_term = f"%{normalized_search}%"
        params.extend([search_term, search_term])

    sql += " ORDER BY s.created_at DESC, s.name ASC"
    rows = fetch_all(sql, params)

    service_ids = [str(row.get("id")) for row in rows if row.get("id")]
    slots_by_service = _load_service_slots_for_ids(service_ids)
    currency_map = _fetch_currency_display_map([str(row.get("currency_code") or "") for row in rows])
    return [
        _normalize_service_record(row, slots_by_service, currency_map)
        for row in rows
    ]


def _fetch_service_for_property(
    property_id: str,
    service_id: str,
    *,
    require_public_active: bool = True,
) -> dict[str, Any] | None:
    sql = """
        SELECT
          s.*,
          sc.name AS category_name,
          sp.name AS partner_name,
          c.display AS currency_display
        FROM services s
        LEFT JOIN service_categories sc ON sc.id = s.category_id
        LEFT JOIN service_partners sp ON sp.id = s.partner_id
        LEFT JOIN currencies c ON c.code = s.currency_code
        WHERE s.id = %s::uuid
          AND s.property_id = %s::uuid
    """
    params: list[Any] = [service_id, property_id]
    if require_public_active:
        sql += " AND s.status = 'active' AND s.visibility = 'public'"
    sql += " LIMIT 1"
    row = fetch_one(sql, params)
    if not row:
        return None

    slots_by_service = _load_service_slots_for_ids([str(row.get("id"))])
    currency_map = _fetch_currency_display_map([str(row.get("currency_code") or "")])
    return _normalize_service_record(row, slots_by_service, currency_map)


def _evaluate_service_availability(
    *,
    property_id: str,
    service: dict[str, Any],
    service_date: str,
    quantity: int = 1,
    slot_time: str = "",
) -> dict[str, Any]:
    service_id = str(service.get("id") or "")
    capacity_mode = str(service.get("capacity_mode") or "unlimited").lower()
    slots = service.get("slots") if isinstance(service.get("slots"), list) else []
    is_slot_based = _service_is_slot_based(service) and bool(slots)
    normalized_slot_key = _normalize_slot_time_key(slot_time)
    safe_quantity = max(1, int(quantity or 1))

    slot_summaries: list[dict[str, Any]] = []
    for slot in slots:
        slot_capacity = _to_int_or_none(slot.get("capacity")) or 0
        slot_booked = _to_int_or_none(slot.get("booked")) or 0
        slot_remaining = max(slot_capacity - slot_booked, 0)
        slot_summaries.append(
            {
                **slot,
                "remaining": slot_remaining,
                "available": slot_capacity > 0 and (slot_booked + safe_quantity <= slot_capacity),
            }
        )

    availability: dict[str, Any] = {
        "available": True,
        "service_id": service_id,
        "service_name": service.get("name"),
        "service_date": service_date,
        "quantity": safe_quantity,
        "capacity_mode": capacity_mode,
        "slots": slot_summaries,
    }

    if is_slot_based:
        selected_slot: dict[str, Any] | None = None
        if normalized_slot_key:
            for slot in slot_summaries:
                if _normalize_slot_time_key(slot.get("time")) == normalized_slot_key:
                    selected_slot = slot
                    break
        else:
            for slot in slot_summaries:
                if slot.get("available"):
                    selected_slot = slot
                    break

        if not selected_slot:
            availability.update(
                {
                    "available": False,
                    "remaining": 0,
                    "error": "No available slot for the selected service.",
                }
            )
            return availability

        selected_capacity = _to_int_or_none(selected_slot.get("capacity")) or 0
        selected_booked = _to_int_or_none(selected_slot.get("booked")) or 0
        selected_remaining = max(selected_capacity - selected_booked, 0)
        slot_is_available = selected_capacity > 0 and (
            selected_booked + safe_quantity <= selected_capacity
        )
        availability.update(
            {
                "available": slot_is_available,
                "remaining": selected_remaining,
                "slot_id": selected_slot.get("id"),
                "slot_time": selected_slot.get("time"),
                "slot_capacity": selected_capacity,
                "slot_booked": selected_booked,
            }
        )
        if not slot_is_available:
            availability["error"] = "Requested quantity exceeds slot capacity."
        return availability

    if capacity_mode == "unlimited":
        availability["remaining"] = None
        return availability

    if capacity_mode in {"limited_quantity", "per_day_limit", "per_hour_limit"}:
        capacity_limit = _to_int_or_none(service.get("capacity_limit")) or 0
        availability["capacity_limit"] = capacity_limit
        if capacity_limit <= 0:
            availability.update(
                {
                    "available": False,
                    "remaining": 0,
                    "error": "Service capacity_limit is not configured.",
                }
            )
            return availability

        sql = """
            SELECT COALESCE(SUM(quantity), 0) AS booked_quantity
            FROM service_bookings
            WHERE property_id = %s::uuid
              AND service_id = %s::uuid
              AND status != 'cancelled'
        """
        params: list[Any] = [property_id, service_id]
        if capacity_mode in {"per_day_limit", "per_hour_limit"}:
            sql += " AND service_date = %s::date"
            params.append(service_date)

        booked_row = fetch_one(sql, params) or {}
        booked_quantity = _to_int_or_none(booked_row.get("booked_quantity")) or 0
        remaining = max(capacity_limit - booked_quantity, 0)
        available = booked_quantity + safe_quantity <= capacity_limit
        availability.update(
            {
                "booked_quantity": booked_quantity,
                "remaining": remaining,
                "available": available,
            }
        )
        if not available:
            availability["error"] = "Requested quantity exceeds remaining capacity."
        return availability

    availability["remaining"] = None
    return availability


def _coerce_to_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Date value is required.")
    return date.fromisoformat(raw.split("T", 1)[0])


def _calculate_service_total(
    *,
    property_id: str,
    service: dict[str, Any],
    quantity: int,
    booking_id: str = "",
) -> dict[str, Any]:
    pricing_type = str(service.get("pricing_type") or "fixed").lower()
    unit_price = _to_float_or_none(service.get("price")) or 0.0
    multiplier = max(1, int(quantity or 1))

    normalized_booking_id = str(booking_id or "").strip()
    if pricing_type == "per_night" and normalized_booking_id:
        booking_row = fetch_one(
            """
            SELECT check_in, check_out
            FROM bookings
            WHERE id = %s::uuid
              AND property_id = %s::uuid
            LIMIT 1
            """,
            [normalized_booking_id, property_id],
        )
        if not booking_row:
            raise ValueError("booking_id not found for this property.")
        check_in = _coerce_to_date(booking_row.get("check_in"))
        check_out = _coerce_to_date(booking_row.get("check_out"))
        nights = (check_out - check_in).days
        if nights <= 0:
            raise ValueError("booking_id has invalid stay dates.")
        multiplier = nights * max(1, int(quantity or 1))

    total = round(unit_price * multiplier, 2)
    return {
        "pricing_type": pricing_type,
        "unit_price": unit_price,
        "multiplier": multiplier,
        "total": total,
    }


def _get_property_team_emails(property_id: str) -> tuple[list[str], str]:
    property_row = fetch_one(
        """
        SELECT
          p.account_id,
          COALESCE(
            NULLIF(TRIM(p.description), ''),
            NULLIF(TRIM(a.name), ''),
            NULLIF(TRIM(p.city), ''),
            'Property'
          ) AS property_name
        FROM properties p
        JOIN accounts a ON a.id = p.account_id
        WHERE p.id = %s::uuid
        LIMIT 1
        """,
        [property_id],
    )
    if not property_row:
        return ([], "Property")

    account_id = property_row.get("account_id")
    property_name = str(property_row.get("property_name") or "Property")
    if not account_id:
        return ([], property_name)

    rows = fetch_all(
        """
        SELECT DISTINCT u.email
        FROM team_members tm
        JOIN users u ON u.id = tm.user_id
        WHERE tm.account_id = %s::uuid
          AND tm.status = 'accepted'
          AND tm.deleted_at IS NULL
          AND u.email IS NOT NULL
          AND TRIM(u.email) != ''
        """,
        [account_id],
    )
    emails = sorted(
        {
            str(row.get("email") or "").strip()
            for row in rows
            if str(row.get("email") or "").strip()
        }
    )
    return (emails, property_name)


def _send_service_booking_notification_email(
    *,
    recipient_emails: list[str],
    service_name: str,
    guest_name: str,
    service_date: str,
    quantity: int,
    total: float,
    currency_code: str,
    external_ref: str,
    property_name: str,
) -> None:
    cleaned_recipients = sorted(
        {
            str(email).strip()
            for email in recipient_emails
            if str(email).strip()
        }
    )
    if not cleaned_recipients:
        return

    total_display = f"{float(total):.2f} {_normalize_currency_code(currency_code)}"
    subject = f"New service booking: {service_name}"
    text_body = "\n".join(
        [
            "A new service booking was created.",
            "",
            f"Property: {property_name}",
            f"Service: {service_name}",
            f"Guest: {guest_name}",
            f"Date: {service_date}",
            f"Quantity: {quantity}",
            f"Total: {total_display}",
            f"Reference: {external_ref}",
        ]
    )
    html_body = (
        "<p>A new service booking was created.</p>"
        f"<p><strong>Property:</strong> {property_name}<br/>"
        f"<strong>Service:</strong> {service_name}<br/>"
        f"<strong>Guest:</strong> {guest_name}<br/>"
        f"<strong>Date:</strong> {service_date}<br/>"
        f"<strong>Quantity:</strong> {quantity}<br/>"
        f"<strong>Total:</strong> {total_display}<br/>"
        f"<strong>Reference:</strong> {external_ref}</p>"
    )

    payload = {
        "to": cleaned_recipients,
        "from": MONOSEND_FROM_EMAIL,
        "subject": subject,
        "text": text_body,
        "html": html_body,
    }
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


def _service_widget_meta() -> dict[str, Any]:
    return {
        "ui": {"resourceUri": SERVICES_CARD_WIDGET_URI},
        "openai/outputTemplate": SERVICES_CARD_WIDGET_URI,
        "openai/widgetAccessible": True,
    }


# ── Tool 8: list_services ───────────────────────────────────────────────────

@mcp.tool(
    annotations={"readOnlyHint": True, "openWorldHint": False},
    app={"resourceUri": SERVICES_CARD_WIDGET_URI},
    meta={
        "openai/outputTemplate": SERVICES_CARD_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def list_services(
    property_id: str = "",
    room_id: str = "",
    category: str = "",
    search: str = "",
) -> dict:
    """List active public services for a property with optional category/search filters."""
    normalized_property_input = str(property_id or "").strip()
    normalized_room_id = str(room_id or "").strip()
    normalized_category = str(category or "").strip()
    normalized_search = str(search or "").strip()

    if not normalized_property_input and not normalized_room_id:
        raise ValueError("Either property_id or room_id is required for list_services.")
    resolved_property_id = _resolve_property_id(normalized_property_input, normalized_room_id)

    services = _fetch_services_for_property(
        property_id=resolved_property_id,
        category=normalized_category,
        search=normalized_search,
    )
    structured = {
        "property_id": resolved_property_id,
        "services": services,
        "count": len(services),
    }

    log_tool_call(
        "list_services",
        f"Listed services (count={len(services)}).",
        property_id=resolved_property_id,
        request_payload={
            "room_id": normalized_room_id or None,
            "property_id_input": normalized_property_input or None,
            "property_id_resolved": resolved_property_id,
            "category": normalized_category or None,
            "search": normalized_search or None,
        },
        response_payload={"count": len(services)},
    )

    return {
        "content": [
            {
                "type": "text",
                "text": f"Loaded {len(services)} service(s).",
            }
        ],
        "structuredContent": structured,
        "_meta": _service_widget_meta(),
    }


# ── Tool 9: get_service_details ─────────────────────────────────────────────

@mcp.tool(
    annotations={"readOnlyHint": True, "openWorldHint": False},
    app={"resourceUri": SERVICES_CARD_WIDGET_URI},
    meta={
        "openai/outputTemplate": SERVICES_CARD_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def get_service_details(
    service_id: str,
    property_id: str = "",
    room_id: str = "",
) -> dict:
    """Get details for one active/public service, including slots and partner/category metadata."""
    normalized_service_id = str(service_id or "").strip()
    if not normalized_service_id:
        raise ValueError("service_id is required.")
    if not _looks_like_uuid(normalized_service_id):
        raise ValueError("service_id must be a UUID.")

    normalized_property_input = str(property_id or "").strip()
    normalized_room_id = str(room_id or "").strip()
    if not normalized_property_input and not normalized_room_id:
        raise ValueError("Either property_id or room_id is required for get_service_details.")
    resolved_property_id = _resolve_property_id(normalized_property_input, normalized_room_id)

    service = _fetch_service_for_property(
        property_id=resolved_property_id,
        service_id=normalized_service_id,
        require_public_active=True,
    )
    if not service:
        raise ValueError("Service not found.")

    structured = {
        "property_id": resolved_property_id,
        "service": service,
    }

    log_tool_call(
        "get_service_details",
        f"Loaded service details for {normalized_service_id}.",
        property_id=resolved_property_id,
        request_payload={
            "service_id": normalized_service_id,
            "room_id": normalized_room_id or None,
            "property_id_input": normalized_property_input or None,
            "property_id_resolved": resolved_property_id,
        },
        response_payload={"found": True},
    )

    return {
        "content": [
            {
                "type": "text",
                "text": f"Loaded details for {service.get('name') or 'service'}.",
            }
        ],
        "structuredContent": structured,
        "_meta": _service_widget_meta(),
    }


# ── Tool 10: check_service_availability ─────────────────────────────────────

@mcp.tool(
    annotations={"readOnlyHint": True, "openWorldHint": False},
)
def check_service_availability(
    service_id: str,
    service_date: str = "",
    slot_time: str = "",
    property_id: str = "",
    room_id: str = "",
) -> dict:
    """Check service availability for a date and optional slot."""
    normalized_service_id = str(service_id or "").strip()
    if not normalized_service_id:
        raise ValueError("service_id is required.")
    if not _looks_like_uuid(normalized_service_id):
        raise ValueError("service_id must be a UUID.")

    normalized_property_input = str(property_id or "").strip()
    normalized_room_id = str(room_id or "").strip()
    if not normalized_property_input and not normalized_room_id:
        raise ValueError("Either property_id or room_id is required for check_service_availability.")
    resolved_property_id = _resolve_property_id(normalized_property_input, normalized_room_id)

    normalized_service_date = str(service_date or "").strip() or date.today().isoformat()
    try:
        normalized_service_date = date.fromisoformat(normalized_service_date).isoformat()
    except ValueError as exc:
        raise ValueError("service_date must be in YYYY-MM-DD format.") from exc

    service = _fetch_service_for_property(
        property_id=resolved_property_id,
        service_id=normalized_service_id,
        require_public_active=True,
    )
    if not service:
        raise ValueError("Service not found.")
    if not _service_public_and_active(service):
        raise ValueError("Service is not active and public.")

    availability = _evaluate_service_availability(
        property_id=resolved_property_id,
        service=service,
        service_date=normalized_service_date,
        quantity=1,
        slot_time=slot_time,
    )
    result = {
        "available": bool(availability.get("available")),
        "remaining": availability.get("remaining"),
        "service_name": service.get("name"),
        "service_id": normalized_service_id,
        "service_date": normalized_service_date,
        "slot_time": availability.get("slot_time") or _normalize_slot_time_key(slot_time),
        "capacity_mode": availability.get("capacity_mode"),
        "slots": availability.get("slots", []),
    }
    if availability.get("error"):
        result["error"] = availability["error"]

    log_tool_call(
        "check_service_availability",
        (
            f"Checked availability for {normalized_service_id}: "
            f"{'available' if result['available'] else 'unavailable'}"
        ),
        property_id=resolved_property_id,
        request_payload={
            "service_id": normalized_service_id,
            "service_date": normalized_service_date,
            "slot_time": _normalize_slot_time_key(slot_time) or None,
            "room_id": normalized_room_id or None,
            "property_id_input": normalized_property_input or None,
            "property_id_resolved": resolved_property_id,
        },
        response_payload={
            "available": result["available"],
            "remaining": result.get("remaining"),
            "slot_time": result.get("slot_time"),
        },
    )

    return result


# ── Tool 11: book_service ───────────────────────────────────────────────────

@mcp.tool(
    annotations={
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": False,
    },
    app={"resourceUri": SERVICES_CARD_WIDGET_URI},
    meta={
        "openai/outputTemplate": SERVICES_CARD_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def book_service(
    service_id: str,
    guest_name: str,
    service_date: str,
    quantity: int = 1,
    guest_email: str = "",
    slot_time: str = "",
    booking_id: str = "",
    property_id: str = "",
    room_id: str = "",
) -> dict:
    """Book a service atomically after checking availability and capacity."""
    normalized_service_id = str(service_id or "").strip()
    if not normalized_service_id:
        raise ValueError("service_id is required.")
    if not _looks_like_uuid(normalized_service_id):
        raise ValueError("service_id must be a UUID.")

    cleaned_guest_name = _sanitize(guest_name)
    if not cleaned_guest_name:
        raise ValueError("guest_name is required.")
    cleaned_guest_email = _sanitize(guest_email)

    safe_quantity = int(quantity or 1)
    if safe_quantity <= 0:
        raise ValueError("quantity must be greater than 0.")

    normalized_service_date = str(service_date or "").strip()
    if not normalized_service_date:
        raise ValueError("service_date is required.")
    try:
        normalized_service_date = date.fromisoformat(normalized_service_date).isoformat()
    except ValueError as exc:
        raise ValueError("service_date must be in YYYY-MM-DD format.") from exc

    normalized_property_input = str(property_id or "").strip()
    normalized_room_id = str(room_id or "").strip()
    if not normalized_property_input and not normalized_room_id:
        raise ValueError("Either property_id or room_id is required for book_service.")
    resolved_property_id = _resolve_property_id(normalized_property_input, normalized_room_id)

    service = _fetch_service_for_property(
        property_id=resolved_property_id,
        service_id=normalized_service_id,
        require_public_active=True,
    )
    if not service:
        raise ValueError("Service not found.")
    if not _service_public_and_active(service):
        raise ValueError("Service is not available for booking.")

    availability = _evaluate_service_availability(
        property_id=resolved_property_id,
        service=service,
        service_date=normalized_service_date,
        quantity=safe_quantity,
        slot_time=slot_time,
    )
    if not availability.get("available"):
        raise ValueError(str(availability.get("error") or "Service is not available."))

    normalized_booking_id = str(booking_id or "").strip()
    if normalized_booking_id and not _looks_like_uuid(normalized_booking_id):
        raise ValueError("booking_id must be a UUID.")
    pricing = _calculate_service_total(
        property_id=resolved_property_id,
        service=service,
        quantity=safe_quantity,
        booking_id=normalized_booking_id,
    )
    external_ref = f"SB-{uuid.uuid4().hex[:6].upper()}"
    currency_code = _normalize_currency_code(service.get("currency_code"))

    inserted = execute_returning(
        """
        INSERT INTO service_bookings
          (property_id, service_id, booking_id, external_ref, guest_name, service_date,
           quantity, total, currency_code, status)
        VALUES
          (%s::uuid, %s::uuid, NULLIF(%s, '')::uuid, %s, %s, %s::date,
           %s, %s, %s, %s::service_booking_status)
        RETURNING *
        """,
        [
            resolved_property_id,
            normalized_service_id,
            normalized_booking_id,
            external_ref,
            cleaned_guest_name,
            normalized_service_date,
            safe_quantity,
            pricing["total"],
            currency_code,
            "confirmed",
        ],
    )
    if not inserted:
        raise RuntimeError("Failed to create service booking.")

    selected_slot_id = availability.get("slot_id")
    selected_slot_time = availability.get("slot_time")
    if selected_slot_id:
        execute(
            """
            UPDATE service_time_slots
            SET booked = GREATEST(booked + %s, 0)
            WHERE id = %s::uuid
              AND service_id = %s::uuid
            """,
            [safe_quantity, selected_slot_id, normalized_service_id],
        )

    try:
        recipient_emails, property_name = _get_property_team_emails(resolved_property_id)
        _send_service_booking_notification_email(
            recipient_emails=recipient_emails,
            service_name=str(service.get("name") or "Service"),
            guest_name=cleaned_guest_name,
            service_date=normalized_service_date,
            quantity=safe_quantity,
            total=float(pricing["total"]),
            currency_code=currency_code,
            external_ref=external_ref,
            property_name=property_name,
        )
    except Exception as exc:
        logger.warning("Failed to send service booking notification email: %s", exc)

    structured = {
        "service_booking_id": inserted.get("id"),
        "external_ref": external_ref,
        "status": inserted.get("status") or "confirmed",
        "service_id": normalized_service_id,
        "service_name": service.get("name"),
        "guest_name": cleaned_guest_name,
        "guest_email": cleaned_guest_email or None,
        "service_date": normalized_service_date,
        "quantity": safe_quantity,
        "unit_price": pricing["unit_price"],
        "pricing_type": pricing["pricing_type"],
        "total": pricing["total"],
        "currency_code": currency_code,
        "slot_time": selected_slot_time,
        "booking_id": normalized_booking_id or None,
        "property_id": resolved_property_id,
        "message": f"Service booked successfully. Reference: {external_ref}",
    }

    log_tool_call(
        "book_service",
        f"Booked service {normalized_service_id} for {cleaned_guest_name}.",
        property_id=resolved_property_id,
        request_payload={
            "service_id": normalized_service_id,
            "service_date": normalized_service_date,
            "quantity": safe_quantity,
            "slot_time": _normalize_slot_time_key(slot_time) or None,
            "booking_id": normalized_booking_id or None,
            "guest_name": cleaned_guest_name,
            "guest_email": cleaned_guest_email or None,
            "room_id": normalized_room_id or None,
            "property_id_input": normalized_property_input or None,
            "property_id_resolved": resolved_property_id,
        },
        response_payload={
            "service_booking_id": inserted.get("id"),
            "external_ref": external_ref,
            "total": pricing["total"],
            "slot_time": selected_slot_time,
        },
    )

    return {
        "content": [
            {
                "type": "text",
                "text": str(structured["message"]),
            }
        ],
        "structuredContent": structured,
        "_meta": _service_widget_meta(),
    }


# ── Tool 12: cancel_service_booking ─────────────────────────────────────────

@mcp.tool(
    annotations={
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": False,
    },
)
def cancel_service_booking(
    service_booking_id: str,
    property_id: str = "",
    room_id: str = "",
) -> dict:
    """Cancel an existing service booking and release slot capacity when possible."""
    normalized_service_booking_id = str(service_booking_id or "").strip()
    if not normalized_service_booking_id:
        raise ValueError("service_booking_id is required.")
    if not _looks_like_uuid(normalized_service_booking_id):
        raise ValueError("service_booking_id must be a UUID.")

    normalized_property_input = str(property_id or "").strip()
    normalized_room_id = str(room_id or "").strip()
    if not normalized_property_input and not normalized_room_id:
        raise ValueError("Either property_id or room_id is required for cancel_service_booking.")
    resolved_property_id = _resolve_property_id(normalized_property_input, normalized_room_id)

    booking = fetch_one(
        """
        SELECT *
        FROM service_bookings
        WHERE id = %s::uuid
          AND property_id = %s::uuid
        LIMIT 1
        """,
        [normalized_service_booking_id, resolved_property_id],
    )
    if not booking:
        raise ValueError("Service booking not found.")

    current_status = str(booking.get("status") or "").lower()
    if current_status == "cancelled":
        result = {
            "cancelled": True,
            "external_ref": booking.get("external_ref"),
            "service_booking_id": normalized_service_booking_id,
            "message": "Service booking is already cancelled.",
        }
        log_tool_call(
            "cancel_service_booking",
            f"Service booking already cancelled: {normalized_service_booking_id}.",
            property_id=resolved_property_id,
            request_payload={
                "service_booking_id": normalized_service_booking_id,
                "room_id": normalized_room_id or None,
                "property_id_input": normalized_property_input or None,
                "property_id_resolved": resolved_property_id,
            },
            response_payload=result,
        )
        return result

    execute(
        """
        UPDATE service_bookings
        SET status = 'cancelled',
            updated_at = now()
        WHERE id = %s::uuid
          AND property_id = %s::uuid
        """,
        [normalized_service_booking_id, resolved_property_id],
    )

    decremented_slot_time: str | None = None
    service_id = str(booking.get("service_id") or "")
    service = _fetch_service_for_property(
        property_id=resolved_property_id,
        service_id=service_id,
        require_public_active=False,
    )
    slots = service.get("slots") if service and isinstance(service.get("slots"), list) else []
    if service and _service_is_slot_based(service) and slots:
        target_slot = None
        for slot in slots:
            if (_to_int_or_none(slot.get("booked")) or 0) > 0:
                target_slot = slot
                break
        if target_slot and target_slot.get("id"):
            execute(
                """
                UPDATE service_time_slots
                SET booked = GREATEST(booked - %s, 0)
                WHERE id = %s::uuid
                  AND service_id = %s::uuid
                """,
                [
                    _to_int_or_none(booking.get("quantity")) or 1,
                    target_slot.get("id"),
                    service_id,
                ],
            )
            decremented_slot_time = str(target_slot.get("time") or "")

    result = {
        "cancelled": True,
        "external_ref": booking.get("external_ref"),
        "service_booking_id": normalized_service_booking_id,
        "decremented_slot_time": decremented_slot_time,
        "message": "Service booking cancelled successfully.",
    }

    log_tool_call(
        "cancel_service_booking",
        f"Cancelled service booking {normalized_service_booking_id}.",
        property_id=resolved_property_id,
        request_payload={
            "service_booking_id": normalized_service_booking_id,
            "room_id": normalized_room_id or None,
            "property_id_input": normalized_property_input or None,
            "property_id_resolved": resolved_property_id,
        },
        response_payload=result,
    )
    return result


def _is_service_related_chunk(chunk: dict[str, Any]) -> bool:
    metadata = chunk.get("metadata") if isinstance(chunk.get("metadata"), dict) else {}
    searchable_text = " ".join(
        [
            str(metadata.get("doc_type") or ""),
            str(metadata.get("section") or ""),
            str(metadata.get("file_name") or ""),
            str(chunk.get("content") or ""),
        ]
    ).lower()
    keywords = (
        "service",
        "services",
        "add-on",
        "addon",
        "spa",
        "wellness",
        "massage",
        "tour",
        "transfer",
        "concierge",
    )
    return any(keyword in searchable_text for keyword in keywords)


# ── Tool 13: search_service_kb ──────────────────────────────────────────────

@mcp.tool(
    annotations={"readOnlyHint": True, "openWorldHint": False},
    app={"resourceUri": KNOWLEDGE_ANSWER_WIDGET_URI},
    meta={
        "openai/outputTemplate": KNOWLEDGE_ANSWER_WIDGET_URI,
        "openai/widgetAccessible": True,
    },
)
def search_service_kb(
    question: str,
    property_id: str = "",
    room_id: str = "",
    language: str = "",
) -> dict:
    """Answer service-related guest questions from indexed property knowledge chunks."""
    started_at = perf_counter()
    normalized_question = str(question or "").strip()
    if not normalized_question:
        raise ValueError("question is required.")

    normalized_property_input = str(property_id or "").strip()
    normalized_room_id = str(room_id or "").strip()
    if not normalized_property_input and not normalized_room_id:
        raise ValueError("Either property_id or room_id is required for search_service_kb.")
    resolved_property_id = _resolve_property_id(normalized_property_input, normalized_room_id)
    normalized_language = str(language or "").strip().lower() or None

    try:
        query_embedding = _embed_query(normalized_question)
        chunks = _search_chunks(
            property_id=resolved_property_id,
            embedding=query_embedding,
            language=normalized_language,
            limit=10,
        )
        if not chunks:
            answer = (
                "I couldn't find service-related details in the uploaded knowledge files. "
                "Please ask the property owner to upload service policies or FAQs."
            )
            latency_ms = int((perf_counter() - started_at) * 1000)
            _log_rag_query(
                property_id=resolved_property_id,
                question=normalized_question,
                answer=answer,
                chunks_used=[],
                language=normalized_language,
                latency_ms=latency_ms,
            )
            log_tool_call(
                "search_service_kb",
                "No service knowledge chunks found.",
                property_id=resolved_property_id,
                request_payload={
                    "question": normalized_question,
                    "room_id": normalized_room_id or None,
                    "language": normalized_language,
                    "property_id_input": normalized_property_input or None,
                    "property_id_resolved": resolved_property_id,
                },
                response_payload={"answer": answer, "chunks_used": []},
            )
            return {
                "content": [{"type": "text", "text": answer}],
                "structuredContent": {
                    "property_id": resolved_property_id,
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

        service_chunks = [chunk for chunk in chunks if _is_service_related_chunk(chunk)]
        selected_chunks = (service_chunks or chunks)[:8]
        answer = _build_rag_answer(normalized_question, selected_chunks)

        sources = []
        for chunk in selected_chunks:
            metadata = chunk.get("metadata") if isinstance(chunk.get("metadata"), dict) else {}
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
            property_id=resolved_property_id,
            question=normalized_question,
            answer=answer,
            chunks_used=sources,
            language=normalized_language,
            latency_ms=latency_ms,
        )

        structured = {
            "property_id": resolved_property_id,
            "question": normalized_question,
            "answer": answer,
            "sources": sources,
        }
        log_tool_call(
            "search_service_kb",
            f"Answered service knowledge query using {len(sources)} chunk(s).",
            property_id=resolved_property_id,
            request_payload={
                "question": normalized_question,
                "room_id": normalized_room_id or None,
                "language": normalized_language,
                "property_id_input": normalized_property_input or None,
                "property_id_resolved": resolved_property_id,
                "service_filtered_chunk_count": len(service_chunks),
            },
            response_payload={"answer": answer, "chunks_used_count": len(sources)},
        )
        return {
            "content": [{"type": "text", "text": answer}],
            "structuredContent": structured,
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
            property_id=resolved_property_id,
            question=normalized_question,
            answer=f"ERROR: {error_message}",
            chunks_used=[],
            language=normalized_language,
            latency_ms=latency_ms,
        )
        log_tool_call(
            "search_service_kb",
            f"Service knowledge query failed: {error_message}",
            status="error",
            property_id=resolved_property_id,
            request_payload={
                "question": normalized_question,
                "room_id": normalized_room_id or None,
                "language": normalized_language,
                "property_id_input": normalized_property_input or None,
                "property_id_resolved": resolved_property_id,
            },
            response_payload={"error": error_message},
        )
        raise


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
app.routes.insert(0, Route("/v1/{account_id}/ping", endpoint=ping_get, methods=["GET", "HEAD", "OPTIONS"]))


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
