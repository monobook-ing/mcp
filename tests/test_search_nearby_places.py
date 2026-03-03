import os
import types
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

import sys

# Mock psycopg2 pool before db/server imports
sys.modules["psycopg2"] = MagicMock()
sys.modules["psycopg2.pool"] = MagicMock()
sys.modules["psycopg2.extras"] = MagicMock()
sys.modules["dotenv"] = MagicMock()

fastapi_module = types.ModuleType("fastapi")
fastapi_staticfiles_module = types.ModuleType("fastapi.staticfiles")


class DummyStaticFiles:
    def __init__(self, *args, **kwargs):
        pass


fastapi_staticfiles_module.StaticFiles = DummyStaticFiles
sys.modules["fastapi"] = fastapi_module
sys.modules["fastapi.staticfiles"] = fastapi_staticfiles_module

fastmcp_module = types.ModuleType("fastmcp")


class DummyApp:
    def __init__(self):
        self.routes = []

    def mount(self, *args, **kwargs):
        return None


class DummyFastMCP:
    def __init__(self, *args, **kwargs):
        pass

    def http_app(self, *args, **kwargs):
        return DummyApp()

    def resource(self, *args, **kwargs):
        def decorator(fn):
            return fn

        return decorator

    def tool(self, *args, **kwargs):
        def decorator(fn):
            return fn

        return decorator


fastmcp_module.FastMCP = DummyFastMCP
sys.modules["fastmcp"] = fastmcp_module

starlette_requests_module = types.ModuleType("starlette.requests")
starlette_responses_module = types.ModuleType("starlette.responses")
starlette_routing_module = types.ModuleType("starlette.routing")


class DummyRequest:
    pass


class DummyJSONResponse(dict):
    def __init__(self, content, *args, **kwargs):
        super().__init__(content)


class DummyResponse:
    def __init__(self, *args, **kwargs):
        pass


class DummyRoute:
    def __init__(self, *args, **kwargs):
        pass


starlette_requests_module.Request = DummyRequest
starlette_responses_module.JSONResponse = DummyJSONResponse
starlette_responses_module.Response = DummyResponse
starlette_routing_module.Route = DummyRoute
sys.modules["starlette.requests"] = starlette_requests_module
sys.modules["starlette.responses"] = starlette_responses_module
sys.modules["starlette.routing"] = starlette_routing_module

import server


class SearchNearbyPlacesTests(unittest.TestCase):
    def test_property_id_only(self):
        log_tool_call_mock = MagicMock()

        def fetch_one_side_effect(sql, params=None):
            if "FROM rooms" in sql:
                raise AssertionError("Room lookup should not happen for property_id-only input")
            if "FROM properties" in sql:
                self.assertEqual(params, ["prop-1"])
                return {"id": "prop-1", "lat": None, "lng": None}
            raise AssertionError(f"Unexpected SQL: {sql}")

        with (
            patch.object(server, "fetch_one", side_effect=fetch_one_side_effect),
            patch.object(server, "fetch_all", return_value=[]),
            patch.object(server, "_search_google_places", return_value=[]) as google_mock,
            patch.object(server, "log_tool_call", log_tool_call_mock),
        ):
            result = server.search_nearby_places(property_id="prop-1", query="sushi")

        self.assertEqual(result["structuredContent"]["property_id"], "prop-1")
        google_mock.assert_not_called()
        payload = log_tool_call_mock.call_args.kwargs["request_payload"]
        self.assertEqual(payload["property_id_input"], "prop-1")
        self.assertEqual(payload["property_id_resolved"], "prop-1")
        self.assertIsNone(payload["room_id"])

    def test_room_id_only_resolves_property(self):
        log_tool_call_mock = MagicMock()

        def fetch_one_side_effect(sql, params=None):
            if "FROM rooms" in sql:
                self.assertEqual(params, ["room-1"])
                return {"property_id": "prop-room"}
            if "FROM properties" in sql:
                self.assertEqual(params, ["prop-room"])
                return {"id": "prop-room", "lat": None, "lng": None}
            raise AssertionError(f"Unexpected SQL: {sql}")

        with (
            patch.object(server, "fetch_one", side_effect=fetch_one_side_effect),
            patch.object(server, "fetch_all", return_value=[]),
            patch.object(server, "_search_google_places", return_value=[]),
            patch.object(server, "log_tool_call", log_tool_call_mock),
        ):
            result = server.search_nearby_places(room_id="room-1", query="sushi")

        self.assertEqual(result["structuredContent"]["property_id"], "prop-room")
        payload = log_tool_call_mock.call_args.kwargs["request_payload"]
        self.assertEqual(payload["room_id"], "room-1")
        self.assertIsNone(payload["property_id_input"])
        self.assertEqual(payload["property_id_resolved"], "prop-room")

    def test_both_ids_prefers_room_resolution_and_logs_inputs(self):
        log_tool_call_mock = MagicMock()

        def fetch_one_side_effect(sql, params=None):
            if "FROM rooms" in sql:
                return {"property_id": "prop-room"}
            if "FROM properties" in sql:
                self.assertEqual(params, ["prop-room"])
                return {"id": "prop-room", "lat": None, "lng": None}
            raise AssertionError(f"Unexpected SQL: {sql}")

        with (
            patch.object(server, "fetch_one", side_effect=fetch_one_side_effect),
            patch.object(server, "fetch_all", return_value=[]),
            patch.object(server, "_search_google_places", return_value=[]),
            patch.object(server, "log_tool_call", log_tool_call_mock),
        ):
            result = server.search_nearby_places(
                property_id="prop-input",
                room_id="room-1",
                query="sushi",
            )

        self.assertEqual(result["structuredContent"]["property_id"], "prop-room")
        payload = log_tool_call_mock.call_args.kwargs["request_payload"]
        self.assertEqual(payload["room_id"], "room-1")
        self.assertEqual(payload["property_id_input"], "prop-input")
        self.assertEqual(payload["property_id_resolved"], "prop-room")

    def test_neither_id_raises_error(self):
        with self.assertRaisesRegex(
            ValueError,
            "Either property_id or room_id is required for search_nearby_places.",
        ):
            server.search_nearby_places(query="sushi")

    def test_invalid_room_id_raises_error(self):
        def fetch_one_side_effect(sql, params=None):
            if "FROM rooms" in sql:
                return None
            raise AssertionError(f"Unexpected SQL: {sql}")

        with patch.object(server, "fetch_one", side_effect=fetch_one_side_effect):
            with self.assertRaisesRegex(ValueError, "room_id not found"):
                server.search_nearby_places(room_id="missing-room", query="sushi")

    def test_property_not_found_raises_error(self):
        def fetch_one_side_effect(sql, params=None):
            if "FROM rooms" in sql:
                raise AssertionError("Room lookup should not happen for property_id-only input")
            if "FROM properties" in sql:
                return None
            raise AssertionError(f"Unexpected SQL: {sql}")

        with patch.object(server, "fetch_one", side_effect=fetch_one_side_effect):
            with self.assertRaisesRegex(ValueError, "property_id not found"):
                server.search_nearby_places(property_id="missing-prop", query="sushi")


if __name__ == "__main__":
    unittest.main()
