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


class SearchKnowledgeTests(unittest.TestCase):
    def test_property_id_only(self):
        with (
            patch.object(server, "_embed_query", return_value=[0.1]),
            patch.object(server, "_search_chunks", return_value=[] ) as search_chunks_mock,
            patch.object(server, "_log_rag_query"),
            patch.object(server, "log_tool_call"),
            patch.object(server, "fetch_one") as fetch_one_mock,
        ):
            result = server.search_knowledge(
                question="What is wifi password?",
                property_id="prop-1",
                language="en",
            )

        fetch_one_mock.assert_not_called()
        search_chunks_mock.assert_called_once()
        self.assertEqual(search_chunks_mock.call_args.kwargs["property_id"], "prop-1")
        self.assertIn("I couldn't find that", result["content"][0]["text"])

    def test_room_id_only_resolves_property(self):
        with (
            patch.object(server, "fetch_one", return_value={"property_id": "prop-room"}),
            patch.object(server, "_embed_query", return_value=[0.1]),
            patch.object(server, "_search_chunks", return_value=[]) as search_chunks_mock,
            patch.object(server, "_log_rag_query"),
            patch.object(server, "log_tool_call"),
        ):
            server.search_knowledge(
                question="What is wifi password?",
                room_id="room-1",
                language="uk",
            )

        self.assertEqual(search_chunks_mock.call_args.kwargs["property_id"], "prop-room")

    def test_both_ids_prefers_room_resolution_and_logs_inputs(self):
        log_tool_call_mock = MagicMock()
        with (
            patch.object(server, "fetch_one", return_value={"property_id": "prop-room"}),
            patch.object(server, "_embed_query", return_value=[0.1]),
            patch.object(server, "_search_chunks", return_value=[]),
            patch.object(server, "_log_rag_query"),
            patch.object(server, "log_tool_call", log_tool_call_mock),
        ):
            server.search_knowledge(
                question="Question",
                property_id="prop-input",
                room_id="room-1",
            )

        payload = log_tool_call_mock.call_args.kwargs["request_payload"]
        self.assertEqual(payload["room_id"], "room-1")
        self.assertEqual(payload["property_id_input"], "prop-input")
        self.assertEqual(payload["property_id_resolved"], "prop-room")

    def test_neither_id_raises_error(self):
        with patch.object(server, "fetch_all", return_value=[]):
            with self.assertRaisesRegex(
                ValueError,
                "Either property_id or room_id is required for search_knowledge.",
            ):
                server.search_knowledge(question="Question")

    def test_invalid_room_id_raises_error(self):
        with patch.object(server, "fetch_one", return_value=None):
            with self.assertRaisesRegex(ValueError, "room_id not found"):
                server.search_knowledge(question="Question", room_id="missing-room")

    def test_no_chunks_still_returns_graceful_message(self):
        with (
            patch.object(server, "_embed_query", return_value=[0.1]),
            patch.object(server, "_search_chunks", return_value=[]),
            patch.object(server, "_log_rag_query"),
            patch.object(server, "log_tool_call"),
        ):
            result = server.search_knowledge(
                question="What is checkout time?",
                property_id="prop-1",
            )

        self.assertIn("I couldn't find that", result["content"][0]["text"])
        self.assertEqual(result["structuredContent"]["sources"], [])

    def test_error_path_logging_includes_room_id(self):
        log_tool_call_mock = MagicMock()
        with (
            patch.object(server, "fetch_one", return_value={"property_id": "prop-room"}),
            patch.object(server, "_embed_query", side_effect=RuntimeError("embedding failed")),
            patch.object(server, "_log_rag_query"),
            patch.object(server, "log_tool_call", log_tool_call_mock),
        ):
            with self.assertRaisesRegex(RuntimeError, "embedding failed"):
                server.search_knowledge(
                    question="What is wifi password?",
                    property_id="prop-input",
                    room_id="room-1",
                )

        payload = log_tool_call_mock.call_args.kwargs["request_payload"]
        self.assertEqual(log_tool_call_mock.call_args.kwargs["status"], "error")
        self.assertEqual(payload["room_id"], "room-1")
        self.assertEqual(payload["property_id_input"], "prop-input")
        self.assertEqual(payload["property_id_resolved"], "prop-room")


if __name__ == "__main__":
    unittest.main()
