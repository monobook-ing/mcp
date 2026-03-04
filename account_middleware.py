from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from threading import Lock

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from account_context import AccountContext, set_account_context
from db import fetch_all, fetch_one


_CACHE_TTL_SECONDS = 60
_CACHE_LOCK = Lock()


@dataclass(frozen=True, slots=True)
class _AccountLookupResult:
    status_code: int | None
    error: str | None
    context: AccountContext | None


_ACCOUNT_CACHE: dict[str, tuple[float, _AccountLookupResult]] = {}


def _fetch_account_lookup(account_id: str) -> _AccountLookupResult:
    account = fetch_one("SELECT id, plan FROM accounts WHERE id = %s LIMIT 1", [account_id])
    if not account:
        return _AccountLookupResult(404, "Account not found", None)

    plan = str(account.get("plan") or "").lower()
    if plan != "pro":
        return _AccountLookupResult(403, "MCP gateway requires Pro plan", None)

    rows = fetch_all("SELECT id FROM properties WHERE account_id = %s", [account_id])
    property_ids = frozenset(str(row["id"]) for row in rows if row.get("id") is not None)
    return _AccountLookupResult(
        status_code=None,
        error=None,
        context=AccountContext(account_id=account_id, plan=plan, property_ids=property_ids),
    )


def _get_account_lookup(account_id: str) -> _AccountLookupResult:
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _ACCOUNT_CACHE.get(account_id)
        if cached and cached[0] > now:
            return cached[1]

    result = _fetch_account_lookup(account_id)

    with _CACHE_LOCK:
        _ACCOUNT_CACHE[account_id] = (now + _CACHE_TTL_SECONDS, result)
        if len(_ACCOUNT_CACHE) > 2048:
            expired_keys = [k for k, (expires_at, _) in _ACCOUNT_CACHE.items() if expires_at <= now]
            for key in expired_keys:
                _ACCOUNT_CACHE.pop(key, None)
    return result


class AccountGatewayMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path_params = scope.get("path_params") or {}
        raw_account_id = str(path_params.get("account_id") or "").strip()
        try:
            account_id = str(uuid.UUID(raw_account_id))
        except ValueError:
            await JSONResponse({"error": "Invalid account_id"}, status_code=400)(scope, receive, send)
            return

        lookup = _get_account_lookup(account_id)
        if lookup.status_code is not None:
            await JSONResponse({"error": lookup.error}, status_code=lookup.status_code)(scope, receive, send)
            return

        token = set_account_context(lookup.context)
        try:
            await self.app(scope, receive, send)
        finally:
            # Reset to preserve any parent context (important in nested async tasks).
            token.var.reset(token)
