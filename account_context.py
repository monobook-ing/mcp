from __future__ import annotations

from contextvars import ContextVar, Token
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AccountContext:
    account_id: str
    plan: str
    property_ids: frozenset[str]


_account_context: ContextVar[AccountContext | None] = ContextVar(
    "account_context",
    default=None,
)


def get_account_context() -> AccountContext | None:
    return _account_context.get()


def set_account_context(ctx: AccountContext | None) -> Token[AccountContext | None]:
    return _account_context.set(ctx)


def require_property_ownership(property_id: str) -> None:
    ctx = get_account_context()
    if ctx is None:
        return
    if str(property_id) not in ctx.property_ids:
        raise ValueError("Access denied: property does not belong to this account")


def get_account_property_ids() -> frozenset[str] | None:
    ctx = get_account_context()
    return None if ctx is None else ctx.property_ids
