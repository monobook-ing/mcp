import os
import logging
from contextlib import contextmanager

import psycopg2
import psycopg2.pool
import psycopg2.extras

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DATABASE_URL = os.environ["DATABASE_URL"]

_pool = psycopg2.pool.SimpleConnectionPool(1, 5, DATABASE_URL)


@contextmanager
def get_conn():
    """Yield a connection from the pool, auto-commit on success, rollback on error."""
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def fetch_all(sql, params=None):
    """Execute a SELECT and return all rows as a list of dicts."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            logger.debug("SQL: %s | params: %s", sql, params)
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def fetch_one(sql, params=None):
    """Execute a SELECT and return a single row as a dict, or None."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            logger.debug("SQL: %s | params: %s", sql, params)
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None


def execute(sql, params=None):
    """Execute an INSERT/UPDATE/DELETE with no return value."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            logger.debug("SQL: %s | params: %s", sql, params)
            cur.execute(sql, params)


def execute_returning(sql, params=None):
    """Execute an INSERT ... RETURNING and return the first row as a dict."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            logger.debug("SQL: %s | params: %s", sql, params)
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None
