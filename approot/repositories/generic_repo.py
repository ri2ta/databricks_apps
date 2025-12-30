"""
Generic repository for YAML-defined entities.
Provides list/detail/lookup helpers using db.py connection pool.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, Iterable, List, Tuple

from .. import db

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_identifier(name: str) -> str:
    """Validate identifier to avoid injection via table/column names."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"unsafe identifier: {name}")
    return name


def _select_columns(entity: Dict[str, Any]) -> List[str]:
    """Gather primary key + list columns, preserving order and uniqueness."""
    cols: List[str] = []
    pk = entity.get("primary_key", "id")
    if pk:
        cols.append(pk)
    for col in entity.get("list", {}).get("columns", []):
        name = col.get("name")
        if name and name not in cols:
            cols.append(name)
    return cols or [pk or "id"]


def _resolve_sort(sort: str | None, columns: Iterable[str], default_sort: str | None) -> Tuple[str | None, str]:
    col = sort or default_sort
    direction = "ASC"
    if col:
        if col.startswith("-"):
            direction = "DESC"
            col = col[1:]
        if col not in columns:
            col = default_sort if default_sort in columns else (next(iter(columns), None))
            direction = "ASC"
    return col, direction


def _rows_to_dicts(description, rows) -> List[Dict[str, Any]]:
    cols = [c[0] for c in description]
    return [dict(zip(cols, row)) for row in rows]


def fetch_list(entity: Dict[str, Any], page: int = 1, page_size: int | None = None, sort: str | None = None) -> List[Dict[str, Any]]:
    """Fetch paginated list for an entity using safe parameter binding."""
    page = max(1, page or 1)
    cfg = entity.get("list", {})
    effective_page_size = page_size or cfg.get("page_size", 20)

    columns = _select_columns(entity)
    sort_col, sort_dir = _resolve_sort(sort, columns, cfg.get("default_sort"))

    select_clause = ", ".join(_safe_identifier(c) for c in columns)
    sort_clause = f" ORDER BY {_safe_identifier(sort_col)} {sort_dir}" if sort_col else ""
    offset = (page - 1) * effective_page_size
    query = f"SELECT {select_clause} FROM {_safe_identifier(entity['table'])}{sort_clause} LIMIT ? OFFSET ?"

    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (effective_page_size, offset))
            rows = cur.fetchall()
            return _rows_to_dicts(cur.description, rows)
    finally:
        db.release_connection(conn)


def fetch_detail(entity: Dict[str, Any], pk: Any) -> Dict[str, Any] | None:
    """Fetch a single record by primary key."""
    columns = _select_columns(entity)
    select_clause = ", ".join(_safe_identifier(c) for c in columns)
    query = f"SELECT {select_clause} FROM {_safe_identifier(entity['table'])} WHERE {_safe_identifier(entity.get('primary_key', 'id'))} = ?"

    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (pk,))
            row = cur.fetchone()
            if not row:
                return None
            return _rows_to_dicts(cur.description, [row])[0]
    finally:
        db.release_connection(conn)


def search_lookup(entity: Dict[str, Any], q: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Lookup helper for modal search; uses first list column as display field."""
    columns = _select_columns(entity)
    if len(columns) < 2:
        display_col = columns[0]
    else:
        display_col = columns[1]
    pk_col = columns[0]

    select_clause = f"{_safe_identifier(pk_col)}, {_safe_identifier(display_col)}"
    query = (
        f"SELECT {select_clause} FROM {_safe_identifier(entity['table'])} "
        f"WHERE {_safe_identifier(display_col)} LIKE ? ORDER BY {_safe_identifier(display_col)} ASC LIMIT ?"
    )

    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (f"%{q}%", limit))
            rows = cur.fetchall()
            return _rows_to_dicts(cur.description, rows)
    finally:
        db.release_connection(conn)
