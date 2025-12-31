"""
Generic repository for YAML-defined entities.
Provides list/detail/lookup helpers using SQLAlchemy Core/ORM.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, Iterable, List, Tuple

from sqlalchemy import Table, MetaData, select, insert, update, text, func
from sqlalchemy.exc import SQLAlchemyError

from .. import db

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Cache for reflected table metadata
_table_cache: Dict[str, Table] = {}


def _get_table(table_name: str, columns: List[str] = None) -> Table:
    """
    Get or reflect SQLAlchemy Table object for the given table name.
    If engine is not available (e.g., in tests with mocked connections),
    creates a minimal Table with provided columns.
    """
    cache_key = f"{table_name}:{','.join(sorted(columns)) if columns else ''}"
    
    if cache_key not in _table_cache:
        _safe_identifier(table_name)  # Validate table name
        metadata = MetaData()
        engine = db.get_engine()
        
        if engine is not None:
            # Production path: reflect the table from the database
            table = Table(table_name, metadata, autoload_with=engine)
            _table_cache[cache_key] = table
        elif columns:
            # Test/fallback path: create minimal table with given columns
            # This is used when engine is not available but we have column info
            from sqlalchemy import Column, String, Integer
            
            # Create columns - use Integer for 'id' and similar, String for others
            cols = []
            for col in columns:
                if col in ('id', 'age') or col.endswith('_id'):
                    cols.append(Column(col, Integer))
                else:
                    cols.append(Column(col, String))
            
            table = Table(table_name, metadata, *cols)
            _table_cache[cache_key] = table
        else:
            # Cannot create table without engine or columns
            raise RuntimeError("Database engine not initialized. Call db.init_pool() first.")
    
    return _table_cache[cache_key]


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


def _allowed_fields(entity: Dict[str, Any]) -> List[str]:
    """Return allowed field names for persistence based on entity form sections."""
    allowed = set()
    pk = entity.get("primary_key", "id")
    if pk:
        allowed.add(pk)

    form_cfg = entity.get("form", {})
    for section in form_cfg.get("sections", []):
        for field in section.get("fields", []):
            name = field.get("name")
            if name:
                allowed.add(name)

    # Fallback: include list columns to avoid failing if forms are absent
    for col in entity.get("list", {}).get("columns", []):
        name = col.get("name")
        if name:
            allowed.add(name)

    return list(allowed)


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
    """Fetch paginated list for an entity using SQLAlchemy Core with parameter binding."""
    page = max(1, page or 1)
    cfg = entity.get("list", {})
    effective_page_size = page_size or cfg.get("page_size", 20)

    columns = _select_columns(entity)
    sort_col, sort_dir = _resolve_sort(sort, columns, cfg.get("default_sort"))

    # Get SQLAlchemy Table object (with column fallback for tests)
    table = _get_table(entity['table'], columns=columns)
    
    # Build select statement with only allowed columns
    selected_cols = [table.c[_safe_identifier(c)] for c in columns]
    stmt = select(*selected_cols)
    
    # Add sorting
    if sort_col:
        col = table.c[_safe_identifier(sort_col)]
        stmt = stmt.order_by(col.desc() if sort_dir == "DESC" else col.asc())
    
    # Add pagination
    offset = (page - 1) * effective_page_size
    stmt = stmt.limit(effective_page_size).offset(offset)
    
    # Execute query using connection
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        # Convert SQLAlchemy statement to string and execute with DBAPI
        compiled = stmt.compile(compile_kwargs={"literal_binds": False})
        cursor.execute(str(compiled), compiled.params)
        rows = cursor.fetchall()
        return _rows_to_dicts(cursor.description, rows)
    finally:
        db.release_connection(conn)


def fetch_detail(entity: Dict[str, Any], pk: Any) -> Dict[str, Any] | None:
    """Fetch a single record by primary key using SQLAlchemy Core."""
    columns = _select_columns(entity)
    pk_name = entity.get('primary_key', 'id')
    
    # Get SQLAlchemy Table object (with column fallback for tests)
    table = _get_table(entity['table'], columns=columns)
    
    # Build select statement
    selected_cols = [table.c[_safe_identifier(c)] for c in columns]
    stmt = select(*selected_cols).where(table.c[_safe_identifier(pk_name)] == pk)
    
    # Execute query
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        compiled = stmt.compile(compile_kwargs={"literal_binds": False})
        cursor.execute(str(compiled), compiled.params)
        row = cursor.fetchone()
        if not row:
            return None
        return _rows_to_dicts(cursor.description, [row])[0]
    finally:
        db.release_connection(conn)


def search_lookup(entity: Dict[str, Any], q: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Lookup helper for modal search using SQLAlchemy Core; uses first list column as display field."""
    columns = _select_columns(entity)
    if len(columns) < 2:
        display_col = columns[0]
    else:
        display_col = columns[1]
    pk_col = columns[0]

    # Get SQLAlchemy Table object (with column fallback for tests)
    table = _get_table(entity['table'], columns=columns)
    
    # Build select statement with LIKE search
    selected_cols = [table.c[_safe_identifier(pk_col)], table.c[_safe_identifier(display_col)]]
    # Use LIKE with parameter binding
    stmt = select(*selected_cols).where(
        table.c[_safe_identifier(display_col)].like(f"%{q}%")
    ).order_by(table.c[_safe_identifier(display_col)].asc()).limit(limit)
    
    # Execute query
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        compiled = stmt.compile(compile_kwargs={"literal_binds": False})
        cursor.execute(str(compiled), compiled.params)
        rows = cursor.fetchall()
        return _rows_to_dicts(cursor.description, rows)
    finally:
        db.release_connection(conn)


def save(entity: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Save a record (insert or update) using SQLAlchemy Core with parameter binding.
    If primary key is present and non-empty in payload, performs UPDATE.
    Otherwise, performs INSERT and returns record with new primary key.
    Raises ValueError if update target doesn't exist.
    """
    pk_name = entity.get("primary_key", "id")
    allowed_fields = _allowed_fields(entity)
    filtered_payload = {k: v for k, v in payload.items() if k in allowed_fields}
    
    # Get SQLAlchemy Table object (with column fallback for tests)
    table = _get_table(entity["table"], columns=allowed_fields)
    
    # Determine if this is insert or update
    is_update = pk_name in filtered_payload and filtered_payload.get(pk_name) not in (None, "", 0)
    
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        if is_update:
            # UPDATE existing record
            pk_value = filtered_payload[pk_name]
            
            # Build SET clause for all fields except primary key
            fields_to_update = {k: v for k, v in filtered_payload.items() if k != pk_name}
            if not fields_to_update:
                # No fields to update, just return existing record
                return fetch_detail(entity, pk_value)
            
            # Build UPDATE statement
            stmt = update(table).where(
                table.c[_safe_identifier(pk_name)] == pk_value
            ).values(**fields_to_update)
            
            # Execute update
            compiled = stmt.compile(compile_kwargs={"literal_binds": False})
            cursor.execute(str(compiled), compiled.params)
            
            # Verify the record was updated
            if cursor.rowcount == 0:
                raise ValueError(f"Record with {pk_name}={pk_value} not found")
            
            # Commit the transaction
            conn.commit()
            
            # Return the updated record
            return fetch_detail(entity, pk_value)
        else:
            # INSERT new record
            fields = {k: v for k, v in filtered_payload.items() 
                     if k != pk_name or v not in (None, "", 0)}
            
            if not fields:
                raise ValueError("No fields to insert")
            
            # Build INSERT statement
            stmt = insert(table).values(**fields)
            
            # Execute insert
            compiled = stmt.compile(compile_kwargs={"literal_binds": False})
            cursor.execute(str(compiled), compiled.params)
            
            # Get the last inserted ID
            new_id = cursor.lastrowid
            
            # Commit the transaction
            conn.commit()
            
            if new_id:
                filtered_payload[pk_name] = new_id
                return filtered_payload
            else:
                # If we can't get the ID, return payload
                return filtered_payload
    finally:
        db.release_connection(conn)
