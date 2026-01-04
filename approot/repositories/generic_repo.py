"""
YAML 定義エンティティ用の共通リポジトリ。
SQLAlchemy Core でリスト/詳細/検索/保存を行うヘルパー群をまとめる。
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


def _fetch_inserted_pk(cursor, pk_name: str):
    """最後に挿入された主キーを取得するため複数の手段を試す。"""
    new_id = getattr(cursor, "lastrowid", None)
    if new_id:
        return new_id

    fallback_queries = (
        "SELECT last_insert_rowid()",
        "SELECT last_identity()",
    )

    for stmt in fallback_queries:
        try:
            cursor.execute(stmt)
            row = cursor.fetchone()
            if row and row[0] is not None:
                return row[0]
        except Exception:
            logger.debug("fallback PK retrieval failed for statement=%s", stmt, exc_info=True)
    return None


def preload_tables(entities: Dict[str, Dict[str, Any]]):
    """起動時にテーブルをリフレクトしてキャッシュし、初回アクセスの遅延を抑える。"""
    for entity in entities.values():
        table_name = entity.get("table")
        if not table_name:
            continue
        cols = _select_columns(entity)
        try:
            _get_table(table_name, columns=cols)
        except Exception:
            logger.exception("preload_tables failed for table=%s", table_name)


def _get_table(table_name: str, columns: List[str] = None) -> Table:
    """
    テーブル名から SQLAlchemy Table を取得または反映する。
    エンジンが無い（テストなど）場合は渡されたカラムで最小構成の Table を組み立てる。
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
    """テーブル名やカラム名へのインジェクションを避けるため識別子を検証する。"""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"unsafe identifier: {name}")
    return name


def _select_columns(entity: Dict[str, Any]) -> List[str]:
    """主キーとリスト列を順序を保ちつつ重複なく集める。"""
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
    """フォーム定義から保存を許可するフィールド名を抽出する。"""
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


def _execute_compiled(cursor, compiled):
    """コンパイル済み SQLAlchemy 文を DBAPI カーソルで実行する。qmark / named の両スタイルに対応。"""
    params = compiled.params or {}
    if hasattr(compiled, "positiontup") and compiled.positiontup:
        # qmark-style: use positional tuple in the order SA expects
        ordered = [params[name] for name in compiled.positiontup]
        cursor.execute(str(compiled), ordered)
    else:
        # named params path
        cursor.execute(str(compiled), params)


def fetch_list(entity: Dict[str, Any], page: int = 1, page_size: int | None = None, sort: str | None = None) -> List[Dict[str, Any]]:
    """エンティティのページネートされた一覧を SQLAlchemy Core で取得する。"""
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
    
    # 接続を取得してクエリ実行
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        # SQLAlchemy 文を文字列化し DBAPI で実行
        # Databricks SQL Warehouse は LIMIT/OFFSET のバインドに弱いのでリテラル化して渡す
        compiled = stmt.compile(compile_kwargs={"literal_binds": True})
        _execute_compiled(cursor, compiled)
        rows = cursor.fetchall()
        return _rows_to_dicts(cursor.description, rows)
    finally:
        db.release_connection(conn)


def fetch_detail(entity: Dict[str, Any], pk: Any) -> Dict[str, Any] | None:
    """主キーで 1 件取得する。見つからなければ None。"""
    columns = _select_columns(entity)
    pk_name = entity.get('primary_key', 'id')
    
    # Get SQLAlchemy Table object (with column fallback for tests)
    table = _get_table(entity['table'], columns=columns)
    
    # Build select statement
    selected_cols = [table.c[_safe_identifier(c)] for c in columns]
    stmt = select(*selected_cols).where(table.c[_safe_identifier(pk_name)] == pk)
    
    # クエリを実行
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        compiled = stmt.compile(compile_kwargs={"literal_binds": True})
        _execute_compiled(cursor, compiled)
        row = cursor.fetchone()
        if not row:
            return None
        return _rows_to_dicts(cursor.description, [row])[0]
    finally:
        db.release_connection(conn)


def search_lookup(entity: Dict[str, Any], q: str, limit: int = 10) -> List[Dict[str, Any]]:
    """モーダル検索用のルックアップヘルパー。2 列目を表示列として検索する。"""
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
    
    # クエリを実行
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        compiled = stmt.compile(compile_kwargs={"literal_binds": False})
        _execute_compiled(cursor, compiled)
        rows = cursor.fetchall()
        return _rows_to_dicts(cursor.description, rows)
    finally:
        db.release_connection(conn)


def save(entity: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    レコードを INSERT/UPDATE する共通保存関数。
    payload に主キーがあれば UPDATE、無ければ INSERT。更新対象が無い場合は ValueError を投げる。
    """
    pk_name = entity.get("primary_key", "id")
    allowed_fields = _allowed_fields(entity)
    filtered_payload = {k: v for k, v in payload.items() if k in allowed_fields}
    
    # Get SQLAlchemy Table object (with column fallback for tests)
    table = _get_table(entity["table"], columns=allowed_fields)
    
    # INSERT / UPDATE 判定
    is_update = pk_name in filtered_payload and filtered_payload.get(pk_name) not in (None, "", 0)
    
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        if is_update:
            pk_value = filtered_payload[pk_name]

            fields_to_update = {k: v for k, v in filtered_payload.items() if k != pk_name}
            if not fields_to_update:
                return fetch_detail(entity, pk_value)

            stmt = update(table).where(
                table.c[_safe_identifier(pk_name)] == pk_value
            ).values(**fields_to_update)

            compiled = stmt.compile(compile_kwargs={"literal_binds": True})
            _execute_compiled(cursor, compiled)

            if cursor.rowcount == 0:
                raise ValueError(f"Record with {pk_name}={pk_value} not found")

            conn.commit()
            return fetch_detail(entity, pk_value)

        # INSERT パス
        fields = {k: v for k, v in filtered_payload.items()
                  if k != pk_name or v not in (None, "", 0)}

        if not fields:
            raise ValueError("No fields to insert")

        stmt = insert(table).values(**fields)
        compiled = stmt.compile(compile_kwargs={"literal_binds": True})
        _execute_compiled(cursor, compiled)

        pk_value = _fetch_inserted_pk(cursor, pk_name)
        if pk_value is None:
            conn.rollback()
            raise ValueError(
                "Insert succeeded but primary key could not be retrieved; "
                "provide a primary key value or configure identity retrieval"
            )

        filtered_payload[pk_name] = pk_value
        conn.commit()
        return filtered_payload

    except Exception:
        try:
            conn.rollback()
        except Exception:
            logger.warning("rollback failed after save error", exc_info=True)
        raise
    finally:
        db.release_connection(conn)
