"""
Unit tests for generic_repo.py (Task 2)
"""
import pytest
import sys
import types


class DummyCursor:
    def __init__(self, rows=None, description=None, fetchone_result=None):
        self.rows = rows or []
        self.description = description or []
        self.fetchone_result = fetchone_result
        self.executed = []

    def execute(self, query, params=()):
        self.executed.append((query, params))

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.fetchone_result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyConnection:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def stub_requests(monkeypatch):
    """Stub requests module so approot.db import succeeds without network deps."""
    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"access_token": "dummy"}

    def fake_post(*args, **kwargs):
        return _Resp()

    requests_stub = types.SimpleNamespace(post=fake_post)
    monkeypatch.setitem(sys.modules, "requests", requests_stub)

    # Stub databricks.sql module required by approot.db
    sql_stub = types.SimpleNamespace(connect=lambda **kwargs: None)
    databricks_stub = types.SimpleNamespace(sql=sql_stub)
    monkeypatch.setitem(sys.modules, "databricks", databricks_stub)
    monkeypatch.setitem(sys.modules, "databricks.sql", sql_stub)

    # Required env vars for approot.db import
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "dummy-host")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/dummy")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "dummy-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "dummy-secret")

@pytest.fixture
def entity_cfg():
    return {
        "name": "customer",
        "table": "customers",
        "label": "Customer",
        "primary_key": "id",
        "list": {
            "columns": [
                {"name": "name", "label": "Name"},
                {"name": "email", "label": "Email"},
            ],
            "default_sort": "name",
            "page_size": 20,
        },
    }


def test_fetch_list_applies_sort_and_pagination(monkeypatch, entity_cfg):
    from approot.repositories import generic_repo

    rows = [(1, "Alice", "a@example.com"), (2, "Bob", "b@example.com")]
    description = [("id",), ("name",), ("email",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    released = {}

    def fake_release(c):
        released["called"] = True
        assert c is conn
    monkeypatch.setattr(generic_repo.db, "release_connection", fake_release)

    result = generic_repo.fetch_list(entity_cfg, page=2, page_size=5, sort="-name")

    assert len(cursor.executed) == 1
    query, params = cursor.executed[0]
    assert "ORDER BY name DESC" in query
    assert "LIMIT ?" in query and "OFFSET ?" in query
    assert params == (5, 5)
    assert released.get("called") is True

    assert result == [
        {"id": 1, "name": "Alice", "email": "a@example.com"},
        {"id": 2, "name": "Bob", "email": "b@example.com"},
    ]


def test_fetch_list_invalid_sort_falls_back_to_default(monkeypatch, entity_cfg):
    from approot.repositories import generic_repo

    rows = [(1, "Alice"), (2, "Bob")]
    description = [("id",), ("name",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.fetch_list(entity_cfg, sort="bad-column")

    query, params = cursor.executed[0]
    assert "ORDER BY name ASC" in query  # default_sort
    assert params == (20, 0)  # default page_size=20 page=1
    assert result[0]["name"] == "Alice"


def test_fetch_detail_binds_primary_key(monkeypatch, entity_cfg):
    from approot.repositories import generic_repo

    row = (2, "Bob", "b@example.com")
    description = [("id",), ("name",), ("email",)]
    cursor = DummyCursor(description=description, fetchone_result=row)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.fetch_detail(entity_cfg, pk=2)

    query, params = cursor.executed[0]
    assert "WHERE id = ?" in query
    assert params == (2,)
    assert result == {"id": 2, "name": "Bob", "email": "b@example.com"}


def test_search_lookup_uses_like(monkeypatch, entity_cfg):
    from approot.repositories import generic_repo

    rows = [(1, "Alice"), (2, "Alan")]
    description = [("id",), ("name",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.search_lookup(entity_cfg, q="Al", limit=3)

    query, params = cursor.executed[0]
    assert "LIKE ?" in query
    assert params == ("%Al%", 3)
    assert [r["name"] for r in result] == ["Alice", "Alan"]


# Task 6: Edge cases and error paths


def test_fetch_list_page_zero(monkeypatch, entity_cfg):
    """Task 6: Test fetch_list with page=0 (should be treated as page 1)"""
    from approot.repositories import generic_repo

    rows = [(1, "Alice")]
    description = [("id",), ("name",), ("email",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.fetch_list(entity_cfg, page=0)

    query, params = cursor.executed[0]
    # page=0 should be treated as page=1, so offset should be 0
    assert params == (20, 0)


def test_fetch_list_negative_page(monkeypatch, entity_cfg):
    """Task 6: Test fetch_list with negative page number"""
    from approot.repositories import generic_repo

    rows = [(1, "Alice")]
    description = [("id",), ("name",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.fetch_list(entity_cfg, page=-5)

    query, params = cursor.executed[0]
    # Negative page should be treated as page=1
    assert params == (20, 0)


def test_fetch_list_large_page_size(monkeypatch, entity_cfg):
    """Task 6: Test fetch_list with very large page_size"""
    from approot.repositories import generic_repo

    rows = []
    description = [("id",), ("name",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.fetch_list(entity_cfg, page=1, page_size=10000)

    query, params = cursor.executed[0]
    assert params == (10000, 0)
    assert result == []


def test_fetch_list_empty_result_set(monkeypatch, entity_cfg):
    """Task 6: Test fetch_list with empty result set"""
    from approot.repositories import generic_repo

    rows = []
    description = [("id",), ("name",), ("email",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.fetch_list(entity_cfg)

    assert result == []
    assert isinstance(result, list)


def test_fetch_detail_returns_none_for_missing_record(monkeypatch, entity_cfg):
    """Task 6: Test fetch_detail when record doesn't exist"""
    from approot.repositories import generic_repo

    description = [("id",), ("name",), ("email",)]
    cursor = DummyCursor(description=description, fetchone_result=None)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.fetch_detail(entity_cfg, pk=999)

    assert result is None


def test_search_lookup_empty_query(monkeypatch, entity_cfg):
    """Task 6: Test search_lookup with empty query string"""
    from approot.repositories import generic_repo

    rows = [(1, "Alice"), (2, "Bob")]
    description = [("id",), ("name",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.search_lookup(entity_cfg, q="", limit=10)

    query, params = cursor.executed[0]
    # Empty query should still work with LIKE pattern
    assert params == ("%%", 10)
    assert len(result) == 2


def test_search_lookup_no_results(monkeypatch, entity_cfg):
    """Task 6: Test search_lookup with no matching results"""
    from approot.repositories import generic_repo

    rows = []
    description = [("id",), ("name",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.search_lookup(entity_cfg, q="nonexistent", limit=10)

    assert result == []


def test_search_lookup_limit_zero(monkeypatch, entity_cfg):
    """Task 6: Test search_lookup with limit=0"""
    from approot.repositories import generic_repo

    rows = []
    description = [("id",), ("name",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.search_lookup(entity_cfg, q="test", limit=0)

    query, params = cursor.executed[0]
    assert params == ("%test%", 0)
    assert result == []


def test_safe_identifier_rejects_sql_injection(entity_cfg):
    """Task 6: Test that unsafe identifiers raise ValueError"""
    from approot.repositories import generic_repo
    import pytest

    # Test various SQL injection attempts
    with pytest.raises(ValueError, match="unsafe identifier"):
        generic_repo._safe_identifier("table; DROP TABLE users--")

    with pytest.raises(ValueError, match="unsafe identifier"):
        generic_repo._safe_identifier("col' OR '1'='1")

    with pytest.raises(ValueError, match="unsafe identifier"):
        generic_repo._safe_identifier("col-name")  # hyphen not allowed

    with pytest.raises(ValueError, match="unsafe identifier"):
        generic_repo._safe_identifier("123invalid")  # can't start with number

    # Valid identifiers should pass
    assert generic_repo._safe_identifier("valid_name") == "valid_name"
    assert generic_repo._safe_identifier("_underscore") == "_underscore"
    assert generic_repo._safe_identifier("CamelCase123") == "CamelCase123"


def test_fetch_list_with_single_column(monkeypatch):
    """Task 6: Test fetch_list with entity that has only primary key column"""
    from approot.repositories import generic_repo

    minimal_entity = {
        "name": "minimal",
        "table": "minimal_table",
        "primary_key": "id",
        "list": {
            "columns": [],  # No additional columns
        }
    }

    rows = [(1,), (2,)]
    description = [("id",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.fetch_list(minimal_entity)

    assert len(result) == 2
    assert result[0] == {"id": 1}
    assert result[1] == {"id": 2}


def test_search_lookup_with_single_column_entity(monkeypatch):
    """Task 6: Test search_lookup with entity that has only one column"""
    from approot.repositories import generic_repo

    minimal_entity = {
        "name": "status",
        "table": "statuses",
        "primary_key": "id",
        "list": {
            "columns": [{"name": "id"}],  # Only one column
        }
    }

    rows = [("active",), ("inactive",)]
    description = [("id",)]
    cursor = DummyCursor(rows=rows, description=description)
    conn = DummyConnection(cursor)

    monkeypatch.setattr(generic_repo.db, "get_connection", lambda timeout=None: conn)
    monkeypatch.setattr(generic_repo.db, "release_connection", lambda c: None)

    result = generic_repo.search_lookup(minimal_entity, q="act", limit=5)

    query, params = cursor.executed[0]
    # When only one column exists, use it for both pk and display
    assert "LIKE ?" in query
    assert params == ("%act%", 5)
