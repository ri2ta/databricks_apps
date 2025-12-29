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
