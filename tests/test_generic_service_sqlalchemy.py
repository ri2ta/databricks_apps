"""
Task 13: SQLAlchemy regression tests for generic_service.py
Tests verify that generic_service works correctly with real SQLAlchemy backend.
Uses file-based SQLite DB to persist across connections.
"""
import pytest
import tempfile
import os
from pathlib import Path
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, text


@pytest.fixture
def temp_db_file():
    """Create temporary file-based SQLite database."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    yield path
    # Cleanup
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture
def sqlalchemy_db_url(temp_db_file):
    """SQLAlchemy database URL for file-based SQLite."""
    return f"sqlite:///{temp_db_file}"


@pytest.fixture
def setup_sqlalchemy_db(monkeypatch, sqlalchemy_db_url, temp_db_file):
    """Initialize db module with file-based SQLite and create test schema."""
    # Set environment variables for db module
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", sqlalchemy_db_url)
    monkeypatch.setenv("DB_POOL_SIZE", "5")
    
    # Import db module and reset state
    from approot import db
    db.close_pool()
    
    # Initialize pool
    db.init_pool()
    
    # Create test tables and seed data
    engine = db.get_engine()
    metadata = MetaData()
    
    # Create customers table
    customers = Table(
        'customers',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('name', String(100)),
        Column('email', String(100)),
        Column('age', Integer),
        Column('status', String(50)),
    )
    
    # Create statuses lookup table
    statuses = Table(
        'statuses',
        metadata,
        Column('id', String(50), primary_key=True),
        Column('name', String(100)),
    )
    
    metadata.create_all(engine)
    
    # Seed data
    with engine.connect() as conn:
        conn.execute(customers.insert(), [
            {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'age': 30, 'status': 'active'},
            {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'age': 25, 'status': 'active'},
            {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35, 'status': 'inactive'},
        ])
        conn.execute(statuses.insert(), [
            {'id': 'active', 'name': 'Active'},
            {'id': 'inactive', 'name': 'Inactive'},
            {'id': 'pending', 'name': 'Pending'},
        ])
        conn.commit()
    
    yield db
    
    # Teardown
    db.close_pool()


@pytest.fixture
def entities():
    """Entity configurations for testing."""
    return {
        "customer": {
            "name": "customer",
            "table": "customers",
            "label": "Customer",
            "primary_key": "id",
            "list": {
                "columns": [
                    {"name": "id", "label": "ID"},
                    {"name": "name", "label": "Name"},
                    {"name": "email", "label": "Email"},
                ],
                "default_sort": "name",
                "page_size": 20,
                "actions": [
                    {"name": "export_csv", "label": "Export CSV"},
                ],
            },
            "form": {
                "sections": [
                    {
                        "label": "Basic Info",
                        "fields": [
                            {"name": "name", "label": "Name", "type": "text", "required": True},
                            {"name": "email", "label": "Email", "type": "email", "required": True},
                            {"name": "age", "label": "Age", "type": "number"},
                            {"name": "status", "label": "Status", "type": "text"},
                        ],
                    }
                ],
                "actions": [
                    {"name": "calc_points", "label": "Calc Points"},
                ],
            },
        },
        "status": {
            "name": "status",
            "table": "statuses",
            "label": "Status",
            "primary_key": "id",
            "list": {
                "columns": [
                    {"name": "id", "label": "ID"},
                    {"name": "name", "label": "Name"},
                ],
                "default_sort": "name",
                "page_size": 20,
            },
            "form": {
                "sections": [
                    {
                        "label": "Info",
                        "fields": [
                            {"name": "id", "label": "ID", "type": "text"},
                            {"name": "name", "label": "Name", "type": "text"},
                        ],
                    }
                ],
            },
        },
    }


# Test render_list with SQLAlchemy backend
def test_render_list_with_sqlalchemy_returns_records(setup_sqlalchemy_db, entities):
    """Task 13: render_list returns records from SQLAlchemy DB."""
    from approot.services import generic_service
    
    ctx = generic_service.render_list(entities, "customer", page=1, page_size=10)
    
    assert ctx["ok"] is True
    assert ctx["status"] == 200
    assert ctx["mode"] == "list"
    assert len(ctx["rows"]) == 3
    assert ctx["rows"][0]["name"] == "Alice"  # Sorted by name


def test_render_list_with_pagination_sqlalchemy(setup_sqlalchemy_db, entities):
    """Task 13: render_list pagination works with SQLAlchemy."""
    from approot.services import generic_service
    
    # Page 1
    ctx1 = generic_service.render_list(entities, "customer", page=1, page_size=2)
    assert ctx1["ok"] is True
    assert len(ctx1["rows"]) == 2
    
    # Page 2
    ctx2 = generic_service.render_list(entities, "customer", page=2, page_size=2)
    assert ctx2["ok"] is True
    assert len(ctx2["rows"]) == 1


def test_render_list_with_sorting_sqlalchemy(setup_sqlalchemy_db, entities):
    """Task 13: render_list sorting works with SQLAlchemy."""
    from approot.services import generic_service
    
    # Ascending
    ctx_asc = generic_service.render_list(entities, "customer", sort="name")
    assert ctx_asc["ok"] is True
    assert ctx_asc["rows"][0]["name"] == "Alice"
    
    # Descending
    ctx_desc = generic_service.render_list(entities, "customer", sort="-name")
    assert ctx_desc["ok"] is True
    assert ctx_desc["rows"][0]["name"] == "Charlie"


def test_render_list_unknown_entity_returns_404(setup_sqlalchemy_db, entities):
    """Task 13: render_list returns 404 for unknown entity."""
    from approot.services import generic_service
    
    ctx = generic_service.render_list(entities, "unknown", page=1)
    
    assert ctx["ok"] is False
    assert ctx["status"] == 404
    assert "unknown entity" in ctx["error"].lower()


# Test render_detail with SQLAlchemy backend
def test_render_detail_with_sqlalchemy_returns_record(setup_sqlalchemy_db, entities):
    """Task 13: render_detail returns single record from SQLAlchemy DB."""
    from approot.services import generic_service
    
    ctx = generic_service.render_detail(entities, "customer", pk=2)
    
    assert ctx["ok"] is True
    assert ctx["status"] == 200
    assert ctx["mode"] == "view"
    assert ctx["record"]["id"] == 2
    assert ctx["record"]["name"] == "Bob"
    assert ctx["record"]["email"] == "bob@example.com"


def test_render_detail_not_found_sqlalchemy(setup_sqlalchemy_db, entities):
    """Task 13: render_detail returns 404 for nonexistent record."""
    from approot.services import generic_service
    
    ctx = generic_service.render_detail(entities, "customer", pk=999)
    
    assert ctx["ok"] is False
    assert ctx["status"] == 404
    assert "not found" in ctx["error"].lower()


# Test render_form with SQLAlchemy backend
def test_render_form_create_mode_sqlalchemy(setup_sqlalchemy_db, entities):
    """Task 13: render_form in create mode works with SQLAlchemy."""
    from approot.services import generic_service
    
    ctx = generic_service.render_form(entities, "customer", pk=None)
    
    assert ctx["ok"] is True
    assert ctx["status"] == 200
    assert ctx["mode"] == "create"
    assert ctx["record"] is None


def test_render_form_edit_mode_sqlalchemy(setup_sqlalchemy_db, entities):
    """Task 13: render_form in edit mode fetches record from SQLAlchemy DB."""
    from approot.services import generic_service
    
    ctx = generic_service.render_form(entities, "customer", pk=1)
    
    assert ctx["ok"] is True
    assert ctx["status"] == 200
    assert ctx["mode"] == "edit"
    assert ctx["record"]["id"] == 1
    assert ctx["record"]["name"] == "Alice"


def test_render_form_edit_not_found_sqlalchemy(setup_sqlalchemy_db, entities):
    """Task 13: render_form returns 404 for nonexistent record in edit mode."""
    from approot.services import generic_service
    
    ctx = generic_service.render_form(entities, "customer", pk=999)
    
    assert ctx["ok"] is False
    assert ctx["status"] == 404


# Test handle_save with SQLAlchemy backend
def test_handle_save_insert_with_sqlalchemy(setup_sqlalchemy_db, entities):
    """Task 13: handle_save inserts new record into SQLAlchemy DB."""
    from approot.services import generic_service
    
    payload = {
        "name": "David",
        "email": "david@example.com",
        "age": 28,
        "status": "active",
    }
    
    ctx = generic_service.handle_save(entities, "customer", payload)
    
    assert ctx["ok"] is True
    assert ctx["status"] == 200
    assert ctx["mode"] == "view"
    assert ctx["record"]["name"] == "David"
    assert "id" in ctx["record"]


def test_handle_save_update_with_sqlalchemy(setup_sqlalchemy_db, entities):
    """Task 13: handle_save updates existing record in SQLAlchemy DB."""
    from approot.services import generic_service
    
    payload = {
        "id": 1,
        "name": "Alice Updated",
        "email": "alice.updated@example.com",
        "age": 31,
        "status": "inactive",
    }
    
    ctx = generic_service.handle_save(entities, "customer", payload)
    
    assert ctx["ok"] is True
    assert ctx["status"] == 200
    assert ctx["record"]["name"] == "Alice Updated"
    
    # Verify update persisted
    ctx_detail = generic_service.render_detail(entities, "customer", pk=1)
    assert ctx_detail["record"]["name"] == "Alice Updated"


def test_handle_save_validation_error_returns_400(setup_sqlalchemy_db, entities):
    """Task 13: handle_save returns 400 with errors for validation failures."""
    from approot.services import generic_service
    
    payload = {
        "name": "",  # Required field empty
        "email": "invalid",  # Invalid email
    }
    
    ctx = generic_service.handle_save(entities, "customer", payload)
    
    assert ctx["ok"] is False
    assert ctx["status"] == 400
    assert "errors" in ctx
    assert "name" in ctx["errors"]  # Required field error


def test_handle_save_nonexistent_record_returns_404(setup_sqlalchemy_db, entities):
    """Task 13: handle_save returns 404 when updating nonexistent record."""
    from approot.services import generic_service
    
    payload = {
        "id": 999,
        "name": "Nonexistent",
        "email": "none@example.com",
    }
    
    ctx = generic_service.handle_save(entities, "customer", payload)
    
    assert ctx["ok"] is False
    assert ctx["status"] == 404


# Test handle_action with SQLAlchemy backend
def test_handle_action_with_sqlalchemy_entity(setup_sqlalchemy_db, entities):
    """Task 13: handle_action works with real entity from SQLAlchemy setup."""
    from approot.services import generic_service
    
    def test_handler(entity, payload):
        return {"message": "Success", "data": payload}
    
    ctx = generic_service.handle_action(
        entities,
        "customer",
        "calc_points",
        payload={"id": 1},
        handlers={"calc_points": test_handler},
    )
    
    assert ctx["ok"] is True
    assert ctx["status"] == 200
    assert ctx["result"]["message"] == "Success"


def test_handle_action_unknown_action_returns_404(setup_sqlalchemy_db, entities):
    """Task 13: handle_action returns 404 for unknown action."""
    from approot.services import generic_service
    
    ctx = generic_service.handle_action(
        entities,
        "customer",
        "unknown_action",
        payload={},
    )
    
    assert ctx["ok"] is False
    assert ctx["status"] == 404


def test_handle_action_missing_handler_returns_501(setup_sqlalchemy_db, entities):
    """Task 13: handle_action returns 501 when handler not registered."""
    from approot.services import generic_service
    
    ctx = generic_service.handle_action(
        entities,
        "customer",
        "calc_points",
        payload={},
        handlers={},
    )
    
    assert ctx["ok"] is False
    assert ctx["status"] == 501


# Test lookup with SQLAlchemy backend
def test_lookup_search_with_sqlalchemy(setup_sqlalchemy_db, entities):
    """Task 13: search_lookup returns rows from SQLAlchemy DB."""
    from approot.repositories import generic_repo
    
    results = generic_repo.search_lookup(entities["status"], q="Pend", limit=10)
    
    assert len(results) == 1
    assert results[0]["name"] == "Pending"


def test_lookup_search_multiple_results(setup_sqlalchemy_db, entities):
    """Task 13: search_lookup returns multiple matching rows."""
    from approot.repositories import generic_repo
    
    results = generic_repo.search_lookup(entities["status"], q="", limit=10)
    
    assert len(results) == 3  # All statuses


def test_lookup_search_respects_limit(setup_sqlalchemy_db, entities):
    """Task 13: search_lookup respects limit parameter."""
    from approot.repositories import generic_repo
    
    results = generic_repo.search_lookup(entities["status"], q="", limit=2)
    
    assert len(results) <= 2
