"""
Task 7: Unit tests for POST /<entity>/save endpoint (TDD)
Tests save pipeline: validation, insert, update, error handling.
"""
import sys
import types
import pytest
from flask import Flask


@pytest.fixture(autouse=True)
def stub_requests_and_env(monkeypatch):
    """Stub network/databricks deps so approot modules import cleanly."""
    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"access_token": "dummy"}

    def fake_post(*args, **kwargs):
        return _Resp()

    requests_stub = types.SimpleNamespace(post=fake_post)
    monkeypatch.setitem(sys.modules, "requests", requests_stub)

    sql_stub = types.SimpleNamespace(connect=lambda **kwargs: None)
    databricks_stub = types.SimpleNamespace(sql=sql_stub)
    monkeypatch.setitem(sys.modules, "databricks", databricks_stub)
    monkeypatch.setitem(sys.modules, "databricks.sql", sql_stub)

    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "dummy-host")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/dummy")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "dummy-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "dummy-secret")


@pytest.fixture
def save_test_mock_db(monkeypatch):
    """Mock db module to avoid actual DB connections."""
    import sys
    from pathlib import Path
    
    root_path = str(Path(__file__).resolve().parents[1])
    if root_path not in sys.path:
        sys.path.insert(0, root_path)
    
    from approot import db
    
    monkeypatch.setattr(db, 'init_pool', lambda: None)
    monkeypatch.setattr(db, 'close_pool', lambda: None)
    monkeypatch.setattr(db, 'get_customers', lambda limit=100: [])
    monkeypatch.setattr(db, 'get_customer_detail', lambda customer_id: None)


@pytest.fixture
def save_test_mock_entities_loader(monkeypatch):
    """Mock entities_loader to return test entity definitions with required fields."""
    from approot.services import entities_loader
    
    test_entities = {
        "customer": {
            "name": "customer",
            "table": "customers",
            "label": "Customer",
            "primary_key": "id",
            "list": {
                "columns": [
                    {"name": "id", "label": "ID", "width": 100, "sortable": True},
                    {"name": "name", "label": "Name", "width": 200, "sortable": True},
                    {"name": "email", "label": "Email", "width": 300, "sortable": True},
                ],
                "default_sort": "name",
                "page_size": 20,
                "actions": [],
            },
            "form": {
                "sections": [
                    {
                        "label": "Basic Info",
                        "fields": [
                            {"name": "name", "label": "Name", "type": "text", "required": True},
                            {"name": "email", "label": "Email", "type": "email", "required": True},
                        ],
                    }
                ],
                "actions": [
                    {"name": "save", "label": "Save"},
                ],
            },
        },
    }
    
    def mock_load_entities(path):
        return entities_loader.ValidationResult(success=True, entities=test_entities)
    
    monkeypatch.setattr(entities_loader, 'load_entities', mock_load_entities)
    return test_entities


@pytest.fixture
def save_test_mock_generic_repo(monkeypatch):
    """Mock generic_repo with save functionality."""
    from approot.repositories import generic_repo
    
    # In-memory store for testing
    _store = {
        1: {"id": 1, "name": "Alice", "email": "alice@example.com"},
        2: {"id": 2, "name": "Bob", "email": "bob@example.com"},
    }
    _next_id = 3
    
    def mock_fetch_list(entity, page=1, page_size=None, sort=None):
        return list(_store.values())
    
    def mock_fetch_detail(entity, pk):
        return _store.get(pk)
    
    def mock_save(entity, payload):
        """Mock save that returns saved record with id."""
        nonlocal _next_id
        pk_name = entity.get("primary_key", "id")
        
        if pk_name in payload and payload[pk_name]:
            # Update existing
            pk = payload[pk_name]
            if pk not in _store:
                raise ValueError(f"Record with {pk_name}={pk} not found")
            _store[pk] = payload
            return payload
        else:
            # Insert new
            new_id = _next_id
            _next_id += 1
            payload[pk_name] = new_id
            _store[new_id] = payload
            return payload
    
    monkeypatch.setattr(generic_repo, 'fetch_list', mock_fetch_list)
    monkeypatch.setattr(generic_repo, 'fetch_detail', mock_fetch_detail)
    monkeypatch.setattr(generic_repo, 'save', mock_save)


@pytest.fixture
def client(save_test_mock_db, save_test_mock_entities_loader, save_test_mock_generic_repo):
    """Create Flask test client with mocked dependencies."""
    import sys
    from pathlib import Path
    
    # Ensure project root is in sys.path so approot can be imported as a package
    root_path = str(Path(__file__).resolve().parents[1])
    if root_path not in sys.path:
        sys.path.insert(0, root_path)
    
    from approot.app import app
    
    app.config['TESTING'] = True
    
    with app.test_client() as client:
        yield client


# Task 7: Test save success (insert)
def test_save_insert_success_returns_detail_200(client):
    """Task 7: POST /<entity>/save for new record returns detail partial with 200"""
    response = client.post('/customer/save', data={
        'name': 'Charlie',
        'email': 'charlie@example.com',
    })
    
    assert response.status_code == 200
    # Should contain the saved data
    assert b'Charlie' in response.data
    assert b'charlie@example.com' in response.data


# Task 7: Test save success (update)
def test_save_update_success_returns_detail_200(client):
    """Task 7: POST /<entity>/save for existing record returns detail partial with 200"""
    response = client.post('/customer/save', data={
        'id': '1',
        'name': 'Alice Updated',
        'email': 'alice.updated@example.com',
    })
    
    assert response.status_code == 200
    # Should contain the updated data
    assert b'Alice Updated' in response.data
    assert b'alice.updated@example.com' in response.data


# Task 7: Test validation error
def test_save_validation_error_returns_form_400(client):
    """Task 7: POST /<entity>/save with validation errors returns form partial with 400"""
    # Missing required field 'email'
    response = client.post('/customer/save', data={
        'name': 'Invalid',
    })
    
    assert response.status_code == 400
    # Should return form with error message
    assert b'form' in response.data.lower() or b'error' in response.data.lower()
    # Should indicate email is required
    assert b'email' in response.data.lower()


# Task 7: Test validation error for invalid email
def test_save_invalid_email_returns_form_400(client):
    """Task 7: POST /<entity>/save with invalid email returns form partial with 400"""
    response = client.post('/customer/save', data={
        'name': 'Test User',
        'email': 'not-an-email',
    })
    
    assert response.status_code == 400
    # Should return form with email error
    assert b'form' in response.data.lower() or b'error' in response.data.lower()
    assert b'email' in response.data.lower()


# Task 7: Test unknown entity
def test_save_unknown_entity_returns_404(client):
    """Task 7: POST /<entity>/save for unknown entity returns 404"""
    response = client.post('/unknown_entity/save', data={
        'name': 'Test',
        'email': 'test@example.com',
    })
    
    assert response.status_code == 404
    assert b'Unknown entity' in response.data or b'not found' in response.data.lower()


# Task 7: Test unknown record (update non-existent)
def test_save_update_nonexistent_record_returns_404(client):
    """Task 7: POST /<entity>/save for non-existent record id returns 404"""
    response = client.post('/customer/save', data={
        'id': '999',
        'name': 'Does Not Exist',
        'email': 'dne@example.com',
    })
    
    assert response.status_code == 404
    assert b'not found' in response.data.lower()


# Task 7: Test server error (simulated)
def test_save_server_error_returns_500(client, monkeypatch):
    """Task 7: POST /<entity>/save with server error returns 500"""
    import sys
    from pathlib import Path
    
    root_path = str(Path(__file__).resolve().parents[1])
    if root_path not in sys.path:
        sys.path.insert(0, root_path)
    
    from approot.repositories import generic_repo
    
    def mock_save_with_error(entity, payload):
        raise RuntimeError("Database connection failed")
    
    monkeypatch.setattr(generic_repo, 'save', mock_save_with_error)
    
    response = client.post('/customer/save', data={
        'name': 'Error Test',
        'email': 'error@example.com',
    })
    
    assert response.status_code == 500
    # Should contain error message
    assert b'error' in response.data.lower() or b'failed' in response.data.lower()


# Task 7: Test field errors are displayed in form
def test_save_validation_error_shows_field_errors(client):
    """Task 7: Validation errors show per-field error messages in form"""
    # Missing both required fields
    response = client.post('/customer/save', data={})
    
    assert response.status_code == 400
    # Should show errors for both fields
    data_lower = response.data.lower()
    assert b'name' in data_lower
    assert b'email' in data_lower
    assert b'required' in data_lower or b'error' in data_lower


# Task 7: Test HTMX-friendly response for success
def test_save_success_returns_htmx_partial(client):
    """Task 7: Save success returns HTMX-compatible partial template"""
    response = client.post('/customer/save', data={
        'name': 'HTMX Test',
        'email': 'htmx@example.com',
    })
    
    assert response.status_code == 200
    # Should be a partial (not full page)
    # Partials typically don't have <html> or <head> tags
    assert b'<!DOCTYPE' not in response.data
    assert b'<html>' not in response.data


# Task 7: Test HTMX-friendly response for validation error
def test_save_validation_error_returns_htmx_partial(client):
    """Task 7: Validation error returns HTMX-compatible form partial"""
    response = client.post('/customer/save', data={
        'name': 'No Email',
    })
    
    assert response.status_code == 400
    # Should be a partial (not full page)
    assert b'<!DOCTYPE' not in response.data
    assert b'<html>' not in response.data
    # Should contain form elements
    assert b'form' in response.data.lower() or b'input' in response.data.lower()


# Task 7: Edge case - empty payload
def test_save_empty_payload_returns_400(client):
    """Task 7: Save with empty payload returns validation error"""
    response = client.post('/customer/save', data={})
    
    assert response.status_code == 400


# Task 7: Edge case - extra fields
def test_save_with_extra_fields_ignores_them(client):
    """Task 7: Save with extra fields ignores unknown fields"""
    response = client.post('/customer/save', data={
        'name': 'Extra Fields',
        'email': 'extra@example.com',
        'unknown_field': 'should be ignored',
        'another_unknown': 'also ignored',
    })
    
    # Should succeed, ignoring unknown fields
    assert response.status_code == 200
    assert b'Extra Fields' in response.data


# Task 7: Test special characters in data
def test_save_with_special_characters(client):
    """Task 7: Save handles special characters correctly"""
    response = client.post('/customer/save', data={
        'name': "O'Brien & Co.",
        'email': 'test+special@example.com',
    })
    
    assert response.status_code == 200
    assert b"O'Brien" in response.data or b"O&#39;Brien" in response.data


# Task 7: Test SQL injection attempt
def test_save_prevents_sql_injection(client):
    """Task 7: Save prevents SQL injection in field values"""
    response = client.post('/customer/save', data={
        'name': "'; DROP TABLE customers; --",
        'email': 'hacker@example.com',
    })
    
    # Should either succeed (storing the malicious string as data) or validate/reject
    assert response.status_code in [200, 400]
    # The key is that it doesn't actually execute SQL injection
