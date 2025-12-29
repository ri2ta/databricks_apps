"""
Task 5: Unit tests for Flask generic routes
Tests entity-based endpoints that use generic_service and return HTMX partial templates.
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
def mock_db(monkeypatch):
    """Mock db module to avoid actual DB connections."""
    import sys
    from pathlib import Path
    
    # Add approot to path
    approot_path = str(Path(__file__).resolve().parents[1] / "approot")
    if approot_path not in sys.path:
        sys.path.insert(0, approot_path)
    
    import db
    
    # Mock the connection pool functions
    monkeypatch.setattr(db, 'init_pool', lambda: None)
    monkeypatch.setattr(db, 'close_pool', lambda: None)
    monkeypatch.setattr(db, 'get_customers', lambda: [])
    monkeypatch.setattr(db, 'get_customer_detail', lambda customer_id: None)


@pytest.fixture
def mock_entities_loader(monkeypatch):
    """Mock entities_loader to return test entity definitions."""
    from approot.services import entities_loader
    
    test_entities = {
        "customer": {
            "name": "customer",
            "table": "customers",
            "label": "Customer",
            "primary_key": "id",
            "list": {
                "columns": [
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
                            {"name": "name", "label": "Name", "type": "text"},
                            {"name": "email", "label": "Email", "type": "email"},
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
def mock_generic_repo(monkeypatch):
    """Mock generic_repo to return test data."""
    from approot.repositories import generic_repo
    
    def mock_fetch_list(entity, page=1, page_size=None, sort=None):
        return [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]
    
    def mock_fetch_detail(entity, pk):
        if pk == 1:
            return {"id": 1, "name": "Alice", "email": "alice@example.com"}
        return None
    
    monkeypatch.setattr(generic_repo, 'fetch_list', mock_fetch_list)
    monkeypatch.setattr(generic_repo, 'fetch_detail', mock_fetch_detail)


@pytest.fixture
def client(mock_db, mock_entities_loader, mock_generic_repo):
    """Create Flask test client with mocked dependencies."""
    import sys
    from pathlib import Path
    
    # Add approot to path for relative imports
    approot_path = str(Path(__file__).resolve().parents[1] / "approot")
    if approot_path not in sys.path:
        sys.path.insert(0, approot_path)
    
    from app import app
    
    app.config['TESTING'] = True
    
    with app.test_client() as client:
        yield client


# Task 5: Test /<entity>/list endpoint
def test_entity_list_route(client):
    """Task 5: GET /<entity>/list returns list partial"""
    response = client.get('/customer/list')
    assert response.status_code == 200
    # Should contain customer data
    assert b'Alice' in response.data or b'alice@example.com' in response.data


def test_entity_list_route_with_pagination(client):
    """Task 5: GET /<entity>/list supports pagination parameters"""
    response = client.get('/customer/list?page=2&page_size=10')
    assert response.status_code == 200


def test_entity_list_route_with_sort(client):
    """Task 5: GET /<entity>/list supports sort parameter"""
    response = client.get('/customer/list?sort=name')
    assert response.status_code == 200


def test_entity_list_route_unknown_entity(client):
    """Task 5: GET /<entity>/list returns 404 for unknown entity"""
    response = client.get('/unknown_entity/list')
    assert response.status_code == 404


# Task 5: Test /<entity>/detail/<id> endpoint
def test_entity_detail_route(client):
    """Task 5: GET /<entity>/detail/<id> returns detail partial in view mode"""
    response = client.get('/customer/detail/1')
    assert response.status_code == 200
    # Should contain customer data
    assert b'Alice' in response.data or b'alice@example.com' in response.data


def test_entity_detail_route_not_found(client):
    """Task 5: GET /<entity>/detail/<id> returns 404 for non-existent record"""
    response = client.get('/customer/detail/999')
    assert response.status_code == 404


def test_entity_detail_route_unknown_entity(client):
    """Task 5: GET /<entity>/detail/<id> returns 404 for unknown entity"""
    response = client.get('/unknown_entity/detail/1')
    assert response.status_code == 404


# Task 5: Test /<entity>/form endpoint
def test_entity_form_create_route(client):
    """Task 5: GET /<entity>/form returns form in create mode"""
    response = client.get('/customer/form')
    assert response.status_code == 200
    # Should contain form elements
    assert b'form' in response.data.lower() or b'input' in response.data.lower()


def test_entity_form_edit_route(client):
    """Task 5: GET /<entity>/form/<id> returns form in edit mode"""
    response = client.get('/customer/form/1')
    assert response.status_code == 200
    # Should contain customer data in form
    assert b'Alice' in response.data or b'alice@example.com' in response.data


def test_entity_form_edit_route_not_found(client):
    """Task 5: GET /<entity>/form/<id> returns 404 for non-existent record"""
    response = client.get('/customer/form/999')
    assert response.status_code == 404


def test_entity_form_route_unknown_entity(client):
    """Task 5: GET /<entity>/form returns 404 for unknown entity"""
    response = client.get('/unknown_entity/form')
    assert response.status_code == 404


# Task 5: Test /<entity>/save endpoint
def test_entity_save_route(client, monkeypatch):
    """Task 5: POST /<entity>/save processes form data"""
    # For now, save returns 501 as it's not fully implemented
    # This test just verifies the route exists and handles the request
    response = client.post('/customer/save', data={
        'name': 'Charlie',
        'email': 'charlie@example.com',
    })
    
    # Should return 501 (Not Implemented) for now
    assert response.status_code == 501


def test_entity_save_route_unknown_entity(client):
    """Task 5: POST /<entity>/save returns 404 for unknown entity"""
    response = client.post('/unknown_entity/save', data={'name': 'Test'})
    assert response.status_code == 404


# Task 5: Test /<entity>/actions/<action> endpoint
def test_entity_action_route(client):
    """Task 5: POST /<entity>/actions/<action> dispatches custom action"""
    response = client.post('/customer/actions/export_csv')
    
    # May return 501 if handler not registered, or 200 if implemented
    # At minimum, should not crash
    assert response.status_code in [200, 404, 501]


def test_entity_action_route_unknown_entity(client):
    """Task 5: POST /<entity>/actions/<action> returns 404 for unknown entity"""
    response = client.post('/unknown_entity/actions/test')
    assert response.status_code == 404


def test_entity_action_route_unknown_action(client):
    """Task 5: POST /<entity>/actions/<action> returns 404 for unknown action"""
    response = client.post('/customer/actions/unknown_action')
    assert response.status_code == 404


# Task 5: Test /lookup/<lookup_name> endpoint
def test_lookup_route(client, monkeypatch):
    """Task 5: GET /lookup/<lookup_name> returns lookup results"""
    import sys
    from pathlib import Path
    
    # Add approot to path
    approot_path = str(Path(__file__).resolve().parents[1] / "approot")
    if approot_path not in sys.path:
        sys.path.insert(0, approot_path)
    
    from repositories import generic_repo
    
    def mock_search_lookup(entity, query, limit=20):
        return [
            {"id": "active", "name": "Active"},
            {"id": "inactive", "name": "Inactive"},
        ]
    
    monkeypatch.setattr(generic_repo, 'search_lookup', mock_search_lookup)
    
    # Add a 'status' entity to mock entities
    from app import _ENTITIES
    _ENTITIES['status'] = {
        'name': 'status',
        'table': 'statuses',
        'label': 'Status',
        'primary_key': 'id',
        'list': {'columns': [{'name': 'id'}, {'name': 'name'}]},
        'form': {'sections': []},
    }
    
    response = client.get('/lookup/status')
    assert response.status_code == 200
    # Should contain lookup results
    assert b'Active' in response.data or b'Inactive' in response.data


def test_lookup_route_with_query(client, monkeypatch):
    """Task 5: GET /lookup/<lookup_name> supports query parameter"""
    import sys
    from pathlib import Path
    
    # Add approot to path
    approot_path = str(Path(__file__).resolve().parents[1] / "approot")
    if approot_path not in sys.path:
        sys.path.insert(0, approot_path)
    
    from repositories import generic_repo
    
    def mock_search_lookup(entity, query, limit=20):
        if query == 'act':
            return [{"id": "active", "name": "Active"}]
        return []
    
    monkeypatch.setattr(generic_repo, 'search_lookup', mock_search_lookup)
    
    # Add a 'status' entity to mock entities
    from app import _ENTITIES
    _ENTITIES['status'] = {
        'name': 'status',
        'table': 'statuses',
        'label': 'Status',
        'primary_key': 'id',
        'list': {'columns': [{'name': 'id'}, {'name': 'name'}]},
        'form': {'sections': []},
    }
    
    response = client.get('/lookup/status?q=act')
    assert response.status_code == 200


# Task 5: Test that existing routes are not broken
def test_existing_index_route_still_works(client):
    """Task 5: Ensure existing / route is not affected"""
    response = client.get('/')
    assert response.status_code == 200


def test_existing_list_route_still_works(client):
    """Task 5: Ensure existing /list route is not affected"""
    response = client.get('/list')
    assert response.status_code == 200


def test_existing_detail_route_still_works(client):
    """Task 5: Ensure existing /detail/<id> route is not affected"""
    # This will return 404 as mock returns None, but route should exist
    response = client.get('/detail/1')
    assert response.status_code in [200, 404]
