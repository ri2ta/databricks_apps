"""
Unit tests for SQLAlchemy error handling (Task 14).
Tests OperationalError, TimeoutError, and pool exhaustion scenarios.
Verifies that services/routes return HTMX-friendly error partials with correct status codes.
"""
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import OperationalError, TimeoutError as SQLAlchemyTimeoutError, DBAPIError


@pytest.fixture(autouse=True)
def stub_env(monkeypatch):
    """Set environment variables for SQLAlchemy"""
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("DB_POOL_SIZE", "5")


@pytest.fixture
def sample_entity():
    """Sample entity config for testing"""
    return {
        "name": "customer",
        "table": "customers",
        "label": "Customer",
        "primary_key": "id",
        "list": {
            "columns": [
                {"name": "id", "label": "ID", "width": 80, "sortable": True},
                {"name": "name", "label": "Name", "width": 200, "sortable": True},
                {"name": "email", "label": "Email", "width": 300, "sortable": True},
            ],
            "default_sort": "name",
            "page_size": 20,
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
        },
    }


# Task 14: Test OperationalError in fetch_list
def test_repo_fetch_list_operational_error_propagates(sample_entity):
    """Task 14: When OperationalError occurs in fetch_list, it should propagate to caller"""
    from approot.repositories import generic_repo
    from approot import db
    
    # Mock db.get_connection to raise OperationalError
    mock_error = OperationalError("connection failed", None, None)
    
    with patch.object(db, 'get_connection', side_effect=mock_error):
        with pytest.raises(OperationalError) as exc_info:
            generic_repo.fetch_list(sample_entity, page=1, page_size=10)
        
        assert "connection failed" in str(exc_info.value)


# Task 14: Test TimeoutError in fetch_detail
def test_repo_fetch_detail_timeout_error_propagates(sample_entity):
    """Task 14: When TimeoutError occurs in fetch_detail, it should propagate to caller"""
    from approot.repositories import generic_repo
    from approot import db
    
    # Mock db.get_connection to raise TimeoutError
    mock_error = SQLAlchemyTimeoutError("connection timeout")
    
    with patch.object(db, 'get_connection', side_effect=mock_error):
        with pytest.raises(SQLAlchemyTimeoutError) as exc_info:
            generic_repo.fetch_detail(sample_entity, pk=1)
        
        assert "connection timeout" in str(exc_info.value)


# Task 14: Test DBAPIError in save operation
def test_repo_save_dbapi_error_propagates(sample_entity):
    """Task 14: When DBAPIError occurs in save, it should propagate to caller"""
    from approot.repositories import generic_repo
    from approot import db
    
    # Mock db.get_connection to raise DBAPIError
    mock_error = DBAPIError("pool overflow", None, None)
    
    with patch.object(db, 'get_connection', side_effect=mock_error):
        with pytest.raises(DBAPIError) as exc_info:
            generic_repo.save(sample_entity, {"name": "Test", "email": "test@example.com"})
        
        assert "pool overflow" in str(exc_info.value)


# Task 14: Test service layer catches and returns error context with status 500
def test_service_render_list_handles_operational_error(sample_entity):
    """Task 14: Service layer should catch OperationalError and return error context with status 500 and generic message"""
    from approot.services import generic_service
    from approot.repositories import generic_repo
    
    entities = {"customer": sample_entity}
    mock_error = OperationalError("database unavailable", None, None)
    
    with patch.object(generic_repo, 'fetch_list', side_effect=mock_error):
        ctx = generic_service.render_list(entities, "customer", page=1)
        
        assert ctx.get('ok') is False
        assert ctx.get('status') == 500
        # UI message should be generic and not leak internal details
        error_msg = ctx.get('error', '')
        assert error_msg
        assert 'unavailable' not in error_msg.lower()


# Task 14: Test service layer handles TimeoutError with status 503
def test_service_render_detail_handles_timeout_error(sample_entity):
    """Task 14: Service layer should catch TimeoutError and return error context with status 503"""
    from approot.services import generic_service
    from approot.repositories import generic_repo
    
    entities = {"customer": sample_entity}
    mock_error = SQLAlchemyTimeoutError("query timeout")
    
    with patch.object(generic_repo, 'fetch_detail', side_effect=mock_error):
        ctx = generic_service.render_detail(entities, "customer", pk=1)
        
        assert ctx.get('ok') is False
        assert ctx.get('status') == 503  # Timeout errors should return 503
        # Error message should be concise
        error_msg = ctx.get('error', '')
        assert error_msg
        assert 'sqlalchemy' not in error_msg.lower()


# Task 14: Test service layer handles pool exhaustion/DBAPIError
def test_service_handle_save_handles_dbapi_error(sample_entity):
    """Task 14: Service layer should catch DBAPIError and return error context with status 503"""
    from approot.services import generic_service
    from approot.repositories import generic_repo
    
    entities = {"customer": sample_entity}
    payload = {"name": "Test", "email": "test@example.com"}
    
    # Pool exhaustion typically raises DBAPIError or OperationalError
    mock_error = DBAPIError("QueuePool limit exceeded", None, None)
    
    with patch.object(generic_repo, 'save', side_effect=mock_error):
        ctx = generic_service.handle_save(entities, "customer", payload)
        
        assert ctx.get('ok') is False
        # Should return 503 for pool/timeout errors
        assert ctx.get('status') == 503
        # Error message should be concise and user-friendly (Japanese or English)
        error_msg = ctx.get('error', '')
        assert error_msg  # Should have an error message
        # Should not expose technical details like "QueuePool limit exceeded"
        assert 'sqlalchemy' not in error_msg.lower()


# Task 14: Test service layer returns 503 for TimeoutError
def test_service_render_list_returns_503_for_timeout(sample_entity):
    """Task 14: Service layer should return 503 for TimeoutError (pool timeout)"""
    from approot.services import generic_service
    from approot.repositories import generic_repo
    
    entities = {"customer": sample_entity}
    mock_error = SQLAlchemyTimeoutError("QueuePool timeout")
    
    with patch.object(generic_repo, 'fetch_list', side_effect=mock_error):
        ctx = generic_service.render_list(entities, "customer", page=1)
        
        assert ctx.get('ok') is False
        assert ctx.get('status') == 503
        # Error message should be concise and not expose technical details
        error_msg = ctx.get('error', '')
        assert error_msg  # Should have an error message
        assert 'sqlalchemy' not in error_msg.lower()


# Task 14: Test Flask route returns error partial with status 500 for OperationalError
def test_flask_route_list_returns_500_for_operational_error(sample_entity):
    """Task 14: Flask route should return error partial with status 500 for database errors"""
    from approot import app as app_module
    from approot.services import generic_service
    
    # Create test client
    app = app_module.app
    app.config['TESTING'] = True
    client = app.test_client()
    
    # Patch entities to include our sample
    with patch.object(app_module, '_ENTITIES', {"customer": sample_entity}):
        # Mock service to return error context
        error_ctx = {
            "ok": False,
            "status": 500,
            "error": "Database connection failed",
            "mode": "list",
            "entity": sample_entity,
        }
        
        with patch.object(generic_service, 'render_list', return_value=error_ctx):
            response = client.get('/customer/list')
            
            assert response.status_code == 500
            assert b'alert-error' in response.data or b'alert' in response.data
            # Should render error partial, not expose technical details
            assert b'Database connection failed' in response.data or b'\xe3\x82\xa8\xe3\x83\xa9\xe3\x83\xbc' in response.data  # "エラー" in UTF-8


# Task 14: Test Flask route returns error partial with status 500 for TimeoutError
def test_flask_route_detail_returns_500_for_timeout_error(sample_entity):
    """Task 14: Flask route should return error partial with status 500 for timeout errors"""
    from approot import app as app_module
    from approot.services import generic_service
    
    app = app_module.app
    app.config['TESTING'] = True
    client = app.test_client()
    
    with patch.object(app_module, '_ENTITIES', {"customer": sample_entity}):
        error_ctx = {
            "ok": False,
            "status": 500,
            "error": "Query timeout exceeded",
            "mode": "view",
            "entity": sample_entity,
        }
        
        with patch.object(generic_service, 'render_detail', return_value=error_ctx):
            response = client.get('/customer/detail/1')
            
            assert response.status_code == 500
            assert b'alert-error' in response.data or b'alert' in response.data


# Task 14: Test Flask route returns 503 or 500 for pool exhaustion
def test_flask_route_save_returns_error_for_pool_exhaustion(sample_entity):
    """Task 14: Flask route should return error partial for pool exhaustion"""
    from approot import app as app_module
    from approot.services import generic_service
    
    app = app_module.app
    app.config['TESTING'] = True
    client = app.test_client()
    
    with patch.object(app_module, '_ENTITIES', {"customer": sample_entity}):
        error_ctx = {
            "ok": False,
            "status": 503,
            "error": "Service temporarily unavailable",
            "mode": "edit",
            "entity": sample_entity,
        }
        
        with patch.object(generic_service, 'handle_save', return_value=error_ctx):
            response = client.post('/customer/save', data={"name": "Test", "email": "test@example.com"})
            
            assert response.status_code == 503
            assert b'alert' in response.data


# Task 14: Test search_lookup handles OperationalError
def test_repo_search_lookup_operational_error_propagates(sample_entity):
    """Task 14: When OperationalError occurs in search_lookup, it should propagate"""
    from approot.repositories import generic_repo
    from approot import db
    
    mock_error = OperationalError("database down", None, None)
    
    with patch.object(db, 'get_connection', side_effect=mock_error):
        with pytest.raises(OperationalError) as exc_info:
            generic_repo.search_lookup(sample_entity, q="test", limit=10)
        
        assert "database down" in str(exc_info.value)


# Task 14: Test error messages are concise and logged (not exposed to UI)
def test_error_messages_are_concise_in_ui():
    """Task 14: Error messages should be concise for UI, detailed logging separate"""
    from approot import app as app_module
    from approot.services import generic_service
    
    app = app_module.app
    app.config['TESTING'] = True
    client = app.test_client()
    
    sample_entity = {
        "name": "test_entity",
        "table": "test_table",
        "primary_key": "id",
        "list": {"columns": [{"name": "id", "label": "ID"}], "page_size": 10},
        "form": {"sections": []},
    }
    
    with patch.object(app_module, '_ENTITIES', {"test_entity": sample_entity}):
        error_ctx = {
            "ok": False,
            "status": 500,
            "error": "Database error",  # Concise message
            "mode": "list",
            "entity": sample_entity,
        }
        
        with patch.object(generic_service, 'render_list', return_value=error_ctx):
            response = client.get('/test_entity/list')
            
            # Should not expose stack traces or detailed error info
            assert b'Traceback' not in response.data
            assert b'sqlalchemy' not in response.data.lower()
            # Should contain Japanese error title or generic error message
            assert response.status_code == 500
