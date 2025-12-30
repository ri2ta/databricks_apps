"""
Test for entity load error banner display (Task 9)
This test verifies that when entities.yaml has errors, the UI shows a banner.
"""
import sys
import types
import pytest
from pathlib import Path


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


def test_entity_load_error_shows_banner_in_layout(monkeypatch):
    """Task 9: When entity loading fails, index route shows error banner"""
    from approot.services import entities_loader
    
    # Mock entities_loader to return failure
    def mock_load_entities_fail(path):
        return entities_loader.ValidationResult(
            success=False, 
            entities={}, 
            errors=["Missing required field 'table'"]
        )
    
    monkeypatch.setattr(entities_loader, 'load_entities', mock_load_entities_fail)
    
    # Need to reload app module to pick up the mocked loader
    import importlib
    from approot import app as app_module
    
    # Mock db functions
    from approot import db
    monkeypatch.setattr(db, 'init_pool', lambda: None)
    monkeypatch.setattr(db, 'close_pool', lambda: None)
    
    # Reload the app module to trigger entity loading with our mock
    importlib.reload(app_module)
    
    app = app_module.app
    app.config['TESTING'] = True
    
    with app.test_client() as client:
        response = client.get('/')
        assert response.status_code == 200
        # Should show error banner
        html = response.data.decode('utf-8')
        assert 'エンティティ設定エラー' in html
        assert 'alert' in html
