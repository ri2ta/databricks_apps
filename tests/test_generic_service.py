"""
Unit tests for generic_service.py (Task 3)
Covers list/detail/form context shaping and action dispatch.
"""
import sys
import types
import pytest


@pytest.fixture(autouse=True)
def stub_requests_and_env(monkeypatch):
    """Stub network/databricks deps so approot.db imports cleanly."""
    # Set SQLAlchemy env vars for new db.py
    monkeypatch.setenv("SQLALCHEMY_DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("DB_POOL_SIZE", "5")


@pytest.fixture
def entities():
    return {
        "customer": {
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
                "actions": [
                    {"name": "export_csv", "label": "Export CSV"},
                ],
            },
            "form": {
                "sections": [
                    {
                        "label": "Main",
                        "fields": [
                            {"name": "name", "label": "Name", "type": "text"},
                            {"name": "email", "label": "Email", "type": "email"},
                        ],
                    }
                ],
                "actions": [
                    {"name": "calc_points", "label": "Calc Points"},
                ],
            },
        }
    }


def test_render_list_success(monkeypatch, entities):
    from approot.services import generic_service

    rows = [{"id": 1, "name": "Alice"}]
    monkeypatch.setattr(
        generic_service.generic_repo,
        "fetch_list",
        lambda entity, page=1, page_size=None, sort=None: rows,
    )

    ctx = generic_service.render_list(entities, "customer", page=2, page_size=5, sort="-name")

    assert ctx["ok"] is True
    assert ctx["mode"] == "list"
    assert ctx["rows"] == rows
    assert ctx["columns"][0]["name"] == "name"
    assert ctx["actions"][0]["name"] == "export_csv"
    assert ctx["page"] == 2
    assert ctx["page_size"] == 5
    assert ctx["sort"] == "-name"


def test_render_list_unknown_entity(entities):
    from approot.services import generic_service

    ctx = generic_service.render_list(entities, "unknown")

    assert ctx["ok"] is False
    assert ctx["status"] == 404
    assert "unknown entity" in ctx["error"].lower()


def test_render_detail_not_found(monkeypatch, entities):
    from approot.services import generic_service

    monkeypatch.setattr(generic_service.generic_repo, "fetch_detail", lambda entity, pk: None)

    ctx = generic_service.render_detail(entities, "customer", pk=999)

    assert ctx["ok"] is False
    assert ctx["status"] == 404
    assert ctx["record"] is None
    assert "not found" in ctx["error"].lower()


def test_render_detail_success(monkeypatch, entities):
    from approot.services import generic_service

    record = {"id": 2, "name": "Bob"}
    monkeypatch.setattr(generic_service.generic_repo, "fetch_detail", lambda entity, pk: record)

    ctx = generic_service.render_detail(entities, "customer", pk=2)

    assert ctx["ok"] is True
    assert ctx["mode"] == "view"
    assert ctx["record"]["id"] == 2
    assert ctx["actions"] == entities["customer"]["form"]["actions"]


def test_render_form_create(monkeypatch, entities):
    from approot.services import generic_service

    ctx = generic_service.render_form(entities, "customer")

    assert ctx["ok"] is True
    assert ctx["mode"] == "create"
    assert ctx["record"] is None
    assert ctx["form"] == entities["customer"]["form"]


def test_render_form_edit(monkeypatch, entities):
    from approot.services import generic_service

    record = {"id": 3, "name": "Carol"}
    monkeypatch.setattr(generic_service.generic_repo, "fetch_detail", lambda entity, pk: record)

    ctx = generic_service.render_form(entities, "customer", pk=3)

    assert ctx["ok"] is True
    assert ctx["mode"] == "edit"
    assert ctx["record"] == record


def test_render_form_missing_record(monkeypatch, entities):
    from approot.services import generic_service

    monkeypatch.setattr(generic_service.generic_repo, "fetch_detail", lambda entity, pk: None)

    ctx = generic_service.render_form(entities, "customer", pk=55)

    assert ctx["ok"] is False
    assert ctx["status"] == 404
    assert ctx["record"] is None


def test_handle_action_dispatch(monkeypatch, entities):
    from approot.services import generic_service

    called = {}

    def calc_handler(entity, payload):
        called["entity"] = entity["name"]
        called["payload"] = payload
        return {"ok": True, "points": 10}

    ctx = generic_service.handle_action(
        entities,
        entity_name="customer",
        action_name="calc_points",
        payload={"id": 1},
        handlers={"calc_points": calc_handler},
    )

    assert ctx["ok"] is True
    assert ctx["result"]["points"] == 10
    assert ctx["action"]["name"] == "calc_points"
    assert called["payload"]["id"] == 1


def test_handle_action_unknown(monkeypatch, entities):
    from approot.services import generic_service

    ctx = generic_service.handle_action(
        entities,
        entity_name="customer",
        action_name="unknown_action",
        payload={},
    )

    assert ctx["ok"] is False
    assert ctx["status"] == 404
    assert "unknown action" in ctx["error"].lower()


def test_handle_action_missing_handler(monkeypatch, entities):
    from approot.services import generic_service

    ctx = generic_service.handle_action(
        entities,
        entity_name="customer",
        action_name="calc_points",
        payload={},
        handlers={},
    )

    assert ctx["ok"] is False
    assert ctx["status"] == 501
    assert "handler" in ctx["error"].lower()


# Task 8: Additional service-level action dispatch tests


def test_handle_action_returns_error_context_with_template_fields(monkeypatch, entities):
    """Task 8: ensure error context carries ok/status/error for template rendering"""
    from approot.services import generic_service

    ctx = generic_service.handle_action(
        entities,
        entity_name="customer",
        action_name="unknown_action",
        payload={}
    )

    assert ctx["ok"] is False
    assert ctx["status"] == 404
    assert "unknown action" in ctx["error"].lower()


def test_handle_action_payload_defaults_to_empty_dict(monkeypatch, entities):
    """Task 8: None payload should be converted to {} and handed to handler"""
    from approot.services import generic_service

    called = {}

    def handler(entity, payload):
        called["payload"] = payload
        return {"ok": True}

    ctx = generic_service.handle_action(
        entities,
        entity_name="customer",
        action_name="calc_points",
        payload=None,
        handlers={"calc_points": handler},
    )

    assert ctx["ok"] is True
    assert called["payload"] == {}


def test_render_list_handles_repo_error(monkeypatch, entities):
    from approot.services import generic_service

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(generic_service.generic_repo, "fetch_list", boom)

    ctx = generic_service.render_list(entities, "customer")

    assert ctx["ok"] is False
    assert ctx["status"] == 500
    assert "boom" in ctx["error"].lower()


# Task 6: Edge cases and error paths


def test_render_list_with_none_entities():
    """Task 6: Test render_list when entities dict is None"""
    from approot.services import generic_service

    ctx = generic_service.render_list(None, "customer")

    assert ctx["ok"] is False
    assert ctx["status"] == 404
    assert "unknown entity" in ctx["error"].lower()


def test_render_list_with_empty_entities():
    """Task 6: Test render_list with empty entities dict"""
    from approot.services import generic_service

    ctx = generic_service.render_list({}, "customer")

    assert ctx["ok"] is False
    assert ctx["status"] == 404


def test_render_detail_with_none_entities():
    """Task 6: Test render_detail when entities dict is None"""
    from approot.services import generic_service

    ctx = generic_service.render_detail(None, "customer", pk=1)

    assert ctx["ok"] is False
    assert ctx["status"] == 404


def test_render_form_with_none_entities():
    """Task 6: Test render_form when entities dict is None"""
    from approot.services import generic_service

    ctx = generic_service.render_form(None, "customer")

    assert ctx["ok"] is False
    assert ctx["status"] == 404


def test_render_detail_handles_repo_error(monkeypatch, entities):
    """Task 6: Test render_detail when repository raises exception"""
    from approot.services import generic_service

    def boom(*args, **kwargs):
        raise RuntimeError("database error")

    monkeypatch.setattr(generic_service.generic_repo, "fetch_detail", boom)

    ctx = generic_service.render_detail(entities, "customer", pk=1)

    assert ctx["ok"] is False
    assert ctx["status"] == 500
    assert "database error" in ctx["error"].lower()


def test_render_form_edit_handles_repo_error(monkeypatch, entities):
    """Task 6: Test render_form in edit mode when repository raises exception"""
    from approot.services import generic_service

    def boom(*args, **kwargs):
        raise RuntimeError("fetch failed")

    monkeypatch.setattr(generic_service.generic_repo, "fetch_detail", boom)

    ctx = generic_service.render_form(entities, "customer", pk=1)

    assert ctx["ok"] is False
    assert ctx["status"] == 500
    assert "fetch failed" in ctx["error"].lower()


def test_handle_action_with_none_entities():
    """Task 6: Test handle_action when entities dict is None"""
    from approot.services import generic_service

    ctx = generic_service.handle_action(
        None,
        entity_name="customer",
        action_name="test",
        payload={}
    )

    assert ctx["ok"] is False
    assert ctx["status"] == 404


def test_handle_action_with_empty_payload():
    """Task 6: Test handle_action with None payload"""
    from approot.services import generic_service

    entities = {
        "customer": {
            "name": "customer",
            "table": "customers",
            "form": {
                "actions": [
                    {"name": "test_action", "label": "Test"}
                ]
            }
        }
    }

    def handler(entity, payload):
        return {"ok": True, "payload_received": payload}

    ctx = generic_service.handle_action(
        entities,
        entity_name="customer",
        action_name="test_action",
        payload=None,
        handlers={"test_action": handler}
    )

    assert ctx["ok"] is True
    assert ctx["result"]["payload_received"] == {}


def test_handle_action_handler_raises_exception(monkeypatch, entities):
    """Task 6: Test handle_action when handler raises exception"""
    from approot.services import generic_service

    def failing_handler(entity, payload):
        raise ValueError("handler failed")

    ctx = generic_service.handle_action(
        entities,
        entity_name="customer",
        action_name="calc_points",
        payload={"id": 1},
        handlers={"calc_points": failing_handler}
    )

    assert ctx["ok"] is False
    assert ctx["status"] == 500
    assert "handler failed" in ctx["error"].lower()


def test_render_list_with_missing_list_config(monkeypatch):
    """Task 6: Test render_list when entity has no list config"""
    from approot.services import generic_service

    entities = {
        "customer": {
            "name": "customer",
            "table": "customers",
            # Missing "list" key
            "form": {"sections": []}
        }
    }

    monkeypatch.setattr(
        generic_service.generic_repo,
        "fetch_list",
        lambda entity, page=1, page_size=None, sort=None: []
    )

    ctx = generic_service.render_list(entities, "customer")

    assert ctx["ok"] is True
    # Should handle missing list config gracefully
    assert ctx["columns"] == []
    assert ctx["actions"] == []


def test_render_detail_with_missing_form_config():
    """Task 6: Test render_detail when entity has no form config"""
    from approot.services import generic_service

    entities = {
        "customer": {
            "name": "customer",
            "table": "customers",
            "list": {"columns": []},
            # Missing "form" key
        }
    }

    def mock_fetch_detail(entity, pk):
        return {"id": 1, "name": "Alice"}

    import approot.services.generic_service as gs
    original_fetch = gs.generic_repo.fetch_detail
    gs.generic_repo.fetch_detail = mock_fetch_detail

    try:
        ctx = generic_service.render_detail(entities, "customer", pk=1)

        assert ctx["ok"] is True
        # Should handle missing form config gracefully
        assert ctx["actions"] == []
    finally:
        gs.generic_repo.fetch_detail = original_fetch


def test_render_form_create_with_missing_form_config():
    """Task 6: Test render_form create mode when entity has no form config"""
    from approot.services import generic_service

    entities = {
        "customer": {
            "name": "customer",
            "table": "customers",
            "list": {"columns": []},
            # Missing "form" key
        }
    }

    ctx = generic_service.render_form(entities, "customer")

    assert ctx["ok"] is True
    assert ctx["mode"] == "create"
    assert ctx["form"] == {}
    assert ctx["actions"] == []


def test_find_action_in_list_actions():
    """Task 6: Test that actions can be found in list.actions"""
    from approot.services.generic_service import _find_action

    entity = {
        "list": {
            "actions": [
                {"name": "export", "label": "Export"}
            ]
        },
        "form": {
            "actions": []
        }
    }

    action = _find_action(entity, "export")
    assert action is not None
    assert action["name"] == "export"


def test_find_action_not_in_any_list():
    """Task 6: Test _find_action returns None for non-existent action"""
    from approot.services.generic_service import _find_action

    entity = {
        "list": {"actions": []},
        "form": {"actions": [{"name": "save", "label": "Save"}]}
    }

    action = _find_action(entity, "nonexistent")
    assert action is None


def test_render_list_with_none_page_and_sort(monkeypatch, entities):
    """Task 6: Test render_list with None page and sort parameters"""
    from approot.services import generic_service

    rows = [{"id": 1, "name": "Alice"}]
    monkeypatch.setattr(
        generic_service.generic_repo,
        "fetch_list",
        lambda entity, page=1, page_size=None, sort=None: rows
    )

    ctx = generic_service.render_list(entities, "customer", page=None, sort=None)

    assert ctx["ok"] is True
    assert ctx["page"] == 1  # Should default to 1
    assert ctx["sort"] == "name"  # Should use default_sort


def test_handle_action_with_empty_handlers_dict(entities):
    """Task 6: Test handle_action with empty handlers dict instead of None"""
    from approot.services import generic_service

    ctx = generic_service.handle_action(
        entities,
        entity_name="customer",
        action_name="calc_points",
        payload={},
        handlers={}  # Empty dict, not None
    )

    assert ctx["ok"] is False
    assert ctx["status"] == 501
    assert "handler" in ctx["error"].lower()
