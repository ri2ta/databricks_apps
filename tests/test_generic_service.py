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


def test_render_list_handles_repo_error(monkeypatch, entities):
    from approot.services import generic_service

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(generic_service.generic_repo, "fetch_list", boom)

    ctx = generic_service.render_list(entities, "customer")

    assert ctx["ok"] is False
    assert ctx["status"] == 500
    assert "boom" in ctx["error"].lower()
