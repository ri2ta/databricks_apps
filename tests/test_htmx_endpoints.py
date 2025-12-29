"""
Unit tests for HTMX endpoint templates (Task 4)
Tests datagrid, entity modes (view/create/edit), form, and lookup components.
"""
import sys
import types
import pytest
from flask import Flask, render_template_string


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
def app():
    """Create Flask app for template rendering tests"""
    from pathlib import Path
    template_folder = Path(__file__).resolve().parents[1] / "approot" / "templates"
    app = Flask(__name__, template_folder=str(template_folder))
    return app


@pytest.fixture
def entity_context():
    """Sample entity context for testing"""
    return {
        "name": "customer",
        "table": "customers",
        "label": "Customer",
        "primary_key": "id",
        "list": {
            "columns": [
                {"name": "name", "label": "Name", "width": 200, "sortable": True},
                {"name": "email", "label": "Email", "width": 300, "sortable": True},
                {"name": "status", "label": "Status", "width": 120, "sortable": False},
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
                        {"name": "name", "label": "Name", "type": "text"},
                        {"name": "email", "label": "Email", "type": "email"},
                    ],
                },
                {
                    "label": "Details",
                    "fields": [
                        {"name": "status", "label": "Status", "type": "lookup", "lookup": "status"},
                        {"name": "note", "label": "Note", "type": "textarea", "rows": 4},
                    ],
                }
            ],
            "actions": [
                {"name": "save", "label": "Save"},
                {"name": "cancel", "label": "Cancel"},
            ],
        },
    }


# Task 4: Test datagrid component
def test_datagrid_renders_columns(app, entity_context):
    """Task 4: datagrid component renders column headers"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/datagrid.html' %}",
            entity=entity_context,
            rows=[
                {"id": 1, "name": "Alice", "email": "alice@example.com", "status": "active"},
                {"id": 2, "name": "Bob", "email": "bob@example.com", "status": "inactive"},
            ],
            columns=entity_context["list"]["columns"],
            page=1,
            page_size=20,
            sort="name",
        )
        
        assert "Name" in html
        assert "Email" in html
        assert "Status" in html
        assert "Alice" in html
        assert "Bob" in html


def test_datagrid_pagination_htmx_attributes(app, entity_context):
    """Task 4: datagrid includes HTMX pagination controls"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/datagrid.html' %}",
            entity=entity_context,
            entity_name="customer",
            rows=[{"id": 1, "name": "Alice"}],
            columns=entity_context["list"]["columns"],
            page=2,
            page_size=20,
            sort="name",
        )
        
        # Should have pagination elements with hx-get
        assert "hx-get" in html
        assert "page=" in html or "Next" in html or "Previous" in html or "Prev" in html


def test_datagrid_sortable_columns(app, entity_context):
    """Task 4: datagrid renders sortable column headers"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/datagrid.html' %}",
            entity=entity_context,
            entity_name="customer",
            rows=[{"id": 1, "name": "Alice"}],
            columns=entity_context["list"]["columns"],
            page=1,
            page_size=20,
            sort=None,
        )
        
        # Sortable columns should have hx-get for sorting
        assert "hx-get" in html


# Task 4: Test entity template with mode switching
def test_entity_view_mode(app, entity_context):
    """Task 4: entity.html in view mode renders read-only display"""
    with app.app_context():
        html = render_template_string(
            "{% include 'partials/entity.html' %}",
            entity=entity_context,
            record={"id": 1, "name": "Alice", "email": "alice@example.com", "status": "active", "note": "Test note"},
            mode="view",
        )
        
        assert "Alice" in html
        assert "alice@example.com" in html
        # View mode should not have editable inputs
        assert 'readonly' in html or 'disabled' in html or mode_is_view_only(html)


def test_entity_create_mode(app, entity_context):
    """Task 4: entity.html in create mode renders empty form"""
    with app.app_context():
        html = render_template_string(
            "{% include 'partials/entity.html' %}",
            entity=entity_context,
            record=None,
            mode="create",
        )
        
        # Create mode should have form elements
        assert "input" in html.lower() or "form" in html.lower()
        # Should not have pre-filled values
        assert 'value=""' in html or 'value=' not in html or html.count('value=') <= html.count('type=')


def test_entity_edit_mode(app, entity_context):
    """Task 4: entity.html in edit mode renders populated form"""
    with app.app_context():
        html = render_template_string(
            "{% include 'partials/entity.html' %}",
            entity=entity_context,
            record={"id": 1, "name": "Alice", "email": "alice@example.com", "status": "active", "note": "Test note"},
            mode="edit",
        )
        
        assert "Alice" in html
        assert "alice@example.com" in html
        # Edit mode should have form inputs with values
        assert "input" in html.lower() or "form" in html.lower()


# Task 4: Test form component
def test_form_renders_sections(app, entity_context):
    """Task 4: form.html renders form sections"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/form.html' %}",
            entity=entity_context,
            record={"name": "Alice", "email": "alice@example.com"},
            mode="edit",
        )
        
        assert "Basic Info" in html
        assert "Details" in html


def test_form_renders_fields_by_type(app, entity_context):
    """Task 4: form.html renders different field types"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/form.html' %}",
            entity=entity_context,
            record={"name": "Alice", "email": "alice@example.com", "note": "Test"},
            mode="edit",
        )
        
        # Should have text and email inputs
        assert 'type="text"' in html or 'input' in html.lower()
        assert 'type="email"' in html or 'email' in html.lower()
        assert 'textarea' in html.lower()


# Task 4: Test field type components
def test_field_text_renders_input(app):
    """Task 4: field_types/text.html renders text input"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/field_types/text.html' %}",
            field={"name": "username", "label": "Username", "type": "text"},
            value="testuser",
            mode="edit",
        )
        
        assert 'type="text"' in html
        assert 'testuser' in html
        assert 'Username' in html


def test_field_text_readonly_in_view_mode(app):
    """Task 4: field_types/text.html is readonly in view mode"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/field_types/text.html' %}",
            field={"name": "username", "label": "Username", "type": "text"},
            value="testuser",
            mode="view",
        )
        
        assert 'readonly' in html or 'disabled' in html


def test_field_email_renders_email_input(app):
    """Task 4: field_types/email.html renders email input"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/field_types/email.html' %}",
            field={"name": "email", "label": "Email", "type": "email"},
            value="test@example.com",
            mode="edit",
        )
        
        assert 'type="email"' in html
        assert 'test@example.com' in html


def test_field_textarea_renders(app):
    """Task 4: field_types/textarea.html renders textarea"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/field_types/textarea.html' %}",
            field={"name": "note", "label": "Note", "type": "textarea", "rows": 4},
            value="Test note",
            mode="edit",
        )
        
        assert '<textarea' in html
        assert 'Test note' in html
        assert 'rows="4"' in html


def test_field_lookup_renders_with_modal_trigger(app):
    """Task 4: field_types/lookup.html renders lookup with modal trigger"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/field_types/lookup.html' %}",
            field={"name": "status", "label": "Status", "type": "lookup", "lookup": "status"},
            value="active",
            mode="edit",
        )
        
        # Should have a trigger for lookup modal
        assert 'lookup' in html.lower() or 'modal' in html.lower() or 'hx-get' in html


# Task 4: Test lookup component
def test_lookup_modal_renders(app):
    """Task 4: lookup.html renders modal structure"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/lookup.html' %}",
            lookup_name="status",
            field_name="status",
            results=[
                {"id": "active", "name": "Active"},
                {"id": "inactive", "name": "Inactive"},
            ],
        )
        
        assert 'modal' in html.lower() or 'dialog' in html.lower()
        assert 'Active' in html
        assert 'Inactive' in html


def test_lookup_search_htmx(app):
    """Task 4: lookup.html includes HTMX search"""
    with app.app_context():
        html = render_template_string(
            "{% include 'components/lookup.html' %}",
            lookup_name="status",
            field_name="status",
            results=[],
        )
        
        # Should have hx-get for search
        assert 'hx-get' in html or 'hx-trigger' in html


# Task 4: Test list partial uses datagrid
def test_list_partial_uses_datagrid(app, entity_context):
    """Task 4: partials/list.html uses datagrid component"""
    with app.app_context():
        html = render_template_string(
            "{% include 'partials/list.html' %}",
            entity=entity_context,
            entity_name="customer",
            rows=[{"id": 1, "name": "Alice"}],
            columns=entity_context["list"]["columns"],
            page=1,
            page_size=20,
        )
        
        # Should include datagrid rendering
        assert "Name" in html or "Alice" in html


# Helper function
def mode_is_view_only(html):
    """Helper to check if HTML represents view-only mode"""
    html_lower = html.lower()
    # In view mode, there should be no editable inputs or we see read-only patterns
    has_readonly = 'readonly' in html_lower or 'disabled' in html_lower
    has_form_control = '<input' in html_lower or '<textarea' in html_lower
    # If has form controls, they should be readonly/disabled
    return not has_form_control or has_readonly
