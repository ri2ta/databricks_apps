# app.py
import logging
import atexit
from flask import Flask, render_template, request
from . import db
from .services import entities_loader, generic_service
from pathlib import Path

# Basic logging setup so db.py logs show up on console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = Flask(__name__)
app.logger.setLevel(logging.INFO)
db.logger = app.logger  # unify db.py logging with Flask's logger

# Initialize DB connection pool once on startup
def _init_db_pool():
    try:
        db.init_pool()
    except Exception:
        app.logger.exception("Failed to initialize DB pool")

_init_db_pool()

# Close pool only when process exits; keep it warm between requests
atexit.register(db.close_pool)

# Load entity definitions once at startup
_entities_path = Path(__file__).parent / "config" / "entities.yaml"
_validation_result = entities_loader.load_entities(str(_entities_path))
if not _validation_result.success:
    app.logger.error("Failed to load entities: %s", _validation_result.errors)
    _ENTITIES = {}
else:
    _ENTITIES = _validation_result.entities
    app.logger.info("Loaded %d entities: %s", len(_ENTITIES), list(_ENTITIES.keys()))

# Action handlers registry
# Register custom action handlers here
# Example: _ACTION_HANDLERS = {"export_csv": my_export_handler, "calc_points": calc_handler}
_ACTION_HANDLERS = {}


@app.route('/')
def index():
    return render_template('layout.html')

@app.route('/detail/<int:customer_id>')
def detail_view(customer_id):
    customer = db.get_customer_detail(customer_id=customer_id)
    if customer is None:
        return "Customer not found", 404
    return render_template('partials/detail.html', customer=customer)


# === Generic Entity Routes (Task 5) ===

@app.route('/<entity_name>/list')
def entity_list(entity_name):
    """Generic list endpoint for any entity"""
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', type=int)
    sort = request.args.get('sort', type=str)
    
    ctx = generic_service.render_list(_ENTITIES, entity_name, page=page, page_size=page_size, sort=sort)
    
    if not ctx.get('ok'):
        status = ctx.get('status', 500)
        error = ctx.get('error', 'Unknown error')
        app.logger.error("entity_list failed for %s: %s", entity_name, error)
        return error, status
    
    return render_template('partials/list.html', **ctx)


@app.route('/<entity_name>/detail/<int:pk>')
def entity_detail(entity_name, pk):
    """Generic detail endpoint for any entity (view mode)"""
    ctx = generic_service.render_detail(_ENTITIES, entity_name, pk)
    
    if not ctx.get('ok'):
        status = ctx.get('status', 500)
        error = ctx.get('error', 'Unknown error')
        app.logger.error("entity_detail failed for %s/%s: %s", entity_name, pk, error)
        return error, status
    
    return render_template('partials/entity.html', **ctx)


@app.route('/<entity_name>/form', methods=['GET'])
@app.route('/<entity_name>/form/<int:pk>', methods=['GET'])
def entity_form(entity_name, pk=None):
    """Generic form endpoint for any entity (create/edit mode)"""
    ctx = generic_service.render_form(_ENTITIES, entity_name, pk)
    
    if not ctx.get('ok'):
        status = ctx.get('status', 500)
        error = ctx.get('error', 'Unknown error')
        app.logger.error("entity_form failed for %s/%s: %s", entity_name, pk, error)
        return error, status
    
    return render_template('partials/entity.html', **ctx)


@app.route('/<entity_name>/save', methods=['POST'])
def entity_save(entity_name):
    """Generic save endpoint for any entity"""
    payload = request.form.to_dict()
    
    ctx = generic_service.handle_save(_ENTITIES, entity_name, payload)
    
    if not ctx.get('ok'):
        status = ctx.get('status', 500)
        error = ctx.get('error')
        
        if status == 404:
            # Unknown entity or record not found
            app.logger.error("entity_save: %s", error)
            return error or "Not found", 404
        elif status == 400:
            # Validation error - return form with errors
            app.logger.warning("entity_save validation failed for %s: %s", entity_name, ctx.get('errors'))
            return render_template('partials/entity.html', **ctx), 400
        else:
            # Server error
            app.logger.error("entity_save failed for %s: %s", entity_name, error)
            return error or "Save failed", status
    
    # Success - return detail partial
    return render_template('partials/entity.html', **ctx), 200


@app.route('/<entity_name>/actions/<action_name>', methods=['POST'])
def entity_action(entity_name, action_name):
    """Generic action endpoint for any entity"""
    # Accept either form data or JSON payload
    if request.is_json:
        payload = request.get_json() or {}
    else:
        payload = request.form.to_dict()
    
    # Call service with registered handlers
    ctx = generic_service.handle_action(_ENTITIES, entity_name, action_name, payload, handlers=_ACTION_HANDLERS)
    
    if not ctx.get('ok'):
        status = ctx.get('status', 500)
        error = ctx.get('error', 'Unknown error')
        app.logger.error("entity_action failed for %s/%s: %s", entity_name, action_name, error)
        return error, status
    
    # Return partial template with action result
    return render_template('partials/action_result.html', **ctx), 200


@app.route('/lookup/<lookup_name>')
def lookup_search(lookup_name):
    """Generic lookup search endpoint"""
    query = request.args.get('q', '')
    limit = request.args.get('limit', 20, type=int)
    
    try:
        # Try to find an entity with this name for the lookup
        # In a real implementation, there might be a lookup config mapping
        # For now, we'll try to use the lookup_name as entity_name
        entity = _ENTITIES.get(lookup_name)
        
        if not entity:
            # If no entity found, return empty results
            app.logger.warning("No entity found for lookup: %s", lookup_name)
            results = []
        else:
            from .repositories import generic_repo
            results = generic_repo.search_lookup(entity, query, limit=limit)
        
        return render_template('components/lookup.html', 
                             lookup_name=lookup_name,
                             field_name=request.args.get('field_name', ''),
                             results=results)
    except Exception as exc:
        app.logger.exception("lookup_search failed for %s", lookup_name)
        return f"Lookup failed: {str(exc)}", 500