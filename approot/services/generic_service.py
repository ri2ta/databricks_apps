"""
Generic service layer for YAML-defined entities.
Builds contexts for list/detail/form views and dispatches actions.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Callable

from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError, DBAPIError

from . import entities_loader
from ..repositories import generic_repo

logger = logging.getLogger(__name__)

ActionHandler = Callable[[Dict[str, Any], Dict[str, Any]], Any]
ActionHandlers = Mapping[str, ActionHandler]


def _missing_entity(entity_name: str, mode: str) -> Dict[str, Any]:
    return {
        "ok": False,
        "status": 404,
        "error": f"Unknown entity '{entity_name}'",
        "entity": None,
        "entity_name": entity_name,
        "mode": mode,
    }


def _error_context(entity: Dict[str, Any] | None, mode: str, exc: Exception, status: int = 500) -> Dict[str, Any]:
    """
    Build error context from exception.
    Returns 503 for pool/timeout errors, 500 for other operational errors.
    """
    # Check if this is a pool timeout or exhaustion error
    if isinstance(exc, SQLAlchemyTimeoutError) or isinstance(exc, DBAPIError):
        error_msg = str(exc)
        # Pool-related errors should return 503 (Service Unavailable)
        if 'QueuePool' in error_msg or 'timeout' in error_msg.lower() or 'pool' in error_msg.lower():
            return {
                "ok": False,
                "status": 503,
                "error": "サービスが一時的に利用できません",  # Service temporarily unavailable
                "entity": entity,
                "mode": mode,
            }
    
    # Default: return provided status (usually 500) with exception message
    return {
        "ok": False,
        "status": status,
        "error": str(exc),
        "entity": entity,
        "mode": mode,
    }


def _get_entity(entities: Dict[str, Dict[str, Any]], entity_name: str) -> Dict[str, Any] | None:
    return entities_loader.get_entity(entities, entity_name) if entities else None


def _find_action(entity: Dict[str, Any], action_name: str) -> Dict[str, Any] | None:
    for action in entity.get("form", {}).get("actions", []):
        if action.get("name") == action_name:
            return action
    for action in entity.get("list", {}).get("actions", []):
        if action.get("name") == action_name:
            return action
    return None


def render_list(
    entities: Dict[str, Dict[str, Any]],
    entity_name: str,
    page: int = 1,
    page_size: int | None = None,
    sort: str | None = None,
) -> Dict[str, Any]:
    entity = _get_entity(entities, entity_name)
    if not entity:
        return _missing_entity(entity_name, mode="list")

    cfg = entity.get("list", {})
    try:
        rows = generic_repo.fetch_list(entity, page=page, page_size=page_size, sort=sort)
    except Exception as exc:  # pragma: no cover - validated via failure context
        logger.exception("render_list failed for entity=%s", entity_name)
        return _error_context(entity, mode="list", exc=exc)

    return {
        "ok": True,
        "status": 200,
        "mode": "list",
        "entity": entity,
        "entity_name": entity_name,
        "rows": rows,
        "columns": cfg.get("columns", []),
        "actions": cfg.get("actions", []),
        "page": max(1, page or 1),
        "page_size": page_size or cfg.get("page_size", 20),
        "sort": sort or cfg.get("default_sort"),
    }


def render_detail(
    entities: Dict[str, Dict[str, Any]],
    entity_name: str,
    pk: Any,
) -> Dict[str, Any]:
    entity = _get_entity(entities, entity_name)
    if not entity:
        return _missing_entity(entity_name, mode="view")

    try:
        record = generic_repo.fetch_detail(entity, pk)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("render_detail failed for entity=%s", entity_name)
        return _error_context(entity, mode="view", exc=exc)

    if record is None:
        return {
            "ok": False,
            "status": 404,
            "error": f"{entity_name} not found",
            "entity": entity,
            "record": None,
            "mode": "view",
        }

    return {
        "ok": True,
        "status": 200,
        "entity": entity,
        "record": record,
        "mode": "view",
        "actions": entity.get("form", {}).get("actions", []),
    }


def render_form(
    entities: Dict[str, Dict[str, Any]],
    entity_name: str,
    pk: Any | None = None,
) -> Dict[str, Any]:
    entity = _get_entity(entities, entity_name)
    if not entity:
        return _missing_entity(entity_name, mode="form")

    mode = "create" if pk is None else "edit"
    record = None

    if pk is not None:
        try:
            record = generic_repo.fetch_detail(entity, pk)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("render_form failed for entity=%s", entity_name)
            return _error_context(entity, mode=mode, exc=exc)

        if record is None:
            return {
                "ok": False,
                "status": 404,
                "error": f"{entity_name} not found",
                "entity": entity,
                "record": None,
                "mode": mode,
            }

    form_cfg = entity.get("form", {})
    return {
        "ok": True,
        "status": 200,
        "entity": entity,
        "record": record,
        "mode": mode,
        "form": form_cfg,
        "actions": form_cfg.get("actions", []),
    }


def handle_action(
    entities: Dict[str, Dict[str, Any]],
    entity_name: str,
    action_name: str,
    payload: Dict[str, Any] | None = None,
    handlers: ActionHandlers | None = None,
) -> Dict[str, Any]:
    entity = _get_entity(entities, entity_name)
    if not entity:
        return _missing_entity(entity_name, mode="action")

    action_def = _find_action(entity, action_name)
    if not action_def:
        return {
            "ok": False,
            "status": 404,
            "error": f"Unknown action '{action_name}'",
            "entity": entity,
            "action": None,
            "mode": "action",
        }

    handler = (handlers or {}).get(action_name)
    if handler is None:
        return {
            "ok": False,
            "status": 501,
            "error": "Action handler not registered",
            "entity": entity,
            "action": action_def,
            "mode": "action",
        }

    try:
        result = handler(entity, payload or {})
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("handle_action failed for entity=%s action=%s", entity_name, action_name)
        return _error_context(entity, mode="action", exc=exc)

    return {
        "ok": True,
        "status": 200,
        "entity": entity,
        "action": action_def,
        "result": result,
        "mode": "action",
    }


def _validate_field(field: Dict[str, Any], value: Any) -> str | None:
    """
    Validate a single field value. Returns error message if invalid, None if valid.
    """
    field_name = field.get("name", "")
    field_type = field.get("type", "text")
    required = field.get("required", False)
    
    # Check required
    if required and (value is None or value == ""):
        return f"{field.get('label', field_name)} is required"
    
    # Skip validation if empty and not required
    if value is None or value == "":
        return None
    
    # Type-specific validation
    if field_type == "email":
        # Simple email validation
        if "@" not in str(value) or "." not in str(value).split("@")[-1]:
            return f"{field.get('label', field_name)} must be a valid email address"
    
    return None


def handle_save(
    entities: Dict[str, Dict[str, Any]],
    entity_name: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Handle save operation (insert or update).
    Validates input, applies primary key for updates, calls repository.save.
    Returns context with errors for validation failures, or success context.
    """
    entity = _get_entity(entities, entity_name)
    if not entity:
        return _missing_entity(entity_name, mode="edit")
    
    pk_name = entity.get("primary_key", "id")
    is_update = pk_name in payload and payload.get(pk_name) not in (None, "", "0", 0)
    mode = "edit" if is_update else "create"
    
    # Validate inputs
    errors = {}
    form_cfg = entity.get("form", {})
    
    for section in form_cfg.get("sections", []):
        for field in section.get("fields", []):
            field_name = field.get("name")
            if not field_name:
                continue
            
            value = payload.get(field_name)
            error_msg = _validate_field(field, value)
            if error_msg:
                errors[field_name] = error_msg
    
    # If there are validation errors, return form context with errors
    if errors:
        return {
            "ok": False,
            "status": 400,
            "entity": entity,
            "record": payload,
            "mode": mode,
            "form": form_cfg,
            "errors": errors,
            "actions": form_cfg.get("actions", []),
        }
    
    # Convert string id to int if needed
    if is_update and pk_name in payload:
        try:
            payload[pk_name] = int(payload[pk_name])
        except (ValueError, TypeError):
            pass
    
    # Whitelist payload fields to avoid unknown columns reaching the repository
    allowed_fields = {pk_name}
    for section in form_cfg.get("sections", []):
        for field in section.get("fields", []):
            name = field.get("name")
            if name:
                allowed_fields.add(name)

    filtered_payload = {k: v for k, v in payload.items() if k in allowed_fields}

    # Try to save
    try:
        saved_record = generic_repo.save(entity, filtered_payload)
    except ValueError as exc:
        # Record not found for update
        logger.warning("handle_save record not found for entity=%s: %s", entity_name, exc)
        return {
            "ok": False,
            "status": 404,
            "error": str(exc),
            "entity": entity,
            "record": filtered_payload,
            "mode": mode,
        }
    except Exception as exc:
        # Server error
        logger.exception("handle_save failed for entity=%s", entity_name)
        return _error_context(entity, mode=mode, exc=exc)
    
    # Success - return detail context
    return {
        "ok": True,
        "status": 200,
        "entity": entity,
        "record": saved_record,
        "mode": "view",
        "actions": entity.get("form", {}).get("actions", []),
    }
