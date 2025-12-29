"""
Generic service layer for YAML-defined entities.
Builds contexts for list/detail/form views and dispatches actions.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Callable

from approot.services import entities_loader
from approot.repositories import generic_repo

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
