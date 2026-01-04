"""
YAML 定義のエンティティに共通するサービス層。
リスト・詳細・フォーム描画用のコンテキスト生成とアクションディスパッチを担う。
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
    例外から UI 表示用のエラ―コンテキストを組み立てる。
    - プール枯渇やタイムアウトは 503 で一般的な文言に丸める
    - それ以外は与えられたステータス（デフォルト 500）で返す
    詳細ログは呼び出し側で出す前提にし、ここでは情報漏えいを避けて簡潔にする。
    """

    # Default responses
    ui_error = "処理に失敗しました"
    status_code = status

    # Pool/timeout errors should surface as 503 (Service Unavailable) with a generic message
    if isinstance(exc, SQLAlchemyTimeoutError):
        status_code = 503
        ui_error = "サービスが一時的に利用できません"
    elif isinstance(exc, DBAPIError):
        # Distinguish pool exhaustion/timeout vs other DBAPI errors
        lowered = str(exc).lower()
        if "queuepool" in lowered or "timeout" in lowered or "pool" in lowered:
            status_code = 503
            ui_error = "サービスが一時的に利用できません"
        else:
            status_code = status
            ui_error = "処理に失敗しました"

    return {
        "ok": False,
        "status": status_code,
        "error": ui_error,
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
    except Exception as exc:  # pragma: no cover - 失敗経路でコンテキストを確認する
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
    except Exception as exc:  # pragma: no cover - 失敗時もログを残す
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
        except Exception as exc:  # pragma: no cover - 失敗時もログを残す
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
    単一フィールドをバリデートし、不正ならエラーメッセージを返す。問題なければ None。
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
    
    # 型ごとのバリデーション
    if field_type == "email":
        # 簡易なメールアドレスチェック
        if "@" not in str(value) or "." not in str(value).split("@")[-1]:
            return f"{field.get('label', field_name)} must be a valid email address"
    
    return None


def handle_save(
    entities: Dict[str, Dict[str, Any]],
    entity_name: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    保存処理（新規/更新）を担当する。
    入力バリデーションを行い、更新時は主キーを適用して repository.save を呼ぶ。
    バリデーション失敗時はフォーム用コンテキストを返し、成功時は詳細表示用コンテキストを返す。
    """
    entity = _get_entity(entities, entity_name)
    if not entity:
        return _missing_entity(entity_name, mode="edit")
    
    pk_name = entity.get("primary_key", "id")
    is_update = pk_name in payload and payload.get(pk_name) not in (None, "", "0", 0)
    mode = "edit" if is_update else "create"
    
    # 入力検証
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
    
    # バリデーションに失敗したらフォームコンテキストを返す
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
    
    # 文字列の主キーは int に寄せる（失敗しても致命的ではない）
    if is_update and pk_name in payload:
        try:
            payload[pk_name] = int(payload[pk_name])
        except (ValueError, TypeError):
            pass
    
    # 許可したフィールドだけを渡し、未知のカラムをリポジトリへ渡さない
    allowed_fields = {pk_name}
    for section in form_cfg.get("sections", []):
        for field in section.get("fields", []):
            name = field.get("name")
            if name:
                allowed_fields.add(name)

    filtered_payload = {k: v for k, v in payload.items() if k in allowed_fields}

    # リポジトリへ保存を委譲
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
        # 想定外のサーバーエラーは共通コンテキストで返却
        logger.exception("handle_save failed for entity=%s", entity_name)
        return _error_context(entity, mode=mode, exc=exc)
    
    # 成功時は詳細表示コンテキストを返す
    return {
        "ok": True,
        "status": 200,
        "entity": entity,
        "record": saved_record,
        "mode": "view",
        "actions": entity.get("form", {}).get("actions", []),
    }
