"""
YAML 定義のエンティティ設定を読み込み、簡易バリデーションと正規化を行うユーティリティ。
config/entities.yaml を読み取り EntityConfig 形式に整える。
"""
import os
from typing import Dict, List, Any, Optional
import yaml


class ValidationResult:
    """バリデーションの結果を運ぶシンプルなコンテナ。"""
    def __init__(self, success: bool = True, entities: Optional[Dict] = None, errors: Optional[List[str]] = None):
        self.success = success
        self.entities = entities if entities is not None else {}
        self.errors = errors if errors is not None else []


def load_entities(yaml_path: str) -> ValidationResult:
    """
    YAML からエンティティ定義を読み込み、必須項目の有無などを検証する。

    Args:
        yaml_path: entities.yaml へのパス

    Returns:
        ValidationResult: 成否フラグとロード済みエンティティ、エラーリストを含む
    """
    errors = []
    
    # Check file exists
    if not os.path.exists(yaml_path):
        return ValidationResult(
            success=False,
            errors=[f"File not found: {yaml_path}"]
        )
    
    # Load YAML
    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            raw_data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        return ValidationResult(
            success=False,
            errors=[f"YAML parse error: {str(e)}"]
        )
    except Exception as e:
        return ValidationResult(
            success=False,
            errors=[f"Error reading file: {str(e)}"]
        )
    
    # Handle empty file or non-mapping root
    if raw_data is None:
        return ValidationResult(success=True, entities={})
    if not isinstance(raw_data, dict):
        return ValidationResult(
            success=False,
            errors=["Root of YAML must be a mapping of entity names to configs"]
        )
    
    # Validate and normalize each entity
    entities = {}
    for entity_name, entity_config in raw_data.items():
        entity_errors = _validate_entity(entity_name, entity_config)
        if entity_errors:
            errors.extend(entity_errors)
        else:
            # Normalize and inject name
            normalized = _normalize_entity(entity_name, entity_config)
            entities[entity_name] = normalized
    
    if errors:
        return ValidationResult(success=False, errors=errors)
    
    return ValidationResult(success=True, entities=entities)


def _validate_entity(name: str, config: Any) -> List[str]:
    """エンティティ設定 1 件を検証し、問題があればメッセージを返す。"""
    errors = []
    
    if not isinstance(config, dict):
        return [f"Entity '{name}': configuration must be a dictionary"]
    
    # Required fields
    required_fields = ['table', 'label', 'list', 'form']
    for field in required_fields:
        if field not in config:
            errors.append(f"Entity '{name}': missing required field '{field}'")
    
    if errors:
        return errors
    
    # Validate list config
    list_config = config.get('list')
    if not isinstance(list_config, dict):
        errors.append(f"Entity '{name}': 'list' must be a dictionary")
    else:
        if 'columns' not in list_config:
            errors.append(f"Entity '{name}': 'list.columns' is required")
        elif not isinstance(list_config['columns'], list):
            errors.append(f"Entity '{name}': 'list.columns' must be a list")
    
    # Validate form config
    form_config = config.get('form')
    if not isinstance(form_config, dict):
        errors.append(f"Entity '{name}': 'form' must be a dictionary")
    else:
        if 'sections' not in form_config:
            errors.append(f"Entity '{name}': 'form.sections' is required")
        elif not isinstance(form_config['sections'], list):
            errors.append(f"Entity '{name}': 'form.sections' must be a list")
    
    return errors


def _normalize_entity(name: str, config: Dict) -> Dict:
    """エンティティ設定を内部で扱いやすい形に正規化する。"""
    normalized = {
        'name': name,
        'table': config['table'],
        'label': config['label'],
        'primary_key': config.get('primary_key', 'id'),
        'list': config['list'],
        'form': config['form']
    }
    return normalized


def get_entity(entities: Dict[str, Dict], name: str) -> Optional[Dict]:
    """
    ロード済みエンティティ辞書から名前で 1 件取得する。

    Args:
        entities: ロード済みエンティティの辞書
        name: 取得したいエンティティ名

    Returns:
        見つかった設定 dict。存在しない場合は None。
    """
    return entities.get(name)
