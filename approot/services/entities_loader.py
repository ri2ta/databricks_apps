"""
Entity loader and schema validator for YAML-driven CRUD framework.
Loads config/entities.yaml and validates/normalizes to EntityConfig shape.
"""
import os
from typing import Dict, List, Any, Optional
import yaml


class ValidationResult:
    """Result object for validation operations"""
    def __init__(self, success: bool = True, entities: Optional[Dict] = None, errors: Optional[List[str]] = None):
        self.success = success
        self.entities = entities if entities is not None else {}
        self.errors = errors if errors is not None else []


def load_entities(yaml_path: str) -> ValidationResult:
    """
    Load and validate entities from YAML file.
    
    Args:
        yaml_path: Path to entities.yaml file
        
    Returns:
        ValidationResult with success flag, entities dict, and any errors
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
    """Validate a single entity configuration"""
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
    """Normalize entity config to EntityConfig shape"""
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
    Get a specific entity by name from loaded entities.
    
    Args:
        entities: Dictionary of loaded entities
        name: Entity name to retrieve
        
    Returns:
        Entity config dict or None if not found
    """
    return entities.get(name)
