"""
Test suite for entities_loader.py - TDD for task #1
Tests YAML loading, validation, and EntityConfig normalization.
"""
import os
import pytest
from pathlib import Path


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_load_valid_entities_yaml():
    """Test loading a valid entities.yaml file"""
    from approot.services.entities_loader import load_entities
    
    yaml_path = FIXTURES_DIR / "valid_entities.yaml"
    result = load_entities(str(yaml_path))
    
    assert result.success is True
    assert result.errors == []
    assert "customer" in result.entities
    assert "product" in result.entities


def test_entity_config_shape_for_customer():
    """Test that loaded customer entity has correct EntityConfig shape"""
    from approot.services.entities_loader import load_entities
    
    yaml_path = FIXTURES_DIR / "valid_entities.yaml"
    result = load_entities(str(yaml_path))
    
    customer = result.entities["customer"]
    assert customer["name"] == "customer"
    assert customer["table"] == "customers"
    assert customer["label"] == "Customer"
    assert customer["primary_key"] == "id"
    
    # Check list config
    assert "list" in customer
    assert isinstance(customer["list"]["columns"], list)
    assert len(customer["list"]["columns"]) == 2
    assert customer["list"]["default_sort"] == "name"
    assert customer["list"]["page_size"] == 20
    
    # Check form config
    assert "form" in customer
    assert isinstance(customer["form"]["sections"], list)
    assert len(customer["form"]["sections"]) == 1


def test_missing_required_table_field():
    """Test detection of missing required 'table' field"""
    from approot.services.entities_loader import load_entities
    
    yaml_path = FIXTURES_DIR / "missing_table.yaml"
    result = load_entities(str(yaml_path))
    
    assert result.success is False
    assert len(result.errors) > 0
    assert any("table" in err.lower() for err in result.errors)
    assert any("customer" in err for err in result.errors)


def test_missing_required_list_field():
    """Test detection of missing required 'list' field"""
    from approot.services.entities_loader import load_entities
    
    yaml_path = FIXTURES_DIR / "missing_list.yaml"
    result = load_entities(str(yaml_path))
    
    assert result.success is False
    assert len(result.errors) > 0
    assert any("list" in err.lower() for err in result.errors)


def test_invalid_type_for_columns():
    """Test detection of type mismatch (columns should be list)"""
    from approot.services.entities_loader import load_entities
    
    yaml_path = FIXTURES_DIR / "invalid_type.yaml"
    result = load_entities(str(yaml_path))
    
    assert result.success is False
    assert len(result.errors) > 0
    assert any("columns" in err.lower() and ("list" in err.lower() or "type" in err.lower()) for err in result.errors)


def test_get_entity_by_name():
    """Test get_entity function to retrieve specific entity config"""
    from approot.services.entities_loader import load_entities, get_entity
    
    yaml_path = FIXTURES_DIR / "valid_entities.yaml"
    result = load_entities(str(yaml_path))
    
    # Assuming get_entity needs the loaded result
    customer = get_entity(result.entities, "customer")
    assert customer is not None
    assert customer["name"] == "customer"
    assert customer["table"] == "customers"
    
    # Test non-existent entity
    missing = get_entity(result.entities, "nonexistent")
    assert missing is None


def test_default_primary_key():
    """Test that primary_key defaults to 'id' when not specified"""
    from approot.services.entities_loader import load_entities
    
    yaml_path = FIXTURES_DIR / "valid_entities.yaml"
    result = load_entities(str(yaml_path))
    
    product = result.entities["product"]
    # product doesn't specify primary_key in fixture, should default to "id"
    assert product.get("primary_key") == "id"


def test_entity_name_injection():
    """Test that entity name is injected into the config"""
    from approot.services.entities_loader import load_entities
    
    yaml_path = FIXTURES_DIR / "valid_entities.yaml"
    result = load_entities(str(yaml_path))
    
    # Verify each entity has its name injected
    for entity_name, entity_config in result.entities.items():
        assert entity_config["name"] == entity_name


def test_load_nonexistent_file():
    """Test handling of nonexistent YAML file"""
    from approot.services.entities_loader import load_entities
    
    result = load_entities("/nonexistent/path.yaml")
    
    assert result.success is False
    assert len(result.errors) > 0
    assert any("file" in err.lower() or "not found" in err.lower() for err in result.errors)


def test_empty_yaml_file():
    """Test handling of empty YAML file"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        # Empty file should return success with no entities
        assert result.success is True
        assert result.entities == {}
    finally:
        os.unlink(temp_path)


def test_malformed_yaml():
    """Test handling of malformed YAML syntax"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("customer:\n  table: [\ninvalid yaml")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        assert result.success is False
        assert len(result.errors) > 0
    finally:
        os.unlink(temp_path)


# Task 6: Edge cases and error paths


def test_entity_config_is_not_dict():
    """Task 6: Test entity config that is not a dictionary"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("customer: 'just a string'\nproduct: 123")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        assert result.success is False
        assert len(result.errors) >= 1
        assert any("dictionary" in err.lower() for err in result.errors)
    finally:
        os.unlink(temp_path)


def test_multiple_missing_fields():
    """Task 6: Test entity with multiple missing required fields"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("customer:\n  table: customers\n")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        assert result.success is False
        # Should have errors for missing label, list, and form
        assert len(result.errors) >= 3
        assert any("label" in err.lower() for err in result.errors)
        assert any("list" in err.lower() for err in result.errors)
        assert any("form" in err.lower() for err in result.errors)
    finally:
        os.unlink(temp_path)


def test_empty_columns_list():
    """Task 6: Test entity with empty columns list"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
customer:
  table: customers
  label: Customer
  list:
    columns: []
    page_size: 20
  form:
    sections: []
""")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        # Empty columns list is valid, loader should accept it
        assert result.success is True
        assert "customer" in result.entities
        assert result.entities["customer"]["list"]["columns"] == []
    finally:
        os.unlink(temp_path)


def test_empty_sections_list():
    """Task 6: Test entity with empty sections list"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
customer:
  table: customers
  label: Customer
  list:
    columns:
      - {name: id, label: ID}
  form:
    sections: []
""")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        # Empty sections list is valid
        assert result.success is True
        assert "customer" in result.entities
        assert result.entities["customer"]["form"]["sections"] == []
    finally:
        os.unlink(temp_path)


def test_list_config_wrong_type():
    """Task 6: Test when list config is wrong type"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
customer:
  table: customers
  label: Customer
  list: "not a dict"
  form:
    sections: []
""")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        assert result.success is False
        assert any("list" in err.lower() and "dictionary" in err.lower() for err in result.errors)
    finally:
        os.unlink(temp_path)


def test_form_config_wrong_type():
    """Task 6: Test when form config is wrong type"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
customer:
  table: customers
  label: Customer
  list:
    columns: []
  form: []
""")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        assert result.success is False
        assert any("form" in err.lower() and "dictionary" in err.lower() for err in result.errors)
    finally:
        os.unlink(temp_path)


def test_form_sections_wrong_type():
    """Task 6: Test when form.sections is wrong type"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
customer:
  table: customers
  label: Customer
  list:
    columns: []
  form:
    sections: "not a list"
""")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        assert result.success is False
        assert any("sections" in err.lower() and "list" in err.lower() for err in result.errors)
    finally:
        os.unlink(temp_path)


def test_root_yaml_is_list():
    """Task 6: Test when root YAML is a list instead of dict"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("- customer\n- product")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        assert result.success is False
        assert any("mapping" in err.lower() for err in result.errors)
    finally:
        os.unlink(temp_path)


def test_entity_with_unicode_name():
    """Task 6: Test entity with unicode characters in name"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False, encoding='utf-8') as f:
        f.write("""
customer_日本語:
  table: customers
  label: Customer
  list:
    columns:
      - {name: name, label: Name}
  form:
    sections: []
""")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        # Unicode entity names should be accepted
        assert result.success is True
        assert "customer_日本語" in result.entities
        assert result.entities["customer_日本語"]["name"] == "customer_日本語"
    finally:
        os.unlink(temp_path)


def test_mixed_valid_and_invalid_entities():
    """Task 6: Test YAML with both valid and invalid entities"""
    from approot.services.entities_loader import load_entities
    import tempfile
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
valid_entity:
  table: valid_table
  label: Valid
  list:
    columns:
      - {name: id, label: ID}
  form:
    sections: []

invalid_entity:
  table: invalid_table
  label: Invalid
  # Missing list and form

another_valid:
  table: another_table
  label: Another
  list:
    columns: []
  form:
    sections: []
""")
        temp_path = f.name
    
    try:
        result = load_entities(temp_path)
        assert result.success is False
        # Valid entities should not be included if any entity is invalid
        assert len(result.errors) >= 2  # Missing list and form
        assert any("invalid_entity" in err for err in result.errors)
    finally:
        os.unlink(temp_path)
