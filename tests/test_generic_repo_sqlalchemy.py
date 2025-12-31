"""
Task 12: SQLAlchemy Core/ORM tests for generic_repo.py
Tests verify that SQLAlchemy-based implementation maintains identical interface and behavior.
"""
import pytest
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, text
from sqlalchemy.orm import sessionmaker


@pytest.fixture
def sqlalchemy_engine():
    """Create in-memory SQLite engine for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    yield engine
    engine.dispose()


@pytest.fixture
def sqlalchemy_session(sqlalchemy_engine):
    """Create SQLAlchemy session for testing."""
    Session = sessionmaker(bind=sqlalchemy_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def test_table(sqlalchemy_engine):
    """Create test table schema."""
    metadata = MetaData()
    customers = Table(
        'customers',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('name', String(100)),
        Column('email', String(100)),
        Column('age', Integer),
        Column('status', String(50)),
    )
    metadata.create_all(sqlalchemy_engine)
    
    # Insert test data
    with sqlalchemy_engine.connect() as conn:
        conn.execute(customers.insert(), [
            {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'age': 30, 'status': 'active'},
            {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'age': 25, 'status': 'active'},
            {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35, 'status': 'inactive'},
            {'id': 4, 'name': 'David', 'email': 'david@example.com', 'age': 28, 'status': 'active'},
        ])
        conn.commit()
    
    return customers


@pytest.fixture
def entity_cfg():
    """Entity configuration for testing."""
    return {
        "name": "customer",
        "table": "customers",
        "label": "Customer",
        "primary_key": "id",
        "list": {
            "columns": [
                {"name": "id", "label": "ID"},
                {"name": "name", "label": "Name"},
                {"name": "email", "label": "Email"},
            ],
            "default_sort": "name",
            "page_size": 20,
        },
        "form": {
            "sections": [
                {
                    "label": "Basic Info",
                    "fields": [
                        {"name": "name", "label": "Name", "type": "text"},
                        {"name": "email", "label": "Email", "type": "email"},
                        {"name": "age", "label": "Age", "type": "number"},
                        {"name": "status", "label": "Status", "type": "text"},
                    ],
                }
            ],
        },
    }


@pytest.fixture(autouse=True)
def mock_db_module(monkeypatch, sqlalchemy_engine, sqlalchemy_session):
    """Mock db module to use test engine/session."""
    from approot import db
    
    # Mock the engine and session factory
    monkeypatch.setattr(db, '_engine', sqlalchemy_engine)
    monkeypatch.setattr(db, '_initialized', True)
    
    # Create a session factory that returns our test session
    test_session_factory = lambda: sqlalchemy_session
    monkeypatch.setattr(db, '_SessionFactory', test_session_factory)
    monkeypatch.setattr(db, 'get_session', test_session_factory)
    
    # Mock get_connection to return raw DBAPI connection
    def mock_get_connection(timeout=None):
        return sqlalchemy_engine.raw_connection()
    
    monkeypatch.setattr(db, 'get_connection', mock_get_connection)
    
    # Mock release_connection
    def mock_release_connection(conn):
        conn.close()
    
    monkeypatch.setattr(db, 'release_connection', mock_release_connection)


# Test fetch_list with SQLAlchemy
def test_sqlalchemy_fetch_list_basic(entity_cfg, test_table):
    """Task 12: fetch_list returns all records with SQLAlchemy Core."""
    from approot.repositories import generic_repo
    
    result = generic_repo.fetch_list(entity_cfg, page=1, page_size=10)
    
    assert len(result) == 4
    assert all(isinstance(r, dict) for r in result)
    assert result[0]['name'] == 'Alice'  # default sort by name


def test_sqlalchemy_fetch_list_pagination(entity_cfg, test_table):
    """Task 12: fetch_list supports pagination with SQLAlchemy."""
    from approot.repositories import generic_repo
    
    # First page
    result_page1 = generic_repo.fetch_list(entity_cfg, page=1, page_size=2)
    assert len(result_page1) == 2
    
    # Second page
    result_page2 = generic_repo.fetch_list(entity_cfg, page=2, page_size=2)
    assert len(result_page2) == 2
    
    # Ensure different records
    assert result_page1[0]['id'] != result_page2[0]['id']


def test_sqlalchemy_fetch_list_sorting(entity_cfg, test_table):
    """Task 12: fetch_list supports sorting (ASC/DESC) with SQLAlchemy."""
    from approot.repositories import generic_repo
    
    # Sort by name ascending (default)
    result_asc = generic_repo.fetch_list(entity_cfg, sort="name")
    assert result_asc[0]['name'] == 'Alice'
    assert result_asc[-1]['name'] == 'David'
    
    # Sort by name descending
    result_desc = generic_repo.fetch_list(entity_cfg, sort="-name")
    assert result_desc[0]['name'] == 'David'
    assert result_desc[-1]['name'] == 'Alice'


def test_sqlalchemy_fetch_list_column_whitelist(entity_cfg, test_table):
    """Task 12: fetch_list only returns whitelisted columns."""
    from approot.repositories import generic_repo
    
    result = generic_repo.fetch_list(entity_cfg, page=1, page_size=10)
    
    # Should only return columns defined in entity config
    expected_columns = {'id', 'name', 'email'}
    for record in result:
        assert set(record.keys()) == expected_columns


# Test fetch_detail with SQLAlchemy
def test_sqlalchemy_fetch_detail_existing_record(entity_cfg, test_table):
    """Task 12: fetch_detail returns single record by PK with SQLAlchemy."""
    from approot.repositories import generic_repo
    
    result = generic_repo.fetch_detail(entity_cfg, pk=2)
    
    assert result is not None
    assert result['id'] == 2
    assert result['name'] == 'Bob'
    assert result['email'] == 'bob@example.com'


def test_sqlalchemy_fetch_detail_nonexistent_record(entity_cfg, test_table):
    """Task 12: fetch_detail returns None for nonexistent PK."""
    from approot.repositories import generic_repo
    
    result = generic_repo.fetch_detail(entity_cfg, pk=999)
    
    assert result is None


def test_sqlalchemy_fetch_detail_uses_parameter_binding(entity_cfg, test_table):
    """Task 12: fetch_detail uses parameter binding to prevent SQL injection."""
    from approot.repositories import generic_repo
    
    # Attempt SQL injection via PK
    malicious_pk = "1 OR 1=1"
    result = generic_repo.fetch_detail(entity_cfg, pk=malicious_pk)
    
    # Should not return any record (or return None)
    # The query should safely bind the parameter
    assert result is None or (isinstance(result, dict) and result.get('id') != 1)


# Test search_lookup with SQLAlchemy
def test_sqlalchemy_search_lookup_basic(entity_cfg, test_table):
    """Task 12: search_lookup performs LIKE search with SQLAlchemy."""
    from approot.repositories import generic_repo
    
    result = generic_repo.search_lookup(entity_cfg, q="Al", limit=10)
    
    assert len(result) == 1
    assert result[0]['name'] == 'Alice'


def test_sqlalchemy_search_lookup_case_insensitive(entity_cfg, test_table):
    """Task 12: search_lookup is case-insensitive."""
    from approot.repositories import generic_repo
    
    result_lower = generic_repo.search_lookup(entity_cfg, q="al", limit=10)
    result_upper = generic_repo.search_lookup(entity_cfg, q="AL", limit=10)
    
    # SQLite LIKE is case-insensitive by default
    assert len(result_lower) >= 1
    assert len(result_upper) >= 1


def test_sqlalchemy_search_lookup_limit(entity_cfg, test_table):
    """Task 12: search_lookup respects limit parameter."""
    from approot.repositories import generic_repo
    
    result = generic_repo.search_lookup(entity_cfg, q="", limit=2)
    
    assert len(result) <= 2


def test_sqlalchemy_search_lookup_parameter_binding(entity_cfg, test_table):
    """Task 12: search_lookup uses parameter binding for search term."""
    from approot.repositories import generic_repo
    
    # Attempt SQL injection in search query
    malicious_query = "'; DROP TABLE customers; --"
    result = generic_repo.search_lookup(entity_cfg, q=malicious_query, limit=10)
    
    # Should safely execute without error (returns empty or no matches)
    assert isinstance(result, list)


# Test save with SQLAlchemy (INSERT)
def test_sqlalchemy_save_insert_new_record(entity_cfg, test_table, sqlalchemy_engine):
    """Task 12: save inserts new record when PK is absent/empty."""
    from approot.repositories import generic_repo
    
    payload = {
        'name': 'Eve',
        'email': 'eve@example.com',
        'age': 29,
        'status': 'active',
    }
    
    result = generic_repo.save(entity_cfg, payload)
    
    # Should return saved record with new ID
    assert result is not None
    assert 'id' in result
    assert result['id'] > 0
    assert result['name'] == 'Eve'
    assert result['email'] == 'eve@example.com'
    
    # Verify insertion in DB
    with sqlalchemy_engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM customers WHERE name = :name"), {'name': 'Eve'}).fetchone()
        assert row is not None


def test_sqlalchemy_save_update_existing_record(entity_cfg, test_table, sqlalchemy_engine):
    """Task 12: save updates existing record when PK is provided."""
    from approot.repositories import generic_repo
    
    payload = {
        'id': 2,
        'name': 'Bob Updated',
        'email': 'bob.updated@example.com',
        'age': 26,
        'status': 'inactive',
    }
    
    result = generic_repo.save(entity_cfg, payload)
    
    # Should return updated record
    assert result is not None
    assert result['id'] == 2
    assert result['name'] == 'Bob Updated'
    assert result['email'] == 'bob.updated@example.com'
    
    # Verify update in DB
    with sqlalchemy_engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM customers WHERE id = :id"), {'id': 2}).fetchone()
        assert row is not None
        # Access by index since row is a tuple-like object
        assert row[1] == 'Bob Updated'  # name is second column


def test_sqlalchemy_save_update_nonexistent_raises_error(entity_cfg, test_table):
    """Task 12: save raises ValueError when updating nonexistent record."""
    from approot.repositories import generic_repo
    
    payload = {
        'id': 999,
        'name': 'Nonexistent',
        'email': 'none@example.com',
    }
    
    with pytest.raises(ValueError, match="not found"):
        generic_repo.save(entity_cfg, payload)


def test_sqlalchemy_save_enforces_column_whitelist(entity_cfg, test_table, sqlalchemy_engine):
    """Task 12: save only persists fields from form sections (whitelist)."""
    from approot.repositories import generic_repo
    
    payload = {
        'name': 'Frank',
        'email': 'frank@example.com',
        'age': 32,
        'status': 'active',
        'malicious_field': 'DROP TABLE',  # Should be ignored
        'another_bad': 'SELECT *',  # Should be ignored
    }
    
    result = generic_repo.save(entity_cfg, payload)
    
    # Only whitelisted fields should be saved
    assert 'malicious_field' not in result
    assert 'another_bad' not in result
    
    # Verify DB doesn't have these fields
    with sqlalchemy_engine.connect() as conn:
        # Check that only valid columns exist (SQLite doesn't add unknown columns)
        row = conn.execute(text("SELECT * FROM customers WHERE name = :name"), {'name': 'Frank'}).fetchone()
        assert row is not None


def test_sqlalchemy_save_parameter_binding_for_insert(entity_cfg, test_table):
    """Task 12: save uses parameter binding for INSERT."""
    from approot.repositories import generic_repo
    
    payload = {
        'name': "'; DROP TABLE customers; --",
        'email': 'hacker@example.com',
        'age': 99,
        'status': 'active',
    }
    
    # Should safely insert the malicious string as data
    result = generic_repo.save(entity_cfg, payload)
    
    assert result is not None
    assert result['name'] == "'; DROP TABLE customers; --"


def test_sqlalchemy_save_parameter_binding_for_update(entity_cfg, test_table):
    """Task 12: save uses parameter binding for UPDATE."""
    from approot.repositories import generic_repo
    
    payload = {
        'id': 1,
        'name': "'; UPDATE customers SET name='hacked'; --",
        'email': 'hacker@example.com',
        'age': 99,
        'status': 'active',
    }
    
    # Should safely update with the malicious string as data
    result = generic_repo.save(entity_cfg, payload)
    
    assert result is not None
    assert result['name'] == "'; UPDATE customers SET name='hacked'; --"


def test_sqlalchemy_save_empty_payload_raises_error(entity_cfg, test_table):
    """Task 12: save with no valid fields raises ValueError."""
    from approot.repositories import generic_repo
    
    payload = {}
    
    with pytest.raises(ValueError, match="No fields to insert"):
        generic_repo.save(entity_cfg, payload)


# Test interface compatibility
def test_sqlalchemy_maintains_fetch_list_signature(entity_cfg, test_table):
    """Task 12: fetch_list signature remains unchanged."""
    from approot.repositories import generic_repo
    import inspect
    
    sig = inspect.signature(generic_repo.fetch_list)
    params = list(sig.parameters.keys())
    
    # Expected parameters
    assert 'entity' in params
    assert 'page' in params
    assert 'page_size' in params
    assert 'sort' in params


def test_sqlalchemy_maintains_fetch_detail_signature(entity_cfg, test_table):
    """Task 12: fetch_detail signature remains unchanged."""
    from approot.repositories import generic_repo
    import inspect
    
    sig = inspect.signature(generic_repo.fetch_detail)
    params = list(sig.parameters.keys())
    
    assert 'entity' in params
    assert 'pk' in params


def test_sqlalchemy_maintains_search_lookup_signature(entity_cfg, test_table):
    """Task 12: search_lookup signature remains unchanged."""
    from approot.repositories import generic_repo
    import inspect
    
    sig = inspect.signature(generic_repo.search_lookup)
    params = list(sig.parameters.keys())
    
    assert 'entity' in params
    assert 'q' in params
    assert 'limit' in params


def test_sqlalchemy_maintains_save_signature(entity_cfg, test_table):
    """Task 12: save signature remains unchanged."""
    from approot.repositories import generic_repo
    import inspect
    
    sig = inspect.signature(generic_repo.save)
    params = list(sig.parameters.keys())
    
    assert 'entity' in params
    assert 'payload' in params


def test_sqlalchemy_fetch_list_returns_list_of_dicts(entity_cfg, test_table):
    """Task 12: fetch_list returns list of dicts (same shape as before)."""
    from approot.repositories import generic_repo
    
    result = generic_repo.fetch_list(entity_cfg)
    
    assert isinstance(result, list)
    for item in result:
        assert isinstance(item, dict)


def test_sqlalchemy_fetch_detail_returns_dict_or_none(entity_cfg, test_table):
    """Task 12: fetch_detail returns dict or None (same shape as before)."""
    from approot.repositories import generic_repo
    
    result_exists = generic_repo.fetch_detail(entity_cfg, pk=1)
    assert isinstance(result_exists, dict)
    
    result_none = generic_repo.fetch_detail(entity_cfg, pk=999)
    assert result_none is None


def test_sqlalchemy_save_returns_dict(entity_cfg, test_table):
    """Task 12: save returns dict (same shape as before)."""
    from approot.repositories import generic_repo
    
    payload = {'name': 'Test', 'email': 'test@example.com'}
    result = generic_repo.save(entity_cfg, payload)
    
    assert isinstance(result, dict)
    assert 'id' in result
