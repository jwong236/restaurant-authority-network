import pytest
import psycopg2
from unittest.mock import MagicMock
from database.db_connector import get_db_connection
from database.db_operations import (
    check_domain_exists,
    insert_domain,
    update_domain_visit_count,
    check_source_exists,
    insert_source,
    check_url_exists,
    update_last_crawled,
    insert_url,
    insert_into_priority_queue,
)


@pytest.fixture
def db_connection():
    """Fixture to set up and tear down a test database connection."""
    conn = get_db_connection()
    yield conn
    conn.close()


@pytest.fixture
def mock_cursor():
    """Fixture to provide a mock database cursor."""
    return MagicMock()


def test_check_domain_exists(mock_cursor):
    """Test if the function correctly retrieves an existing domain ID."""
    mock_cursor.fetchone.return_value = (1,)
    domain_id = check_domain_exists("example.com", mock_cursor)
    assert domain_id == 1
    mock_cursor.execute.assert_called_once_with(
        "SELECT id FROM domain WHERE domain_name = %s", ("example.com",)
    )


def test_check_domain_exists_not_found(mock_cursor):
    """Test if the function returns None for a non-existing domain."""
    mock_cursor.fetchone.return_value = None
    domain_id = check_domain_exists("nonexistent.com", mock_cursor)
    assert domain_id is None


def test_insert_domain(mock_cursor):
    """Test inserting a new domain."""
    mock_cursor.fetchone.return_value = (1,)
    domain_id = insert_domain("example.com", mock_cursor)
    assert domain_id == 1
    mock_cursor.execute.assert_called_once_with(
        "INSERT INTO domain (domain_name, visit_count) VALUES (%s, 1) RETURNING id",
        ("example.com",),
    )


def test_update_domain_visit_count(mock_cursor):
    """Test updating the visit count of a domain."""
    update_domain_visit_count(1, mock_cursor)
    mock_cursor.execute.assert_called_once_with(
        "UPDATE domain SET visit_count = visit_count + 1 WHERE id = %s", (1,)
    )


def test_check_source_exists(mock_cursor):
    """Test checking if a source exists."""
    mock_cursor.fetchone.return_value = (1,)
    exists = check_source_exists(1, mock_cursor)
    assert exists is True


def test_check_source_not_exists(mock_cursor):
    """Test checking if a source does not exist."""
    mock_cursor.fetchone.return_value = None
    exists = check_source_exists(1, mock_cursor)
    assert exists is False


def test_insert_source(mock_cursor):
    """Test inserting a source and getting it's source_id."""
    mock_cursor.fetchone.return_value = (10,)
    source_id = insert_source(10, 0.9, mock_cursor)
    mock_cursor.execute.assert_called_once_with(
        """
        INSERT INTO source (domain_id, source_type, credibility_score)
        VALUES (%s, 'news', %s)
        ON CONFLICT (domain_id, source_type) DO NOTHING
        RETURNING id
        """,
        (10, 0.9),
    )
    assert source_id == 10


def test_check_url_exists(mock_cursor):
    """Test checking if a URL exists."""
    mock_cursor.fetchone.return_value = (1,)
    url_id = check_url_exists("http://example.com", mock_cursor)
    assert url_id == 1


def test_check_url_not_exists(mock_cursor):
    """Test checking if a URL does not exist."""
    mock_cursor.fetchone.return_value = None
    url_id = check_url_exists("http://notfound.com", mock_cursor)
    assert url_id is None


def test_update_last_crawled(mock_cursor):
    """Test updating the last crawled timestamp."""
    update_last_crawled(1, mock_cursor)
    mock_cursor.execute.assert_called_once_with(
        "UPDATE url SET last_crawled = NOW() WHERE id = %s", (1,)
    )


def test_insert_url(mock_cursor):
    """Test inserting a new URL."""
    mock_cursor.fetchone.return_value = (1,)
    url_id = insert_url("http://example.com", 1, mock_cursor)
    assert url_id == 1
    mock_cursor.execute.assert_called_once_with(
        """
        INSERT INTO url (full_url, source_id, first_seen, last_crawled)
        VALUES (%s, %s, NOW(), NOW()) RETURNING id
        """,
        ("http://example.com", 1),
    )


def test_insert_into_priority_queue(mock_cursor):
    """Test inserting into the priority queue."""
    insert_into_priority_queue(1, 10, mock_cursor)
    mock_cursor.execute.assert_called_once_with(
        """
        INSERT INTO priority_queue (url_id, priority, queued_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (url_id) DO UPDATE SET priority = EXCLUDED.priority
        """,
        (1, 10),
    )
