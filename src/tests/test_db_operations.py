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
    get_priority_queue_url,
    update_priority_queue_url,
    remove_priority_queue_url,
    check_restaurant_exists,
    fuzzy_search_restaurant_name,
    insert_reference,
    insert_restaurant,
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
        VALUES (%s, 'webpage', %s)
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


def test_get_priority_queue_url(mock_cursor):
    """
    Test fetching the highest priority URL.
    """
    # Mock database return value (URL, Priority)
    mock_cursor.fetchone.return_value = ("https://example.com", 5)

    result = get_priority_queue_url(mock_cursor)

    assert result == (
        "https://example.com",
        5,
    ), "Should return the highest priority URL"
    mock_cursor.execute.assert_called_once()


def test_get_priority_queue_url_empty(mock_cursor):
    """
    Test when no URLs exist in the priority queue.
    """
    # Simulate an empty queue
    mock_cursor.fetchone.return_value = None

    result = get_priority_queue_url(mock_cursor)

    assert result is None, "Should return None if queue is empty"
    mock_cursor.execute.assert_called_once()


def test_update_priority_queue_url_success(mock_cursor):
    """
    Test updating priority when the URL exists.
    """
    mock_cursor.fetchone.return_value = (1,)

    update_priority_queue_url("https://example.com", 10, mock_cursor)

    expected_query = "UPDATE priority_queue\n            SET priority = %s\n            WHERE url_id = %s"
    executed_queries = [
        call[0][0].strip() for call in mock_cursor.execute.call_args_list
    ]
    print(executed_queries)
    assert (
        expected_query in executed_queries
    ), "SQL UPDATE query not found in executed calls"


def test_update_priority_queue_url_not_found(mock_cursor, capsys):
    """
    Test updating priority when the URL does not exist.
    """
    mock_cursor.fetchone.return_value = None

    update_priority_queue_url("https://example.com", 10, mock_cursor)

    captured = capsys.readouterr()
    assert "⚠️ URL not found in database: https://example.com" in captured.out
    mock_cursor.execute.assert_any_call(
        "SELECT id FROM url WHERE full_url = %s", ("https://example.com",)
    )
    mock_cursor.execute.assert_called_once()


def test_remove_priority_queue_url_success(mock_cursor):
    """
    Test removing a URL from the priority queue when it exists.
    """
    mock_cursor.fetchone.return_value = (1,)

    remove_priority_queue_url("https://example.com", mock_cursor)

    mock_cursor.execute.assert_any_call(
        "SELECT id FROM url WHERE full_url = %s", ("https://example.com",)
    )
    mock_cursor.execute.assert_any_call(
        "DELETE FROM priority_queue WHERE url_id = %s", (1,)
    )


def test_remove_priority_queue_url_not_found(mock_cursor, capsys):
    """
    Test removing a URL when it's not found in the database.
    """
    mock_cursor.fetchone.return_value = None

    remove_priority_queue_url("https://example.com", mock_cursor)

    captured = capsys.readouterr()
    assert "URL not found in database: https://example.com" in captured.out
    mock_cursor.execute.assert_any_call(
        "SELECT id FROM url WHERE full_url = %s", ("https://example.com",)
    )
    mock_cursor.execute.assert_called_once()


@pytest.mark.parametrize(
    "mock_result, expected_output",
    [
        ((5,), 5),
        (None, None),
    ],
)
def test_check_restaurant_exists(mock_result, expected_output):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = mock_result

    result = check_restaurant_exists("Hironori", mock_cursor)

    assert result == expected_output
    mock_cursor.execute.assert_called_once_with(
        "SELECT id FROM restaurant WHERE name = %s", ("Hironori",)
    )


@pytest.mark.parametrize(
    "mock_result, expected_output",
    [
        (
            (10, "Hironori", "Irvine, CA", 0.92),
            {"id": 10, "name": "Hironori", "address": "Irvine, CA", "confidence": 0.92},
        ),
        (None, None),
    ],
)
def test_fuzzy_search_restaurant_name(mock_result, expected_output):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = mock_result

    result = fuzzy_search_restaurant_name("Hironori", mock_cursor)

    assert result == expected_output
    mock_cursor.execute.assert_called_once_with(
        "SELECT id, name, address, confidence FROM fuzzy_search_restaurant_name(%s)",
        ("Hironori",),
    )


def test_fuzzy_search_real(db_connection):
    """Test fuzzy_search_restaurant_name using the actual PostgreSQL stored procedure."""
    cur = db_connection.cursor()

    # Step 1: Insert a fake test restaurant
    test_name = "FakeTestRestaurant12345"
    test_address = "123 Fake Street, Nowhere, XY"

    cur.execute(
        "INSERT INTO restaurant (name, address) VALUES (%s, %s) RETURNING id;",
        (test_name, test_address),
    )
    test_id = cur.fetchone()[0]
    db_connection.commit()

    try:
        # Step 2: Run the fuzzy search on the fake restaurant
        result = fuzzy_search_restaurant_name(test_name, cur)

        # Step 3: Validate the result
        assert result is not None, "No restaurant found, but one was inserted"
        assert result["id"] == test_id, "ID mismatch"
        assert result["name"] == test_name, "Name mismatch"
        assert "confidence" in result, "Confidence score missing"

    finally:
        # Step 4: Cleanup - Remove the fake test restaurant
        cur.execute("DELETE FROM restaurant WHERE id = %s;", (test_id,))
        db_connection.commit()
        cur.close()


def test_insert_restaurant():
    """
    Tests insert_restaurant() by verifying the correct SQL query is executed
    and that the returned restaurant ID is as expected.
    """
    mock_cur = MagicMock()
    mock_cur.fetchone.return_value = (123,)

    restaurant_id = insert_restaurant("Joe's Pizza", "123 Main St", mock_cur)

    mock_cur.execute.assert_called_once_with(
        """
        INSERT INTO restaurant (name, address)
        VALUES (%s, %s)
        RETURNING id
        """,
        ("Joe's Pizza", "123 Main St"),
    )

    assert restaurant_id == 123, f"Expected restaurant ID 123, got {restaurant_id}"


def test_insert_reference():
    """
    Tests insert_reference() by verifying the correct SQL query is executed.
    """
    mock_cur = MagicMock()
    insert_reference(456, 789, mock_cur)

    mock_cur.execute.assert_called_once_with(
        """
        INSERT INTO reference (restaurant_id, url_id)
        VALUES (%s, %s)
        ON CONFLICT (restaurant_id, url_id) DO NOTHING
        """,
        (456, 789),
    )
