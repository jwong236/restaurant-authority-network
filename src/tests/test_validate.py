import pytest
from unittest.mock import patch
from pipeline.validate import (
    normalize_url,
    calculate_url_score,
    calculate_priority_score,
    validate_url,
)
from database.db_operations import (
    check_url_exists,
    check_domain_exists,
    check_source_exists,
)
from database.db_connector import get_db_connection


@pytest.fixture(scope="function")
def setup_test_database():
    """
    Fixture to set up and tear down a test database.
    Creates a fresh test environment before each test.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Clean database before test
    cur.execute("DELETE FROM priority_queue")
    cur.execute("DELETE FROM reference")
    cur.execute("DELETE FROM url")
    cur.execute("DELETE FROM restaurant")
    cur.execute("DELETE FROM source")
    cur.execute("DELETE FROM domain")

    conn.commit()
    yield conn, cur  # Provide connection and cursor for test cases
    cur.close()
    conn.close()


def test_normalize_url():
    """Test that normalize_url correctly removes query parameters and fragments."""
    assert (
        normalize_url("https://example.com/page?query=123")
        == "https://example.com/page"
    )
    assert (
        normalize_url("https://example.com/page#section") == "https://example.com/page"
    )
    assert (
        normalize_url("https://example.com/page?query=123#section")
        == "https://example.com/page"
    )


@pytest.mark.parametrize(
    "url,expected_score",
    [
        ("https://example.com", 0.5),
        ("https://example.org", 1.0),
        ("https://news.com", 1.0),
        ("https://weeklydigest.com", 0.8),
        ("https://blog.com", 0.5),
    ],
)
def test_calculate_url_score(url, expected_score):
    """Test URL scoring mechanism based on keywords and domain type."""
    assert calculate_url_score(url) == expected_score


@pytest.mark.parametrize(
    "url_score,relevance_score,expected_priority",
    [
        (1.0, 1.0, 1.0),
        (0.5, 1.0, 0.825),
        (0.0, 1.0, 0.65),
        (0.5, 0.5, 0.575),
        (1.0, 0.0, 0.35),
    ],
)
def test_calculate_priority_score(url_score, relevance_score, expected_priority):
    """Test priority calculation based on URL score and relevance score."""
    assert calculate_priority_score(url_score, relevance_score) == pytest.approx(
        expected_priority, rel=1e-2
    )


def test_validate_url_new_entry(setup_test_database):
    """Test inserting a new URL and associated data into the database."""
    conn, cur = setup_test_database
    url = "https://example.com/review"
    relevance_score = 0.9

    validate_url(url, relevance_score)

    # Verify domain exists
    cur.execute("SELECT id FROM domain WHERE domain_name = 'example.com'")
    domain_id = cur.fetchone()
    assert domain_id is not None, "Domain should be inserted"

    # Verify URL exists
    cur.execute("SELECT id FROM url WHERE full_url = %s", (url,))
    url_id = cur.fetchone()
    assert url_id is not None, "URL should be inserted"

    # Verify priority queue entry
    cur.execute("SELECT priority FROM priority_queue WHERE url_id = %s", (url_id,))
    priority = cur.fetchone()
    assert priority is not None, "URL should be added to priority queue"


def test_validate_url_existing_url(setup_test_database):
    """Test that if a URL already exists, it updates `last_crawled` instead of inserting a new entry."""
    conn, cur = setup_test_database
    url = "https://example.com/existing"
    relevance_score = 0.7

    # Manually insert URL
    cur.execute("INSERT INTO domain (domain_name) VALUES ('example.com') RETURNING id")
    domain_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO url (source_id, full_url) VALUES (%s, %s) RETURNING id",
        (domain_id, url),
    )
    url_id = cur.fetchone()[0]
    conn.commit()

    # Validate URL
    validate_url(url, relevance_score)

    # Ensure last_crawled is updated
    cur.execute("SELECT last_crawled FROM url WHERE id = %s", (url_id,))
    last_crawled = cur.fetchone()[0]
    assert last_crawled is not None, "last_crawled should be updated"


@patch("database.db_operations.insert_url")
@patch("database.db_operations.insert_into_priority_queue")
def test_validate_url_error_handling(
    mock_insert_url, mock_priority_queue, setup_test_database
):
    """Test error handling during database failures."""
    mock_insert_url.side_effect = Exception("DB Error")
    conn, cur = setup_test_database

    with pytest.raises(Exception, match="DB Error"):
        validate_url("https://error.com", 0.6)

    # Ensure priority queue was never updated due to failure
    mock_priority_queue.assert_not_called()
