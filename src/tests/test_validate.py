import pytest
from unittest.mock import patch
from pipeline.validate import (
    normalize_url,
    calculate_url_score,
    calculate_priority_score,
    validate_url,
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

    cur.execute("DELETE FROM priority_queue")
    cur.execute("DELETE FROM reference")
    cur.execute("DELETE FROM url")
    cur.execute("DELETE FROM restaurant")
    cur.execute("DELETE FROM source")
    cur.execute("DELETE FROM domain")

    conn.commit()
    yield conn, cur
    cur.close()
    conn.close()


@pytest.fixture
def url_score():
    """Return a URL score for testing."""
    return 0.8


@pytest.fixture
def relevance_score():
    """Return a relevance score for testing."""
    return 0.9


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


def test_calculate_priority_score(url_score, relevance_score):
    """Test that calculate_priority_score returns a float."""
    priority_score = calculate_priority_score(url_score, relevance_score)
    assert isinstance(priority_score, float), "Priority score should be a float"


def test_validate_url_new_entry(setup_test_database):
    """Test inserting a new URL and associated data into the database."""
    conn, cur = setup_test_database
    url = "https://example.com/review"
    relevance_score = 0.9

    validate_url(url, relevance_score)

    cur.execute("SELECT id FROM domain WHERE domain_name = 'example.com'")
    domain_id = cur.fetchone()
    assert domain_id is not None, "Domain should be inserted"

    cur.execute("SELECT id FROM url WHERE full_url = %s", (url,))
    url_id = cur.fetchone()
    assert url_id is not None, "URL should be inserted"

    cur.execute("SELECT priority FROM priority_queue WHERE url_id = %s", (url_id,))
    priority = cur.fetchone()
    assert priority is not None, "URL should be added to priority queue"


def test_validate_url_existing_url(setup_test_database):
    """Test that if a URL already exists, it updates `last_crawled` instead of inserting a new entry and it is not inserted into the priority queue"""
    conn, cur = setup_test_database
    url = "https://example.com/existing"
    relevance_score = 0.7

    cur.execute(
        "INSERT INTO domain (domain_name, visit_count) VALUES ('example.com', 5) RETURNING id"
    )
    domain_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO source(domain_id, source_type, credibility_score) VALUES (%s, 'news', %s) RETURNING id",
        (domain_id, 0.8),
    )
    source_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO url (source_id, full_url, first_seen, last_crawled) VALUES (%s, %s, NOW(), NOW()) RETURNING id",
        (source_id, url),
    )
    url_id = cur.fetchone()[0]
    conn.commit()

    validate_url(url, relevance_score)

    cur.execute("SELECT last_crawled FROM url WHERE id = %s", (url_id,))
    last_crawled = cur.fetchone()[0]
    assert last_crawled is not None, "last_crawled should be updated"

    cur.execute("SELECT priority FROM priority_queue WHERE url_id = %s", (url_id,))
    priority = cur.fetchone()
    assert priority is None, "URL should not be added to priority queue"


@patch("pipeline.validate.insert_url")
@patch("database.db_operations.insert_into_priority_queue")
def test_validate_url_error_handling(
    mock_priority_queue, mock_insert_url, setup_test_database
):
    """Test error handling during database failures."""

    mock_insert_url.side_effect = Exception("DB Error")

    conn, cur = setup_test_database

    with pytest.raises(Exception, match="DB Error"):
        validate_url("https://error.com", 0.6)

    mock_priority_queue.assert_not_called()
