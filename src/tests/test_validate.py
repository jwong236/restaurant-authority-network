# test_validate.py

import pytest
from unittest.mock import patch
from database.db_connector import get_db_connection
from pipeline.validate import (
    normalize_url,
    calculate_url_score,
    calculate_priority_score,
    validate_url,
)


@pytest.fixture(scope="function")
def setup_test_database():
    conn = get_db_connection()
    cur = conn.cursor()
    # Cleanup
    cur.execute("DELETE FROM url_priority_queue")
    cur.execute("DELETE FROM reference")
    cur.execute("DELETE FROM url")
    cur.execute("DELETE FROM source")
    cur.execute("DELETE FROM domain")
    conn.commit()
    yield conn
    cur.close()
    conn.close()


def test_normalize_url():
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
    "url,expected",
    [
        ("https://example.com", 0.5),
        ("https://example.org", 1.0),  # .org => +0.5
        ("https://news.com", 1.0),  # "news" => +0.5
        ("https://weeklydigest.com", 0.8),  # "weekly" => +0.3
        ("https://blog.com", 0.5),
    ],
)
def test_calculate_url_score(url, expected):
    assert calculate_url_score(url) == expected


def test_calculate_priority_score():
    # Example: 0.65 * 0.9 + 0.35 * 0.8 = 0.585 + 0.28 = 0.865
    p = calculate_priority_score(0.9, 0.8)
    assert abs(p - 0.865) < 1e-9


@pytest.mark.parametrize(
    "url,relevance,expected_priority_range",
    [
        ("https://example.org/review", 1.0, (0.9999, 1.0001)),  # near 1.0
        ("https://unknown.com", 0.0, (0.0, 0.5)),  # 0 or low
    ],
)
def test_calculate_priority_score_ranges(url, relevance, expected_priority_range):
    u_score = calculate_url_score(url)
    p = calculate_priority_score(relevance, u_score)
    assert expected_priority_range[0] <= p <= expected_priority_range[1]


# ------------------------------------------------------------------------------
#                               REAL DB TESTS
# ------------------------------------------------------------------------------
def test_validate_url_new_entry(setup_test_database):
    conn = setup_test_database
    url = "https://example.com/review"
    relevance_score = 0.9

    validate_url((url, relevance_score))

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, quality_score, visit_count FROM domain WHERE domain_name = 'example.com'"
        )
        domain_row = cur.fetchone()
        assert domain_row is not None, "Domain should be inserted"
        dom_id, dom_score, dom_visits = domain_row
        # If new, domain_score = 0.0 by default, visits = 1
        assert abs(dom_score - 0.0) < 1e-9
        assert dom_visits == 1

        cur.execute("SELECT id FROM url WHERE full_url = %s", (url,))
        url_row = cur.fetchone()
        assert url_row is not None, "URL should be inserted"
        url_id = url_row[0]

        # Check priority queue
        cur.execute(
            "SELECT priority FROM url_priority_queue WHERE url_id = %s", (url_id,)
        )
        priority_row = cur.fetchone()
        assert priority_row is not None, "URL should be added to priority queue"
        assert (
            0.0 < priority_row[0] <= 1.0
        ), "Priority should be within [0,1] if that's your scoring logic"


def test_validate_url_existing_url(setup_test_database):
    conn = setup_test_database
    url = "https://example.com/existing"
    relevance_score = 0.7

    # Manually insert a domain, source, url
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO domain (domain_name, visit_count, quality_score) VALUES (%s, %s, %s) RETURNING id",
            ("example.com", 5, 0.2),
        )
        dom_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO source (domain_id, source_type) VALUES (%s, %s) RETURNING id",
            (dom_id, "webpage"),
        )
        src_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO url (source_id, full_url, first_seen, last_crawled) VALUES (%s, %s, NOW(), NOW()) RETURNING id",
            (src_id, url),
        )
        existing_url_id = cur.fetchone()[0]
        conn.commit()

    validate_url((url, relevance_score))

    # Should update last_crawled but not insert new priority queue row
    with conn.cursor() as cur:
        cur.execute("SELECT last_crawled FROM url WHERE id = %s", (existing_url_id,))
        updated_ts = cur.fetchone()[0]
        assert updated_ts is not None, "last_crawled should be updated"

        cur.execute(
            "SELECT priority FROM url_priority_queue WHERE url_id = %s",
            (existing_url_id,),
        )
        pq_check = cur.fetchone()
        assert pq_check is None, "Existing URL should not be re-added to priority queue"


@patch("pipeline.validate.insert_url")
@patch("pipeline.validate.insert_into_url_priority_queue")
def test_validate_url_error_handling(
    mock_insert_queue, mock_insert_url, setup_test_database
):
    conn = setup_test_database
    mock_insert_url.side_effect = Exception("DB Error")

    with pytest.raises(Exception, match="DB Error"):
        validate_url(("https://error.com", 0.6))

    mock_insert_queue.assert_not_called()
