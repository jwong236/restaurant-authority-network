import pytest
from unittest.mock import MagicMock
from database.db_connector import get_db_connection
from database.db_operations import (
    # Domain
    check_domain_exists,
    insert_domain,
    get_domain_quality_score,
    update_domain_visit_count,
    update_domain_quality_score,
    # Source
    insert_source,
    check_source_exists,
    # URL
    insert_url,
    check_url_exists,
    update_last_crawled,
    # Restaurant
    insert_restaurant,
    check_restaurant_exists,
    # Reference
    insert_reference,
    # Priority Queues
    insert_into_url_priority_queue,
    insert_into_restaurant_priority_queue,
    remove_from_url_priority_queue,
    remove_from_restaurant_priority_queue,
    get_priority_queue_url,
    get_priority_queue_restaurant,
    update_priority_queue_url,
    update_priority_queue_restaurant,
    # Custom
    fuzzy_search_restaurant_name,
)


@pytest.fixture
def db_connection():
    conn = get_db_connection()
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def mock_conn():
    m_conn = MagicMock()
    m_cursor = MagicMock()
    m_conn.cursor.return_value.__enter__.return_value = m_cursor
    return m_conn


# =================================================================================================
#                                          MOCK TESTS
# =================================================================================================


# ---------------- DOMAIN ----------------
def test_check_domain_exists_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (123,)
    r = check_domain_exists("example.com", mock_conn)
    c.execute.assert_called_once_with(
        "SELECT id FROM domain WHERE domain_name = %s", ("example.com",)
    )
    assert r == 123


def test_insert_domain_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (456,)
    r = insert_domain("example.com", 0.5, mock_conn)
    c.execute.assert_called_once_with(
        "INSERT INTO domain (domain_name, visit_count, quality_score) VALUES (%s, 0, %s) RETURNING id",
        ("example.com", 0.5),
    )
    assert r == 456


def test_get_domain_quality_score_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (0.75,)
    score = get_domain_quality_score(999, mock_conn)
    c.execute.assert_called_once_with(
        "SELECT quality_score FROM domain WHERE id = %s", (999,)
    )
    assert score == 0.75


def test_update_domain_visit_count_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    update_domain_visit_count(456, mock_conn)
    c.execute.assert_called_once_with(
        "UPDATE domain SET visit_count = visit_count + 1 WHERE id = %s", (456,)
    )


def test_update_domain_quality_score_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    update_domain_quality_score(456, -0.3, mock_conn)
    c.execute.assert_called_once_with(
        "UPDATE domain SET quality_score = %s WHERE id = %s", (-0.3, 456)
    )


# ---------------- SOURCE ----------------
def test_insert_source_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (789,)
    r = insert_source(123, "my_source", mock_conn)
    c.execute.assert_called_once_with(
        "INSERT INTO source (domain_id, source_type) VALUES (%s, %s) RETURNING id",
        (123, "my_source"),
    )
    assert r == 789


def test_check_source_exists_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (999,)
    r = check_source_exists(123, mock_conn)
    c.execute.assert_called_once_with(
        "SELECT id FROM source WHERE domain_id = %s", (123,)
    )
    assert r == 999


# ---------------- URL ----------------
def test_insert_url_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (101,)
    r = insert_url("https://test.com", 999, mock_conn)
    c.execute.assert_called_once_with(
        "INSERT INTO url (full_url, source_id, first_seen, last_crawled) VALUES (%s, %s, NOW(), NOW()) RETURNING id",
        ("https://test.com", 999),
    )
    assert r == 101


def test_check_url_exists_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (202,)
    r = check_url_exists("https://exists.com", mock_conn)
    c.execute.assert_called_once_with(
        "SELECT id FROM url WHERE full_url = %s", ("https://exists.com",)
    )
    assert r == 202


def test_update_last_crawled_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    update_last_crawled(101, mock_conn)
    c.execute.assert_called_once_with(
        "UPDATE url SET last_crawled = NOW() WHERE id = %s", (101,)
    )


# ---------------- RESTAURANT ----------------
def test_insert_restaurant_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (303,)
    r = insert_restaurant("Mock Cafe", "123 Mock St", mock_conn)
    c.execute.assert_called_once_with(
        "INSERT INTO restaurant (name, address) VALUES (%s, %s) RETURNING id",
        ("Mock Cafe", "123 Mock St"),
    )
    assert r == 303


def test_check_restaurant_exists_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (404,)
    r = check_restaurant_exists("Mock Cafe", mock_conn)
    c.execute.assert_called_once_with(
        "SELECT id FROM restaurant WHERE name = %s", ("Mock Cafe",)
    )
    assert r == 404


# ---------------- REFERENCE ----------------
def test_insert_reference_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (505,)
    r = insert_reference(1, 2, 0.75, mock_conn)
    c.execute.assert_called_once_with(
        "INSERT INTO reference (restaurant_id, url_id, relevance_score, discovered_at) "
        "VALUES (%s, %s, %s, NOW()) RETURNING id",
        (1, 2, 0.75),
    )
    assert r == 505


# ---------------- PRIORITY QUEUE ----------------
def test_insert_into_url_priority_queue_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    insert_into_url_priority_queue(1, 50, mock_conn)
    c.execute.assert_called_once_with(
        "INSERT INTO url_priority_queue (url_id, priority) "
        "VALUES (%s, %s) ON CONFLICT (url_id) DO UPDATE SET priority = EXCLUDED.priority",
        (1, 50),
    )


def test_insert_into_restaurant_priority_queue_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    insert_into_restaurant_priority_queue("Test Resto", 60, mock_conn)
    c.execute.assert_called_once_with(
        "INSERT INTO restaurant_priority_queue (name, priority) "
        "VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET priority = EXCLUDED.priority",
        ("Test Resto", 60),
    )


def test_get_priority_queue_url_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (999, 88)
    r = get_priority_queue_url(mock_conn)
    c.execute.assert_called_once_with(
        "SELECT url_id, priority FROM url_priority_queue ORDER BY priority DESC LIMIT 1 FOR UPDATE"
    )
    assert r == (999, 88)


def test_get_priority_queue_restaurant_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = ("Resto X", 99)
    r = get_priority_queue_restaurant(mock_conn)
    c.execute.assert_called_once_with(
        "SELECT name, priority FROM restaurant_priority_queue ORDER BY priority DESC LIMIT 1 FOR UPDATE"
    )
    assert r == ("Resto X", 99)


def test_update_priority_queue_url_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    update_priority_queue_url(mock_conn, 123, 77)
    c.execute.assert_called_once_with(
        "UPDATE url_priority_queue SET priority = %s WHERE url_id = %s", (77, 123)
    )


def test_update_priority_queue_restaurant_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    update_priority_queue_restaurant(mock_conn, "Test", 45)
    c.execute.assert_called_once_with(
        "UPDATE restaurant_priority_queue SET priority = %s WHERE name = %s",
        (45, "Test"),
    )


def test_remove_from_url_priority_queue_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    remove_from_url_priority_queue(1, mock_conn)
    c.execute.assert_called_once_with(
        "DELETE FROM url_priority_queue WHERE url_id = %s", (1,)
    )


def test_remove_from_restaurant_priority_queue_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    remove_from_restaurant_priority_queue("Some Resto", mock_conn)
    c.execute.assert_called_once_with(
        "DELETE FROM restaurant_priority_queue WHERE name = %s", ("Some Resto",)
    )


# ---------------- CUSTOM ----------------
def test_fuzzy_search_restaurant_name_mock(mock_conn):
    c = mock_conn.cursor.return_value.__enter__.return_value
    c.fetchone.return_value = (111, "Fancy Resto", "321 Street", 0.9)
    r = fuzzy_search_restaurant_name("Resto", mock_conn)
    c.execute.assert_called_once_with(
        "SELECT id, name, address, confidence FROM fuzzy_search_restaurant_name(%s)",
        ("Resto",),
    )
    assert r == {
        "id": 111,
        "name": "Fancy Resto",
        "address": "321 Street",
        "confidence": 0.9,
    }


# =================================================================================================
#                                        REAL DB TESTS
# =================================================================================================


# ---------------- DOMAIN ----------------
def test_check_domain_exists_db(db_connection):
    d = insert_domain("pytest-domain.com", 0.2, db_connection)
    assert d
    found = check_domain_exists("pytest-domain.com", db_connection)
    assert found == d
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


def test_insert_domain_db(db_connection):
    d = insert_domain("pytest-insert.com", -0.5, db_connection)
    assert d
    with db_connection.cursor() as cur:
        cur.execute("SELECT quality_score FROM domain WHERE id = %s", (d,))
        row = cur.fetchone()
        assert row and row[0] == -0.5
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


def test_get_domain_quality_score_db(db_connection):
    d_id = insert_domain("test-domain-qual.com", 0.3, db_connection)
    assert d_id is not None

    fetched_score = get_domain_quality_score(d_id, db_connection)
    assert fetched_score == 0.3

    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM domain WHERE id = %s", (d_id,))
    db_connection.commit()


def test_update_domain_visit_count_db(db_connection):
    d = insert_domain("pytest-visit.com", 0.1, db_connection)
    with db_connection.cursor() as cur:
        cur.execute("SELECT visit_count FROM domain WHERE id = %s", (d,))
        old = cur.fetchone()[0]
    update_domain_visit_count(d, db_connection)
    with db_connection.cursor() as cur:
        cur.execute("SELECT visit_count FROM domain WHERE id = %s", (d,))
        new = cur.fetchone()[0]
        assert new == old + 1
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


def test_update_domain_quality_score_db(db_connection):
    d = insert_domain("pytest-quality.com", 0.2, db_connection)
    update_domain_quality_score(d, 0.9, db_connection)
    with db_connection.cursor() as cur:
        cur.execute("SELECT quality_score FROM domain WHERE id = %s", (d,))
        val = cur.fetchone()[0]
        assert val == 0.9
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


# ---------------- SOURCE ----------------
def test_insert_source_db(db_connection):
    d = insert_domain("source-domain.com", 0.0, db_connection)
    s = insert_source(d, "my_source", db_connection)
    assert s
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM source WHERE id = %s", (s,))
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


def test_check_source_exists_db(db_connection):
    d = insert_domain("source-exists.com", 0.3, db_connection)
    s = insert_source(d, "unique_source", db_connection)
    found = check_source_exists(d, db_connection)
    assert found == s
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM source WHERE id = %s", (s,))
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


# ---------------- URL ----------------
def test_insert_url_db(db_connection):
    d = insert_domain("pytest-url.com", 0.3, db_connection)
    s = insert_source(d, "test_src", db_connection)
    u = insert_url("https://url-test.com", s, db_connection)
    assert u
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM url WHERE id = %s", (u,))
        cur.execute("DELETE FROM source WHERE id = %s", (s,))
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


def test_check_url_exists_db(db_connection):
    d = insert_domain("url-exists.com", -0.1, db_connection)
    s = insert_source(d, "src_exists", db_connection)
    u = insert_url("https://exists.com", s, db_connection)
    found = check_url_exists("https://exists.com", db_connection)
    assert found == u
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM url WHERE id = %s", (u,))
        cur.execute("DELETE FROM source WHERE id = %s", (s,))
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


def test_update_last_crawled_db(db_connection):
    d = insert_domain("last-crawl.com", 0.5, db_connection)
    s = insert_source(d, "crawl_src", db_connection)
    u = insert_url("https://crawl.com", s, db_connection)
    with db_connection.cursor() as cur:
        cur.execute("SELECT last_crawled FROM url WHERE id = %s", (u,))
        old_ts = cur.fetchone()[0]
    update_last_crawled(u, db_connection)
    with db_connection.cursor() as cur:
        cur.execute("SELECT last_crawled FROM url WHERE id = %s", (u,))
        new_ts = cur.fetchone()[0]
        assert new_ts is not None
        cur.execute("DELETE FROM url WHERE id = %s", (u,))
        cur.execute("DELETE FROM source WHERE id = %s", (s,))
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


# ---------------- RESTAURANT ----------------
def test_insert_restaurant_db(db_connection):
    r = insert_restaurant("DB Resto", "123 DB St", db_connection)
    assert r
    with db_connection.cursor() as cur:
        cur.execute("SELECT address FROM restaurant WHERE id = %s", (r,))
        row = cur.fetchone()
        assert row and row[0] == "123 DB St"
        cur.execute("DELETE FROM restaurant WHERE id = %s", (r,))
    db_connection.commit()


def test_check_restaurant_exists_db(db_connection):
    r_id = insert_restaurant("Check Resto", "456 Check Ave", db_connection)
    found_id = check_restaurant_exists("Check Resto", db_connection)
    assert found_id == r_id
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM restaurant WHERE id = %s", (r_id,))
    db_connection.commit()


# ---------------- REFERENCE ----------------
def test_insert_reference_db(db_connection):
    r_id = insert_restaurant("Ref Resto", "789 Ref Rd", db_connection)
    d_id = insert_domain("ref-domain.com", 0.1, db_connection)
    s_id = insert_source(d_id, "ref_src", db_connection)
    u_id = insert_url("https://ref.com", s_id, db_connection)
    ref_id = insert_reference(r_id, u_id, 0.8, db_connection)
    assert ref_id
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM reference WHERE id = %s", (ref_id,))
        cur.execute("DELETE FROM url WHERE id = %s", (u_id,))
        cur.execute("DELETE FROM source WHERE id = %s", (s_id,))
        cur.execute("DELETE FROM domain WHERE id = %s", (d_id,))
        cur.execute("DELETE FROM restaurant WHERE id = %s", (r_id,))
    db_connection.commit()


# ---------------- PRIORITY QUEUES ----------------
def test_insert_into_url_priority_queue_db(db_connection):
    d = insert_domain("pq-url.com", 0.2, db_connection)
    s = insert_source(d, "pq-src", db_connection)
    u = insert_url("https://pq-url.com", s, db_connection)
    insert_into_url_priority_queue(u, 30, db_connection)
    with db_connection.cursor() as cur:
        cur.execute("SELECT priority FROM url_priority_queue WHERE url_id = %s", (u,))
        row = cur.fetchone()
        assert row and row[0] == 30
        cur.execute("DELETE FROM url_priority_queue WHERE url_id = %s", (u,))
        cur.execute("DELETE FROM url WHERE id = %s", (u,))
        cur.execute("DELETE FROM source WHERE id = %s", (s,))
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


def test_insert_into_restaurant_priority_queue_db(db_connection):
    insert_into_restaurant_priority_queue("RestPQ", 40, db_connection)
    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT priority FROM restaurant_priority_queue WHERE name = %s",
            ("RestPQ",),
        )
        row = cur.fetchone()
        assert row and row[0] == 40
        cur.execute(
            "DELETE FROM restaurant_priority_queue WHERE name = %s", ("RestPQ",)
        )
    db_connection.commit()


def test_get_priority_queue_url_db(db_connection):
    d = insert_domain("get-pq-url.com", 0.3, db_connection)
    s = insert_source(d, "get-pq-src", db_connection)
    u1 = insert_url("https://getpqu1.com", s, db_connection)
    u2 = insert_url("https://getpqu2.com", s, db_connection)
    insert_into_url_priority_queue(u1, 10, db_connection)
    insert_into_url_priority_queue(u2, 80, db_connection)
    top = get_priority_queue_url(db_connection)
    assert top == (u2, 80)
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM url_priority_queue WHERE url_id IN (%s, %s)", (u1, u2))
        cur.execute("DELETE FROM url WHERE id IN (%s, %s)", (u1, u2))
        cur.execute("DELETE FROM source WHERE id = %s", (s,))
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


def test_get_priority_queue_restaurant_db(db_connection):
    insert_into_restaurant_priority_queue("PQ1", 20, db_connection)
    insert_into_restaurant_priority_queue("PQ2", 90, db_connection)
    top = get_priority_queue_restaurant(db_connection)
    assert top == ("PQ2", 90)
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM restaurant_priority_queue WHERE name IN ('PQ1','PQ2')")
    db_connection.commit()


def test_update_priority_queue_url_db(db_connection):
    d = insert_domain("update-pq-url.com", 0.0, db_connection)
    s = insert_source(d, "upd-src", db_connection)
    u = insert_url("https://update-pq.com", s, db_connection)
    insert_into_url_priority_queue(u, 10, db_connection)
    update_priority_queue_url(db_connection, u, 99)
    with db_connection.cursor() as cur:
        cur.execute("SELECT priority FROM url_priority_queue WHERE url_id = %s", (u,))
        row = cur.fetchone()
        assert row and row[0] == 99
        cur.execute("DELETE FROM url_priority_queue WHERE url_id = %s", (u,))
        cur.execute("DELETE FROM url WHERE id = %s", (u,))
        cur.execute("DELETE FROM source WHERE id = %s", (s,))
        cur.execute("DELETE FROM domain WHERE id = %s", (d,))
    db_connection.commit()


def test_update_priority_queue_restaurant_db(db_connection):
    insert_into_restaurant_priority_queue("upd-resto", 10, db_connection)
    update_priority_queue_restaurant(db_connection, "upd-resto", 55)
    with db_connection.cursor() as cur:
        cur.execute(
            "SELECT priority FROM restaurant_priority_queue WHERE name = %s",
            ("upd-resto",),
        )
        row = cur.fetchone()
        assert row and row[0] == 55
        cur.execute(
            "DELETE FROM restaurant_priority_queue WHERE name = %s", ("upd-resto",)
        )
    db_connection.commit()


# ---------------- CUSTOM ----------------
def test_fuzzy_search_restaurant_name_db(db_connection):
    # Might be None if no function or no match
    r = fuzzy_search_restaurant_name("Test Resto", db_connection)
    if r is not None:
        assert all(k in r for k in ("id", "name", "address", "confidence"))
    else:
        assert r is None
