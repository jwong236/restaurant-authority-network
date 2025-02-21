import pytest
import logging
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
from queue_manager.task_queues import transform_queue
from pipeline.extract import extract_content
from queue_manager.task_queues import transform_queue
from database.db_operations import (
    insert_domain,
    insert_source,
    insert_url,
    insert_into_url_priority_queue,
    check_url_exists,
    get_priority_queue_url,
)
from database.db_connector import get_db_connection
from pipeline.extract import extract_content

PHASE = "EXTRACT"


@pytest.fixture
def mock_conn():
    """Fixture for a mock database connection."""
    return MagicMock()


@pytest.fixture
def db_connection():
    """Fixture for a real database connection."""
    conn = get_db_connection()
    yield conn
    conn.rollback()
    conn.close()


def test_extract_content_request_failure(mock_conn):
    """If request_url fails (returns None), we remove URL from queue and keep going."""
    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url",
        side_effect=[(123, "https://fail.com", 50), None],
    ), patch("pipeline.extract.request_url", return_value=None) as mock_req, patch(
        "pipeline.extract.remove_from_url_priority_queue"
    ) as mock_remove:

        transform_queue.queue.clear()
        result = extract_content()
        assert result is False
        mock_req.assert_called_once_with("https://fail.com")
        mock_remove.assert_called_once_with(123, mock_conn)
        assert transform_queue.qsize() == 0


def test_extract_content_404(mock_conn):
    """404 => remove from queue, no transform enqueued."""
    resp_mock = MagicMock()
    resp_mock.status_code = 404

    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url",
        side_effect=[(999, "https://404.com", 60), None],
    ), patch("pipeline.extract.request_url", return_value=resp_mock), patch(
        "pipeline.extract.remove_from_url_priority_queue"
    ) as mock_remove:

        transform_queue.queue.clear()
        result = extract_content()
        assert result is False
        mock_remove.assert_called_once_with(999, mock_conn)
        assert transform_queue.qsize() == 0


def test_extract_content_5xx(mock_conn):
    """5xx => update priority to old * 0.75, skip transform."""
    resp_mock = MagicMock()
    resp_mock.status_code = 503

    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url",
        side_effect=[(321, "https://5xx.com", 80), None],
    ), patch("pipeline.extract.request_url", return_value=resp_mock), patch(
        "pipeline.extract.update_priority_queue_url"
    ) as mock_update:

        transform_queue.queue.clear()
        result = extract_content()
        assert result is False
        mock_update.assert_called_once_with(mock_conn, 321, 60.0)  # 80 * 0.75 = 60
        assert transform_queue.qsize() == 0


def test_extract_content_minimal_content(mock_conn):
    """If the page is too short (<10 chars), remove from queue, skip transform."""
    resp_mock = MagicMock()
    resp_mock.status_code = 200
    resp_mock.text = "<html><body></body></html>"

    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url",
        side_effect=[(456, "https://empty.com", 30), None],
    ), patch("pipeline.extract.request_url", return_value=resp_mock), patch(
        "pipeline.extract.remove_from_url_priority_queue"
    ) as mock_remove:

        transform_queue.queue.clear()
        result = extract_content()
        assert result is False
        mock_remove.assert_called_once_with(456, mock_conn)
        assert transform_queue.qsize() == 0


def test_extract_content_successful(mock_conn):
    """A successful extraction => remove from queue, push to transform_queue."""
    resp_mock = MagicMock()
    resp_mock.status_code = 200
    resp_mock.text = "<html><body><p>Valid content here.</p></body></html>"

    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url",
        side_effect=[(888, "https://valid.com", 75), None],
    ), patch("pipeline.extract.request_url", return_value=resp_mock), patch(
        "pipeline.extract.remove_from_url_priority_queue"
    ) as mock_remove:

        transform_queue.queue.clear()
        result = extract_content()
        assert result is True  # At least one successful extraction

        mock_remove.assert_called_once_with(888, mock_conn)
        assert transform_queue.qsize() == 1

        # Verify transform_queue contents
        item = transform_queue.get()
        assert item[0] == "https://valid.com"
        assert item[1] == 75
        assert isinstance(item[2], BeautifulSoup)


def test_extract_content_db(db_connection):
    """
    Integration test for extract_content using a real DB.
    1) Insert a domain+source+url_id into url_priority_queue
    2) Call extract_content
    3) Confirm it processes, removes from DB, and enqueues transform
    """

    transform_queue.queue.clear()

    # Insert domain
    with db_connection.cursor() as cur:
        cur.execute("DELETE FROM domain WHERE domain_name='test-extract.com'")
    db_connection.commit()

    d_id = insert_domain("test-extract.com", 0.0, db_connection)
    s_id = insert_source(d_id, "webpage", db_connection)
    u_id = insert_url("https://test-extract.com/page", s_id, db_connection)
    insert_into_url_priority_queue(u_id, 99, db_connection)
    db_connection.commit()

    with patch("pipeline.extract.requests.get") as mock_get:
        resp_mock = MagicMock()
        resp_mock.status_code = 200
        resp_mock.text = "<html><body>Real DB test content here</body></html>"
        mock_get.return_value = resp_mock

        success = extract_content()
        assert success is True

        # Confirm URL is removed from priority queue
        res = get_priority_queue_url(db_connection)
        assert res is None

        # Check transform queue has 1 item
        assert transform_queue.qsize() == 1
        item = transform_queue.get()
        assert item[0] == "https://test-extract.com/page"
        assert item[1] == 99
        assert "Real DB test content" in item[2].get_text()
