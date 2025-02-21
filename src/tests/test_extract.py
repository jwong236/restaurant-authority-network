# test_extract.py

import pytest
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
from queue_manager.task_queues import text_transformation_queue
from pipeline.extract import extract_content


@pytest.fixture
def mock_conn():
    """
    Returns a mock DB connection object whose cursor is a mock.
    """
    m_conn = MagicMock()
    m_cursor = MagicMock()
    m_conn.cursor.return_value.__enter__.return_value = m_cursor
    return m_conn


# --------------------------------------------------------------------------------------------------
#                                   MOCK TESTS
# --------------------------------------------------------------------------------------------------


def test_extract_content_no_url_in_queue(mock_conn):
    """
    If get_priority_queue_url returns None, no URL to process => returns False, prints message.
    """
    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url", return_value=None
    ):
        text_transformation_queue.queue.clear()

        result = extract_content()
        assert result is False
        assert text_transformation_queue.qsize() == 0


def test_extract_content_request_fails(mock_conn):
    """
    If request_url fails (returns None), we remove the URL from queue and return False.
    """
    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url",
        return_value=(123, "https://fail.com", 50),
    ), patch("pipeline.extract.request_url", return_value=None) as mock_req, patch(
        "pipeline.extract.remove_from_url_priority_queue"
    ) as mock_remove:

        text_transformation_queue.queue.clear()

        result = extract_content()
        assert result is False
        mock_req.assert_called_once_with("https://fail.com")
        mock_remove.assert_called_once_with(123, mock_conn)
        assert text_transformation_queue.qsize() == 0


def test_extract_content_http_404(mock_conn):
    """
    If response.status_code == 404, remove from queue, return False, skip transformation.
    """
    resp_mock = MagicMock()
    resp_mock.status_code = 404

    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url",
        return_value=(999, "https://404.com", 50),
    ), patch("pipeline.extract.request_url", return_value=resp_mock) as mock_req, patch(
        "pipeline.extract.remove_from_url_priority_queue"
    ) as mock_remove:
        text_transformation_queue.queue.clear()

        result = extract_content()
        assert result is False
        mock_req.assert_called_once_with("https://404.com")
        mock_remove.assert_called_once_with(999, mock_conn)
        assert text_transformation_queue.qsize() == 0


def test_extract_content_temporary_error(mock_conn):
    """
    If response is a 5xx, we update priority (0.75 * old), skip further processing.
    """
    resp_mock = MagicMock()
    resp_mock.status_code = 503

    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url",
        return_value=(777, "https://5xx.com", 80),
    ), patch("pipeline.extract.request_url", return_value=resp_mock), patch(
        "pipeline.extract.update_priority_queue_url"
    ) as mock_update:
        text_transformation_queue.queue.clear()

        result = extract_content()
        assert result is False
        mock_update.assert_called_once_with(mock_conn, 777, 60.0)
        assert text_transformation_queue.qsize() == 0


def test_extract_content_successful_extraction(mock_conn):
    """
    If everything goes well, we remove the URL from queue, parse HTML, and enqueue to transformation queue.
    """
    resp_mock = MagicMock()
    resp_mock.status_code = 200
    resp_mock.text = "<html><body><p>Some content here</p></body></html>"

    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url",
        return_value=(321, "https://success.com", 90),
    ), patch("pipeline.extract.request_url", return_value=resp_mock) as mock_req, patch(
        "pipeline.extract.remove_from_url_priority_queue"
    ) as mock_remove:

        text_transformation_queue.queue.clear()
        result = extract_content()
        assert result is True
        mock_req.assert_called_once_with("https://success.com")
        mock_remove.assert_called_once_with(321, mock_conn)
        assert text_transformation_queue.qsize() == 1
        item = text_transformation_queue.get()
        assert item[0] == "https://success.com"
        assert item[1] == 90
        assert isinstance(item[2], BeautifulSoup)


def test_extract_content_minimal_content(mock_conn):
    """
    If the page has minimal content (<10 chars), we remove from the queue and skip transformation.
    """
    resp_mock = MagicMock()
    resp_mock.status_code = 200
    resp_mock.text = "<html><body></body></html>"

    with patch("pipeline.extract.get_db_connection", return_value=mock_conn), patch(
        "pipeline.extract.get_priority_queue_url",
        return_value=(555, "https://empty.com", 25),
    ), patch("pipeline.extract.request_url", return_value=resp_mock), patch(
        "pipeline.extract.remove_from_url_priority_queue"
    ) as mock_remove:

        text_transformation_queue.queue.clear()
        result = extract_content()
        assert result is False
        mock_remove.assert_called_once_with(555, mock_conn)
        assert text_transformation_queue.qsize() == 0
