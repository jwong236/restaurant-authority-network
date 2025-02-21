import pytest
from unittest.mock import MagicMock, patch
from queue_manager.task_queues import url_validate_queue
from database.db_connector import get_db_connection
from pipeline.load import (
    load_data,
    load_identified_restaurants,
    load_rejected_restaurants,
    load_reference,
)


@pytest.fixture
def mock_conn():
    """
    Returns a mock DB connection object whose cursor is also a mock,
    for use in mock-based tests.
    """
    m_conn = MagicMock()
    m_cursor = MagicMock()
    m_conn.cursor.return_value.__enter__.return_value = m_cursor
    return m_conn


@pytest.fixture
def db_connection():
    """
    A real DB connection for integration tests (requires a test environment).
    Rolls back after each test to keep DB clean.
    """
    conn = get_db_connection()
    yield conn
    conn.rollback()
    conn.close()


# --------------------------------------------------------------------------------------------------
#                                       MOCK TESTS
# --------------------------------------------------------------------------------------------------


def test_load_identified_restaurants_mock(mock_conn):
    """
    Mock test for load_identified_restaurants.
    """

    with patch("pipeline.load.insert_restaurant") as mock_insert:
        mock_insert.side_effect = [101, None, 202]
        input_data = [
            {"name": "R1", "address": "Addr1"},
            {"name": "R2", "address": "Addr2"},
            {"name": "R3", "address": "Addr3"},
        ]

        result = load_identified_restaurants(input_data, mock_conn)
        assert result == [101, 202]
        assert mock_insert.call_count == 3


def test_load_rejected_restaurants_mock(mock_conn):
    """
    Mocks load_rejected_restaurants to confirm it inserts them into the restaurant priority queue.
    """
    with patch("pipeline.load.insert_into_restaurant_priority_queue") as mock_insert_pq:
        load_rejected_restaurants(["Bad A", "Bad B"], 0.75, mock_conn)
        assert mock_insert_pq.call_count == 2
        mock_insert_pq.assert_any_call("Bad A", 75.0, mock_conn)
        mock_insert_pq.assert_any_call("Bad B", 75.0, mock_conn)


def test_load_reference_mock(mock_conn):
    """
    Mocks load_reference to confirm it calls insert_reference as expected.
    """
    with patch("pipeline.load.insert_reference") as mock_insert_ref:
        ref_id = load_reference(123, 456, mock_conn, relevance=0.8)
        assert ref_id == mock_insert_ref.return_value
        mock_insert_ref.assert_called_once_with(123, 456, 0.8, mock_conn)


def test_load_data_mock(mock_conn):
    """
    Mock-based test for load_data. We patch out DB calls to confirm logic flow.
    """
    payload = {
        "target_url": "https://target.com",
        "relevance_score": 0.7,
        "derived_url_pairs": [("https://deriv1.com", 0.3), ("https://deriv2.com", 0.9)],
        "identified_restaurants": [
            {"name": "R1", "address": "A1"},
            {"name": "Existing Resto", "address": "????"},
        ],
        "rejected_restaurants": ["Bad R1", "Bad R2"],
    }

    with patch(
        "pipeline.load.get_db_connection", return_value=mock_conn
    ) as mock_get_db, patch("pipeline.load.check_url_exists") as mock_check_url, patch(
        "pipeline.load.check_restaurant_exists"
    ) as mock_check_rest, patch(
        "pipeline.load.insert_restaurant"
    ) as mock_insert_rest, patch(
        "pipeline.load.insert_reference"
    ) as mock_insert_ref, patch(
        "pipeline.load.insert_into_restaurant_priority_queue"
    ) as mock_insert_rpq:

        mock_check_url.return_value = 999
        mock_check_rest.side_effect = [
            None,
            123,
        ]
        mock_insert_rest.return_value = 888
        mock_insert_ref.return_value = 777

        url_validate_queue.queue.clear()

        load_data(payload)

        mock_check_url.assert_called_once_with("https://target.com", mock_conn)

        assert mock_check_rest.call_count == 2
        mock_check_rest.assert_any_call("R1", mock_conn)
        mock_check_rest.assert_any_call("Existing Resto", mock_conn)

        mock_insert_rest.assert_called_once_with("R1", "A1", mock_conn)

        mock_insert_ref.assert_called_once_with(888, 999, 0.7, mock_conn)

        assert mock_insert_rpq.call_count == 2
        mock_insert_rpq.assert_any_call("Bad R1", 70.0, mock_conn)
        mock_insert_rpq.assert_any_call("Bad R2", 70.0, mock_conn)

        assert url_validate_queue.qsize() == 2
        queued = []
        while not url_validate_queue.empty():
            queued.append(url_validate_queue.get())
        assert ("https://deriv1.com", 0.3) in queued
        assert ("https://deriv2.com", 0.9) in queued
