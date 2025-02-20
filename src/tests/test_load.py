import pytest
from unittest.mock import MagicMock, patch
from pipeline.load import (
    extract_domain,
    load_target_url,
    load_identified_restaurants,
    load_reference,
    load_data,
)


def test_extract_domain():
    """Tests extract_domain() to ensure proper domain extraction."""
    assert extract_domain("https://example.com/path/page") == "example.com"
    assert extract_domain("http://sub.example.com") == "sub.example.com"
    assert extract_domain("https://example.com:8080") == "example.com"
    assert extract_domain("ftp://ftp.example.com/resource") == "ftp.example.com"


def test_load_target_url():
    """Tests load_target_url() ensuring correct database operations."""
    mock_cur = MagicMock()

    with patch(
        "pipeline.load.insert_domain", return_value=1
    ) as mock_insert_domain, patch(
        "pipeline.load.insert_source", return_value=2
    ) as mock_insert_source, patch(
        "pipeline.load.insert_url", return_value=3
    ) as mock_insert_url:

        url_id = load_target_url("https://example.com/page", 90, mock_cur)

        mock_insert_domain.assert_called_once_with("example.com", mock_cur)
        mock_insert_source.assert_called_once_with(1, 90, mock_cur)
        mock_insert_url.assert_called_once_with("https://example.com/page", 2, mock_cur)

        assert url_id == 3  # Expected URL ID


def test_load_identified_restaurants():
    """Tests load_identified_restaurants() ensuring that all new restaurants are inserted."""
    mock_cur = MagicMock()
    mock_insert_restaurant = MagicMock(side_effect=[101, 102, 103])  # Mock IDs

    with patch("pipeline.load.insert_restaurant", mock_insert_restaurant):
        restaurant_ids = load_identified_restaurants(
            ["Restaurant A", "Restaurant B", "Restaurant C"], mock_cur
        )

        assert restaurant_ids == [101, 102, 103]
        assert mock_insert_restaurant.call_count == 3


def test_load_reference():
    """Tests load_reference() ensuring references are inserted correctly."""
    mock_cur = MagicMock()

    with patch(
        "pipeline.load.insert_reference", return_value=(200, 300)
    ) as mock_insert_reference:
        restaurant_id, url_id = load_reference(200, 300, mock_cur)

        mock_insert_reference.assert_called_once_with(200, 300, mock_cur)
        assert restaurant_id == 200
        assert url_id == 300


def test_load_data():
    """Tests load_data() ensuring database operations are correctly executed."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur

    payload = (
        "https://example.com",
        85,  # Relevance score
        [("https://new-url.com", 60), ("https://another-url.com", 70)],  # Derived URLs
        ["Restaurant X", "Restaurant Y"],  # Identified restaurants
        ["Restaurant Z"],  # Rejected restaurants (not yet implemented)
    )

    with patch("pipeline.load.get_db_connection", return_value=mock_conn), patch(
        "pipeline.load.load_target_url", return_value=10
    ) as mock_load_target, patch(
        "pipeline.load.check_restaurant_exists",
        side_effect=lambda name, _: name
        == "Restaurant X",  # Restaurant X already exists
    ), patch(
        "pipeline.load.load_identified_restaurants", return_value=[100]
    ) as mock_load_restaurants, patch(
        "pipeline.load.load_reference"
    ) as mock_load_reference:

        result = load_data(payload)

        # ✅ Ensure the target URL is processed
        mock_load_target.assert_called_once_with(payload[0], payload[1], mock_cur)

        # ✅ Ensure only new restaurants are inserted
        mock_load_restaurants.assert_called_once_with(["Restaurant Y"], mock_cur)

        # ✅ Ensure references are loaded
        mock_load_reference.assert_called_once_with(
            100, 10, mock_cur
        )  # Restaurant Y ID = 100, URL ID = 10

        # ✅ Ensure transaction is committed
        mock_conn.commit.assert_called_once()
        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()

        # ✅ Ensure correct return value (no restaurant re-inserted)
        assert result == {
            "restaurant_search_queue": None,  # No new restaurants for re-processing
            "url_validate_queue": [
                "https://new-url.com",
                "https://another-url.com",
            ],  # URLs for validation
        }
