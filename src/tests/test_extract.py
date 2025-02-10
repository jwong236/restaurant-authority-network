import pytest
from unittest.mock import MagicMock, patch
from pipeline.extract import reprioritize_url, extract_content


@pytest.fixture
def long_html_content():
    """Fixture to generate an HTML document with more than 100 words."""
    return """
        <html>
        <body>
            <h1 class="title">Gourmet Ramen Spot</h1>
            <div class="review">
                This is a detailed review about an amazing ramen shop. 
                The broth was rich and flavorful, with deep umami notes. 
                The noodles were perfectly cooked, maintaining a slight chewiness that complemented the broth well. 
                The toppings, including chashu pork and a soft-boiled egg, were expertly prepared. 
                Each bite provided a harmonious balance of flavors and textures. 
                The ambiance of the restaurant was warm and welcoming, making it an enjoyable dining experience. 
                The staff was attentive, ensuring that every guest felt taken care of. 
                Additionally, the menu offered a variety of ramen styles, catering to different preferences. 
                Overall, this place is a must-visit for ramen lovers. The price point was reasonable, 
                considering the high-quality ingredients used. The location was convenient, 
                situated in the heart of the city. Would definitely recommend!
            </div>
            <div class="rating">4.8/5</div>
        </body>
        </html>
    """


@pytest.fixture
def mock_db():
    """Fixture to mock database connection and cursor."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


@patch("pipeline.extract.get_db_connection")
@patch("pipeline.extract.request_url")
@patch("pipeline.extract.get_priority_queue_url")
@patch("pipeline.extract.remove_priority_queue_url")
def test_extract_content_success(
    mock_remove_url,
    mock_get_priority,
    mock_request,
    mock_get_db,
    long_html_content,
):
    """Test successful extraction with a long and valid food review."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_get_priority.return_value = ("https://example.com", 10)
    mock_request.return_value.text = long_html_content

    result = extract_content()

    assert result is not None
    mock_remove_url.assert_called_once_with("https://example.com", mock_cursor)


@patch("pipeline.extract.get_db_connection")
@patch("pipeline.extract.request_url")
@patch("pipeline.extract.get_priority_queue_url")
def test_extract_content_empty_page(mock_get_priority, mock_request, mock_get_db):
    """Test handling of an empty or useless page."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_get_priority.return_value = ("https://example.com", 10)
    mock_request.return_value.text = "<html><body></body></html>"

    result = extract_content()

    assert result is None  # Should return None for empty content


@patch("pipeline.extract.get_db_connection")
@patch("pipeline.extract.request_url")
@patch("pipeline.extract.get_priority_queue_url")
def test_extract_content_failed_request(mock_get_priority, mock_request, mock_get_db):
    """Test handling of a failed request (e.g., 404 error)."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_get_priority.return_value = ("https://example.com", 10)
    mock_request.return_value = None  # Simulating a failed request

    result = extract_content()

    assert result is None  # Should return None if the request fails


@patch("pipeline.extract.get_db_connection")
@patch("pipeline.extract.get_priority_queue_url")
def test_extract_content_database_failure(mock_get_priority, mock_get_db):
    """Test handling of a database failure."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    mock_get_priority.side_effect = Exception("DB Error")  # Force database error

    result = extract_content()  # Function should gracefully handle errors

    assert result is None  # Should return None instead of raising an exception


@pytest.mark.parametrize(
    "status_code, expected_priority, expected_remove",
    [
        (404, None, True),  # 404 → Remove from queue
        (403, 7.5, False),  # 403 → Lower priority (×0.75)
        (500, 7.5, False),  # 500 → Lower priority (×0.75)
        (502, 7.5, False),  # 502 → Lower priority (×0.75)
        (503, 7.5, False),  # 503 → Lower priority (×0.75)
        (504, 7.5, False),  # 504 → Lower priority (×0.75)
        (200, None, False),  # 200 → No priority change
    ],
)
def test_reprioritize_url(mock_db, status_code, expected_priority, expected_remove):
    """Test reprioritization logic for various response codes."""
    mock_conn, mock_cursor = mock_db
    url = "https://example.com"
    priority = 10
    mock_response = MagicMock()
    mock_response.status_code = status_code

    with patch(
        "pipeline.extract.update_priority_queue_url"
    ) as mock_update_priority, patch(
        "pipeline.extract.remove_priority_queue_url"
    ) as mock_remove_priority:

        result = reprioritize_url(url, priority, mock_response, mock_cursor)

        if expected_remove:
            mock_remove_priority.assert_called_once_with(url, mock_cursor)
            assert result is None
        elif expected_priority:
            mock_update_priority.assert_called_once_with(
                mock_cursor, url, expected_priority
            )
            assert result is None
        else:
            assert result == mock_response  # 200 case → should return response as-is
