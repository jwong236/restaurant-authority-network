import pytest
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
from pipeline.transform import transform_data
from queue_manager.task_queues import load_queue


def test_transform_data_weighted():
    load_queue.queue.clear()

    html = """
    <html>
      <h1>Michelin Guide</h1>
      <h2>Fancy Bistro</h2>
      <p>This is a review of Fancy Bistro. Very large text ... (imagine 2000 characters) ... </p>
      <a href="https://deriv1.com">Link1</a>
      <a href="https://example.com/home">Home</a>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")

    with patch("pipeline.transform.get_db_connection") as mock_conn, patch(
        "pipeline.transform.identify_restaurants", return_value=["Fancy Bistro"]
    ), patch("pipeline.transform.is_restaurant", return_value=True), patch(
        "pipeline.transform.extract_homepage", return_value="https://example.com/home"
    ), patch(
        "pipeline.transform.identify_urls_from_soup",
        return_value=["https://deriv1.com", "https://example.com/home"],
    ):

        transform_data(("https://example.com/page", 40, soup))

    assert load_queue.qsize() == 1
    payload = load_queue.get()
    assert payload["target_url"] == "https://example.com/page"
    assert 0 <= payload["relevance_score"] <= 100
    derived = payload["derived_url_pairs"]
    assert len(derived) == 2
    assert derived[0][0] == "https://example.com/home"
    assert derived[0][1] == 40
