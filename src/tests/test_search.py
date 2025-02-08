import os
import pytest
import requests
import requests_mock
import re
from pipeline.search import search_engine_search


@pytest.fixture
def mock_brave_api():
    """Mock Brave API to return predefined search results."""
    with requests_mock.Mocker() as mock:
        mock.get(
            re.compile(r"https://api.search.brave.com/res/v1/web/search.*"),
            additional_matcher=lambda request: "q=best+pizza+in+New+York+review"
            in request.url,
            json={
                "web": {
                    "results": [
                        {"url": "https://example.com/review1"},
                        {"url": "https://example.com/review2"},
                    ]
                }
            },
        )
        mock.get(
            re.compile(r"https://api.search.brave.com/res/v1/web/search.*"),
            additional_matcher=lambda request: "q=best+pizza+in+New+York+restaurant+review"
            in request.url,
            json={
                "web": {
                    "results": [
                        {"url": "https://example.com/review3"},
                        {"url": "https://example.com/review4"},
                    ]
                }
            },
        )
        yield mock


@pytest.fixture
def mock_brave_api_no_results():
    """Mock Brave API to return empty results."""
    with requests_mock.Mocker() as mock:
        mock.get(
            re.compile(r"https://api.search.brave.com/res/v1/web/search.*"),
            json={"web": {"results": []}},
        )
        yield mock


@pytest.fixture
def mock_brave_api_error():
    """Mock Brave API to simulate a request failure."""
    with requests_mock.Mocker() as mock:
        mock.get(
            re.compile(r"https://api.search.brave.com/res/v1/web/search.*"),
            status_code=500,
        )
        yield mock


def test_search_engine_search_success(mock_brave_api):
    """Test successful search query with mock API response."""
    query = "best pizza in New York"
    results = search_engine_search(query, result_size=3)

    assert isinstance(results, list), "Function should return a list"
    assert len(results) == 4, f"Expected 4 results, got {len(results)}"
    assert set(results) == {
        "https://example.com/review1",
        "https://example.com/review2",
        "https://example.com/review3",
        "https://example.com/review4",
    }, "Unexpected URLs returned"


def test_search_engine_search_empty(mock_brave_api_no_results):
    """Test search function when no results are found."""
    query = "nonexistent restaurant"
    results = search_engine_search(query, result_size=3)

    assert results == [], "Expected an empty list when no results are found"


def test_search_engine_search_api_failure(mock_brave_api_error):
    """Test API failure handling."""
    query = "random search"

    with pytest.raises(requests.exceptions.HTTPError):
        search_engine_search(query, result_size=3)
