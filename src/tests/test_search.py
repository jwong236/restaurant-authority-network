import pytest
from pipeline.search import search_engine_search


# def test_search_engine_search(capsys):
#     """
#     Tests search_engine_search() by sending a real request and verifying the response structure.
#     Ensures:
#       - The function returns a list of URLs.
#       - The URLs are valid strings.
#     """

#     restaurant_data = {"name": "Pijja Palace", "location": "Los Angeles, CA"}
#     urls = search_engine_search(restaurant_data)
#     captured = capsys.readouterr()
#     print(captured.out)
#     assert isinstance(urls, list), "Expected a list of URLs"
#     assert len(urls) > 0, "Expected at least one search result"
#     for url in urls:
#         assert isinstance(url, str), "Expected URL to be a string"
#         assert url.startswith("http"), f"Invalid URL format: {url}"
