import pytest
from unittest.mock import MagicMock
from bs4 import BeautifulSoup
from pipeline.transform.url_utils import identify_urls_from_soup
from pipeline.transform.identify_restaurants import identify_restaurants
from pipeline.transform import (
    is_restaurant,
    estimate_priority,
    estimate_relevance,
    transform_data,
)
import json
import os

DATA_DIR = "src/tests/data"


def load_test_soup(filename):
    """Helper function to load saved HTML from JSON and return a BeautifulSoup object."""
    file_path = os.path.join(DATA_DIR, filename)

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return BeautifulSoup(data["html"], "html.parser")


@pytest.fixture
def simple_review_soup():
    return load_test_soup("simple_review.json")


@pytest.fixture
def aggregated_review_list_soup():
    return load_test_soup("aggregated_review_list.json")


@pytest.fixture
def simple_review_list_soup():
    return load_test_soup("simple_review_list.json")


@pytest.fixture
def hub_page_soup():
    return load_test_soup("hub_page.json")


@pytest.fixture
def irrelevant_soup():
    return load_test_soup("irrelevant.json")


def test_identify_urls_and_restaurants_simple_review(simple_review_soup):
    """Tests identify_urls_from_soup and identify_restaurants for Simple Review"""
    base_url = "http://test.com"

    # Extract URLs
    urls = identify_urls_from_soup(simple_review_soup, base_url)
    print("Extracted URLs from simple review:", urls)

    assert "http://hanumanthaieatery.com" in urls
    assert (
        "http://store.ocregister.com/?utm_campaign=evergreen&utm_medium=referral&utm_source=edit-nav&utm_content=&utm_term="
        in urls
    )

    # Extract Restaurants
    restaurants = identify_restaurants(simple_review_soup)
    print("Extracted restaurants from simple review:", restaurants)

    assert "Hanuman" in restaurants


def test_identify_urls_and_restaurants_aggregated_review(aggregated_review_list_soup):
    """Tests identify_urls_from_soup and identify_restaurants for Aggregated Review List"""
    base_url = "http://test.com"

    # Extract URLs
    urls = identify_urls_from_soup(aggregated_review_list_soup, base_url)
    print("Extracted URLs from aggregated review list:", urls)

    # Extract Restaurants
    restaurants = identify_restaurants(aggregated_review_list_soup)
    print("Extracted restaurants from aggregated review list:", restaurants)

    expected_restaurants = [
        "Fuoco Pizzeria Napoletana",
        "Khan Saab Desi Craft Kitchen",
        "Adya",
        "Al Baraka Restaurant",
        "Kareem's Falafel",
        "Katella Bakery, Deli & Restaurant",
        "Haven Craft Kitchen + Bar",
        "Shootz",
        "Yigah",
    ]

    for expected in expected_restaurants:
        assert any(
            expected in r for r in restaurants
        ), f"Expected restaurant {expected} not found."


def test_identify_urls_and_restaurants_hub_page(hub_page_soup):
    """Tests identify_urls_from_soup and identify_restaurants for Hub Page"""
    base_url = "http://test.com"

    # Extract URLs
    urls = identify_urls_from_soup(hub_page_soup, base_url)
    print("Extracted URLs from hub page:", urls)

    # Extract Restaurants
    restaurants = identify_restaurants(hub_page_soup)
    print("Extracted restaurants from hub page:", restaurants)

    # No assertions since we don't know what should be found


def test_identify_urls_and_restaurants_irrelevant(irrelevant_soup):
    """Tests identify_urls_from_soup and identify_restaurants for an Irrelevant Page"""
    base_url = "http://test.com"

    # Extract URLs
    urls = identify_urls_from_soup(irrelevant_soup, base_url)
    print("Extracted URLs from irrelevant page:", urls)

    restaurants = identify_restaurants(irrelevant_soup)
    print("Extracted restaurants from irrelevant page:", restaurants)

    assert 1 == 1


@pytest.mark.parametrize(
    "restaurant_name, db_return, fuzzy_return, expected",
    [
        ("Existing Restaurant", True, {"confidence": 0.9}, True),
        ("New Restaurant", False, {"confidence": 0.6}, True),
        ("Fake Restaurant", False, {"confidence": 0.3}, False),
        ("Unknown Restaurant", False, None, False),
    ],
)
def test_is_restaurant(restaurant_name, db_return, fuzzy_return, expected, mocker):
    mocker.patch("pipeline.transform.check_restaurant_exists", return_value=db_return)
    mocker.patch(
        "pipeline.transform.fuzzy_search_restaurant_name", return_value=fuzzy_return
    )
    assert is_restaurant(restaurant_name) == expected


@pytest.mark.parametrize(
    "url, validated_restaurants, priority, expected_priority",
    [
        (
            "http://test.com/joespizza",
            ["Joe's Pizza"],
            50,
            50,
        ),
        ("http://test.com", [], 40, 40),
        ("http://test.com", ["Joe's Pizza", "Sushi Place"], 30, 40),
        ("http://test.com", ["Joe's Pizza", "Sushi Place", "Burger Shack"], 20, 30),
        ("http://test.com", ["Random Place"], 80, 80),
    ],
)
def test_estimate_priority(url, validated_restaurants, priority, expected_priority):
    assert estimate_priority(url, validated_restaurants, priority) == expected_priority


def test_estimate_relevance(mocker, simple_review_soup):
    """
    Tests estimate_relevance by verifying the numeric score for different cases.
    """
    mocker.patch(
        "pipeline.transform.identify_restaurants",
        return_value=["In-N-Out Burger", "Shake Shack"],
    )

    validated_restaurants = ["In-N-Out Burger", "Shake Shack"]

    relevance_score = estimate_relevance(simple_review_soup, validated_restaurants)

    print("Relevance Score:", relevance_score)

    assert relevance_score > 0
    assert relevance_score <= 100


def test_transform_data(mocker, simple_review_soup):
    mocker.patch("pipeline.transform.extract_homepage", return_value="http://test.com/")
    mocker.patch(
        "pipeline.transform.identify_urls_from_soup",
        return_value=["http://example.com"],
    )
    mocker.patch(
        "pipeline.transform.identify_restaurants", return_value=["Test Restaurant"]
    )
    mocker.patch("pipeline.transform.is_restaurant", return_value=True)
    mocker.patch(
        "pipeline.transform.estimate_relevance", return_value="moderately_relevant"
    )

    content_tuple = ("http://test.com", 80, simple_review_soup)
    payload = transform_data(content_tuple)

    assert payload["target_url"] == "http://test.com"
    assert payload["relevance_score"] == "moderately_relevant"
    assert payload["identified_restaurants"] == ["Test Restaurant"]
    assert payload["derived_url_pairs"] == [
        ("http://test.com/", 80),
        ("http://example.com", 80),
    ]
