import logging
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from database.db_connector import get_db_connection
from database.db_operations import check_restaurant_exists, fuzzy_search_restaurant_name
from queue_manager.task_queues import data_loading_queue
from .url_utils import identify_urls_from_soup, extract_homepage
from .identify_restaurants import identify_restaurants


def is_restaurant(restaurant_name, conn):
    """
    Checks if the restaurant exists in the database.
    - First, checks for an exact match.
    - If not found, uses fuzzy matching (confidence > 0.5).
    """
    if check_restaurant_exists(restaurant_name, conn):
        return True
    result = fuzzy_search_restaurant_name(restaurant_name, conn)
    return result and result.get("confidence", 0) > 0.5


def estimate_priority(url, validated_restaurants, current_priority):
    """
    Estimates the priority score [0-100] for a derived URL.
    Factors:
    - Parent page's priority (weighted 50%).
    - Number of validated restaurants in the URL (weighted 30%).
    - Count of validated restaurants linked to the page (capped at 5, weighted 20%).
    """
    parent_signal = current_priority / 100.0
    rest_in_url_signal = (
        sum(1 for r in validated_restaurants if r.lower() in url.lower())
        / len(validated_restaurants)
        if validated_restaurants
        else 0.0
    )
    rest_count_signal = min(len(validated_restaurants) / 5.0, 1.0)

    # Weighted sum
    w_p, w_url, w_count = 0.5, 0.3, 0.2
    combined_score = (
        w_p * parent_signal + w_url * rest_in_url_signal + w_count * rest_count_signal
    )

    return min(100, max(0, combined_score * 100.0))


def estimate_relevance(soup, validated_restaurants, current_priority):
    """
    Computes a relevance score [0-100] for the target URL.
    Factors:
    - (20%) How many validated restaurants appear in headers (h1/h2/h3).
    - (20%) Text length (scaled up to 3000 characters).
    - (30%) Presence of review-related keywords.
    - (30%) Parent page priority.
    """
    text = soup.get_text(separator=" ", strip=True).lower()
    text_length = len(text)
    headers = [
        h.get_text(strip=True).lower() for h in soup.find_all(["h1", "h2", "h3"])
    ]

    # Compute signals
    header_signal = (
        sum(1 for r in validated_restaurants if any(r.lower() in hd for hd in headers))
        / len(validated_restaurants)
        if validated_restaurants
        else 0.0
    )
    text_len_signal = min(text_length / 3000.0, 1.0)
    keyword_signal = min(
        sum(
            1
            for kw in [
                "review",
                "menu",
                "dish",
                "chef",
                "restaurant",
                "michelin",
                "wine list",
            ]
            if kw in text
        )
        / 5.0,
        1.0,
    )
    parent_signal = current_priority / 100.0

    # Weighted sum
    A, B, C, D = 0.2, 0.2, 0.3, 0.3
    combined_score = (
        A * header_signal + B * text_len_signal + C * keyword_signal + D * parent_signal
    )

    return min(1.0, max(0, combined_score))


def transform_data(content_tuple):
    """
    Processes extracted content and prepares a payload for data loading.
    - Identifies validated and rejected restaurant mentions.
    - Extracts derived URLs and computes new priorities.
    - Estimates relevance score for the current page.
    - Enqueues the processed payload to `data_loading_queue`.
    """
    conn = get_db_connection()
    target_url, parent_priority, soup = content_tuple

    logging.info(f"Processing extracted content for: {target_url}")

    # Identify restaurants in content
    validated_restaurants = []
    rejected_restaurants = []
    potential_restaurants = identify_restaurants(soup)

    for rest_name in potential_restaurants:
        if is_restaurant(rest_name, conn):
            validated_restaurants.append(rest_name)
        else:
            rejected_restaurants.append(rest_name)

    logging.debug(f"Validated restaurants: {validated_restaurants}")
    logging.debug(f"Rejected restaurants: {rejected_restaurants}")

    # Extract derived URLs
    homepage = extract_homepage(target_url)
    all_links = identify_urls_from_soup(soup, target_url)
    derived_links = set(all_links) - {homepage}

    derived_url_pairs = [
        (homepage, min(100, parent_priority))
    ]  # Homepage inherits full parent priority
    for link in derived_links:
        new_priority = estimate_priority(link, validated_restaurants, parent_priority)
        derived_url_pairs.append((link, new_priority))

    logging.info(f"Identified {len(derived_links)} derived URLs from {target_url}")

    # Compute relevance score
    relevance_score = estimate_relevance(soup, validated_restaurants, parent_priority)

    # Construct and enqueue payload
    payload = {
        "target_url": target_url,
        "relevance_score": relevance_score,
        "derived_url_pairs": derived_url_pairs,
        "identified_restaurants": validated_restaurants,
        "rejected_restaurants": rejected_restaurants,
    }

    logging.info(
        f"Enqueueing payload for {target_url} with relevance {relevance_score}"
    )
    data_loading_queue.put(payload)

    conn.close()
