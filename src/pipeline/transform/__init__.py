import logging
from urllib.parse import urlparse
from database.db_connector import get_db_connection
from database.db_operations import check_restaurant_exists, fuzzy_search_restaurant_name
from queue_manager.task_queues import load_queue
from .url_utils import identify_urls_from_soup, extract_homepage
from .identify_restaurants import identify_restaurants

PHASE = "TRANSFORM"


def is_restaurant(restaurant_name, conn):
    """Checks if a restaurant exists in DB using exact match or fuzzy search."""
    if check_restaurant_exists(restaurant_name, conn):
        return True, restaurant_name
    result = fuzzy_search_restaurant_name(restaurant_name, conn)
    return result and result.get("confidence", 0) > 0.75, (
        result["name"] if result else None
    )


def estimate_priority(url, validated_restaurants, current_priority):
    """Estimates priority [0-100] for derived URLs using weighted factors."""
    parent_signal = current_priority / 100.0
    rest_in_url_signal = (
        sum(1 for r in validated_restaurants if r.lower() in url.lower())
        / len(validated_restaurants)
        if validated_restaurants
        else 0.0
    )
    rest_count_signal = min(len(validated_restaurants) / 5.0, 1.0)

    w_p, w_url, w_count = 0.5, 0.3, 0.2
    combined_score = (
        w_p * parent_signal + w_url * rest_in_url_signal + w_count * rest_count_signal
    )

    return min(100, max(0, combined_score * 100.0))


def estimate_relevance(soup, validated_restaurants, current_priority):
    """Computes a relevance score [0-1] using weighted signals."""
    text = soup.get_text(separator=" ", strip=True).lower()
    text_length = len(text)
    headers = [
        h.get_text(strip=True).lower() for h in soup.find_all(["h1", "h2", "h3"])
    ]

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

    A, B, C, D = 0.2, 0.2, 0.3, 0.3
    combined_score = (
        A * header_signal + B * text_len_signal + C * keyword_signal + D * parent_signal
    )

    return min(1.0, max(0, combined_score))


def transform_data(content_tuple):
    """
    Processes extracted content, identifies restaurants & derived URLs,
    estimates relevance, and enqueues results.
    """
    conn = get_db_connection()
    target_url, parent_priority, soup = content_tuple
    processed_count = 0

    try:
        logging.info(f"[{PHASE}]: {target_url} - Processing content...")

        # Identify restaurants in content
        validated_restaurants = set()
        rejected_restaurants = []
        potential_restaurants = identify_restaurants(soup)

        for rest_name in potential_restaurants:
            exists, rest_name = is_restaurant(rest_name, conn)
            if exists:
                validated_restaurants.add(rest_name)
                logging.info(f"[{PHASE}]: Identified: {rest_name}")
            else:
                rejected_restaurants.append(rest_name)
                logging.info(f"[{PHASE}]: Rejected: {rest_name}")
        validated_restaurants = list(validated_restaurants)
        logging.info(f"[{PHASE}]: Identified {len(validated_restaurants)} restaurants.")
        logging.info(f"[{PHASE}]: Rejected {len(rejected_restaurants)} restaurants.")

        # Extract derived URLs
        homepage = extract_homepage(target_url)
        all_links = identify_urls_from_soup(soup, target_url)
        derived_links = set(all_links) - {homepage}

        derived_url_pairs = [(homepage, min(100, parent_priority))]
        for link in derived_links:
            new_priority = estimate_priority(
                link, validated_restaurants, parent_priority
            )
            derived_url_pairs.append((link, new_priority))
            logging.info(f"[{PHASE}]: Derived URL: {link} (Priority: {new_priority})")

        logging.info(f"[{PHASE}]: Extracted {len(derived_links)} URLs.")

        # Compute relevance score
        relevance_score = estimate_relevance(
            soup, validated_restaurants, parent_priority
        )

        # Construct and enqueue payload
        payload = {
            "target_url": target_url,
            "relevance_score": relevance_score,
            "derived_url_pairs": derived_url_pairs,
            "identified_restaurants": validated_restaurants,
            "rejected_restaurants": rejected_restaurants,
        }

        logging.info(f"[{PHASE}]: Enqueuing payload")
        load_queue.put(payload)
        processed_count += 1

        return True

    except Exception as e:
        logging.error(f"[{PHASE}]: Error: {e}")
        return False
    finally:
        conn.close()
        logging.info(f"[{PHASE}]: Processed {processed_count} URLs.")
        print(f"[{PHASE}]: Processed {processed_count} URLs.")
