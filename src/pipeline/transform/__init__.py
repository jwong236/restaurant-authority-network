# ./src/pipeline/transform/__init__.py
from .url_utils import identify_urls_from_soup, extract_homepage
from database.db_operations import check_restaurant_exists, fuzzy_search_restaurant_name
from .identify_restaurants import identify_restaurants
from queue_manager.task_queues import data_loading_queue


def is_restaurant(restaurant):
    if check_restaurant_exists(restaurant):
        return True
    result = fuzzy_search_restaurant_name(restaurant)
    if result and result.get("confidence", 0) > 0.5:
        return True
    return False


def estimate_priority(url, validated_restaurants, priority):
    """
    Estimates the priority of a URL based on restaurant mentions and structural hints.
    """
    priority_adjustments = 0

    # High priority if URL itself contains a validated restaurant name
    if any(r.lower() in url.lower() for r in validated_restaurants):

        priority_adjustments += 50

    # Increase priority if many validated restaurants are present
    if len(validated_restaurants) > 5:
        priority_adjustments += 20
    elif 2 <= len(validated_restaurants) <= 5:
        priority_adjustments += 10

    # Ensure priority is within the range [0, 100]
    return min(100, max(0, priority + priority_adjustments))


def estimate_relevance(soup, validated_restaurants):
    """
    Estimates the relevance of a webpage as a restaurant review based on structural and semantic hints.
    Returns a numerical relevance score instead of categorical labels.
    """
    text_length = len(soup.get_text().strip())
    headers = [h.get_text().strip().lower() for h in soup.find_all(["h1", "h2", "h3"])]

    # Count occurrences of validated restaurants in headers
    restaurant_mentions = sum(
        1 for r in validated_restaurants if any(r.lower() in h for h in headers)
    )

    # Base relevance score
    relevance_score = 0

    # Boost score based on restaurant mentions in headers
    relevance_score += restaurant_mentions * 10  # Each mention adds +10

    # Increase score based on text length (indicates content depth)
    if text_length > 3000:
        relevance_score += 30
    elif text_length > 1000:
        relevance_score += 20
    elif text_length > 500:
        relevance_score += 10

    # Ensure score is within a reasonable range
    return min(100, relevance_score)  # Max out at 100


def transform_data(content_tuple):
    """
    Main function for transforming extracted data.
    content_tuple = (url, priority, soup)
    Goal:
    - Load data for target reference into the reference table
    - Load data for derived urls into the priotity queue
    - Load identified restaurants into the restaurants table

    Args:
        content_tuple (tuple): Tuple containing URL, priority, and BeautifulSoup object.

    Returns:
        dict: Dictionary containing the transformed data
    """

    target_url, priority, soup = content_tuple

    # Step 1: Identify restaurants. Cross reference potential restaurants with built database
    validated_restaurants = []
    rejected_restaurants = []
    potential_restaurants = identify_restaurants(soup)  # All potential restaurants
    for restaurant in potential_restaurants:
        if is_restaurant(restaurant):
            validated_restaurants.append(restaurant)
        else:
            rejected_restaurants.append(restaurant)

    # Step 2: Identify derived urls
    homepage = extract_homepage(target_url)
    derived_urls = identify_urls_from_soup(soup, target_url)
    derived_urls = list(set(derived_urls) - {homepage})

    # Step 3: Estimate priority for each derived url, using the priority of the target url
    derived_url_pairs = []
    derived_url_pairs.append((homepage, min(100, priority)))
    for url in derived_urls:
        derived_url_pairs.append(
            (
                url,
                estimate_priority(
                    url,
                    validated_restaurants,
                    priority,
                ),
            )
        )

    relevance_score = estimate_relevance(soup, validated_restaurants)

    # Step 4: Pass all data to loader
    payload = {
        "target_url": target_url,  # Main URL
        "relevance_score": relevance_score,  # For the future
        "derived_url_pairs": derived_url_pairs,  # For loading to the priority queue
        "identified_restaurants": validated_restaurants,  # For loading to the restaurants table
        "rejected_restaurants": rejected_restaurants,  # For manual review
    }
    data_loading_queue.put(payload)
