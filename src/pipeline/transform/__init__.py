from .utils import identify_urls_from_soup
from database.db_operations import check_restaurant_exists, fuzzy_search_restaurant_name
from .classify_webpage import classify_webpage
from .identify_restaurants import identify_restaurants


def is_restaurant(restaurant):
    if check_restaurant_exists(restaurant):
        return True
    if fuzzy_search_restaurant_name(restaurant)["confidence"] > 0.5:
        return True
    return False


def estimate_priority(priority, webpage_type):
    priority_adjustments = {
        "simple_review": -10,
        "aggregated_review_list": -15,
        "simple_review_list": -20,
        "hub_page": -30,
    }
    return max(
        0, priority + priority_adjustments.get(webpage_type, -50)
    )  # Prevent negative priority


def transform_data(content_tuple):
    """
    Main function for transforming extracted data.
    content_tuple = (url, priority, soup)
    Goal:
    - Load data for target reference into the reference table
    - Load data for derived urls into the priotity queue
    - Load identified restaurants into the restaurants table
    """

    target_url, priority, soup = content_tuple
    relevant_webpage_types = [
        "simple_review",
        "aggregated_review_list",
        "simple_review_list",
    ]

    # Step 1: Classify webpage type (simple_review, aggregated_review_list, simple_review_list, hub_page, irrelevant)
    webpage_type, relevance_score = classify_webpage(soup)
    if webpage_type == "irrelevant":
        return None

    # Step 2: Identify derived urls
    derived_urls = identify_urls_from_soup(soup)

    # Step 3: Estimate priority for each derived url, using the priority of the target url
    derived_url_pairs = []
    for url in derived_urls:
        derived_url_pairs.append((url, estimate_priority(priority, webpage_type)))

    # Step 4: Identify restaurants
    validated_restaurants = []
    other_restaurants = []
    if webpage_type in relevant_webpage_types:
        potential_restaurants = identify_restaurants(
            webpage_type, soup
        )  # All potential restaurants
        validated_restaurants = (
            []
        )  # Restaurants that are in the database (confirmed to be restaurants)
        other_restaurants = (
            []
        )  # Restaurants that are not in the database (possible new restaurants)
        for restaurant in potential_restaurants:
            if is_restaurant(restaurant):
                validated_restaurants.append(restaurant)
            else:
                other_restaurants.append(restaurant)

    # Step 5: Pass all data to loader
    # If its a simple_review page, the target_url refers to 1 restaurant
    # If its an aggregated_review_list, the target_url refers to multiple restaurants
    # If its a review list, the target_url refers to multiple restaurants
    # If its a hub_page, no target_url is made
    # If its irrelevant, no target_url is made
    payload = {
        "target_url": target_url,  # Main URL
        "webpage_type": webpage_type,  # Determines loading algorithm
        "relevance_score": relevance_score,  # For the future
        "derived_url_pairs": derived_url_pairs,  # For loading to the priority queue
        "identified_restaurants": validated_restaurants,  # For loading to the restaurants table
        "new_restaurants": other_restaurants,  # For manual review
    }
    return payload
