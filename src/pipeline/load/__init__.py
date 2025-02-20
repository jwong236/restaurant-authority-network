from database.db_connector import get_db_connection
from database.db_operations import (
    check_restaurant_exists,
    insert_domain,
    insert_source,
    insert_url,
    insert_restaurant,
    insert_reference,
)
from urllib.parse import urlparse
from queue_manager.task_queues import restaurant_search_queue, url_validate_queue


def extract_domain(url):
    """
    Extract the domain from a given URL.
    """
    parsed_url = urlparse(url)
    return parsed_url.netloc.split(":")[0]


def load_target_url(target_url, relevance_score, cur):
    """
    Extracted data into the database.
    """
    domain = extract_domain(target_url)
    domain_id = insert_domain(domain, cur)
    source_id = insert_source(domain_id, relevance_score, cur)
    url_id = insert_url(target_url, source_id, cur)
    return url_id


def load_identified_restaurants(identified_restaurants, cur):
    """
    Load identified restaurants into the restaurants table.
    """
    id_list = []
    for restaurant in identified_restaurants:
        id_list.append(insert_restaurant(restaurant, cur))
    return id_list


def load_rejected_restaurants(rejected_restaurants, cur):
    """
    Load rejected restaurants into the rejected restaurants table.
    """
    pass


def load_reference(restaurant_id, url_id, cur):
    """
    Load the reference between a restaurant and a URL.
    """
    restaurant_id, url_id = insert_reference(restaurant_id, url_id, cur)
    return restaurant_id, url_id


def load_data(payload):
    """
    4 Main goals:
        1. Derived urls that go to the validator
        2. Restaurants that exist are discarded
        3. Restaurants that are new go to the restaurant table
        4. Reference data for the target url is loaded to the database

    Args:
        payload (dict): Dictionary containing the transformed data

    Returns:
        dict: Dictionary containing the transformed data
    """
    (
        target_url,
        relevance_score,
        derived_url_pairs,
        identified_restaurants,
        rejected_restaurants,
    ) = payload

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        url_id = load_target_url(target_url, relevance_score, cur)

        new_identified_restaurants = []
        for identified_restaurant in identified_restaurants:
            restaurant_id = check_restaurant_exists(identified_restaurant, cur)
            if restaurant_id:
                # 2. Restaurants that exist are discarded
                continue
            # 3. Restaurants that are new go to the restaurant table
            new_identified_restaurants.append(identified_restaurant)
        identified_restaurant_id_list = load_identified_restaurants(
            new_identified_restaurants, cur
        )
        rejected_restaurant_id_list = load_rejected_restaurants(
            rejected_restaurants, cur
        )  # TODO: Load rejected restaurants into a quarantined table
        reference_id_list = []
        for restaurant_id in identified_restaurant_id_list:
            # 4. Reference data for the target URL is loaded to the database
            reference_id_list.append(load_reference(restaurant_id, url_id, cur))

        conn.commit()

        # In the future rejected restaurants will be sent to a new phase which checks if they are valid and worth searching in the search phase.
        # for rejected_restaurant in rejected_restaurants:
        #     restaurant_search_queue.put(rejected_restaurant)

        for url, relevance_score in derived_url_pairs:
            url_validate_queue.put((url, relevance_score))

    except Exception as e:
        print(f"Error loading data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
