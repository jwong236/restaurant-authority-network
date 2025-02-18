from database.db_connector import get_db_connection
from database.db_operations import (
    check_url_exists,
    check_restaurant_exists,
    insert_domain,
    insert_source,
    insert_url,
    insert_restaurant,
    insert_reference,
)
from pipeline.validate import validate_url
from urllib.parse import urlparse


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
    5 Main goals:
        1. Derived URLs that exist are discarded
        2. Derived urls that are new go to the validator
        3. Restaurants that exist are discarded
        4. Restaurants that are new go to the restaurant table
        5. Reference data for the target url is loaded to the database
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

        for url, priority in derived_url_pairs:
            if check_url_exists(url, cur):
                # 1. Derived URLs that exist are discarded
                continue
            # 2. Derived URLs that are new go to the validator
            validate_url(url, priority, cur)

        url_id = load_target_url(target_url, relevance_score, cur)

        new_identified_restaurants = []
        for identified_restaurant in identified_restaurants:
            restaurant_id = check_restaurant_exists(identified_restaurant, cur)
            if restaurant_id:
                # 3. Restaurants that exist are discarded
                continue
            # 4. Restaurants that are new go to the restaurant table
            new_identified_restaurants.append(identified_restaurant)
        identified_restaurant_id_list = load_identified_restaurants(
            new_identified_restaurants, cur
        )
        rejected_restaurant_id_list = load_rejected_restaurants(
            rejected_restaurants, cur
        )  # TODO: Load rejected restaurants into a quarantined table
        reference_id_list = []
        for restaurant_id in identified_restaurant_id_list:
            reference_id_list.append(load_reference(restaurant_id, url_id, cur))

        conn.commit()
    except Exception as e:
        print(f"Error loading data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
