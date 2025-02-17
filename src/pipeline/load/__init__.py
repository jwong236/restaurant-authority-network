from database.db_connector import get_db_connection
from database.db_operations import (
    check_url_exists,
    check_restaurant_exists,
    insert_into_priority_queue,
    insert_domain,
    insert_source,
    insert_url,
    insert_restaurant,
    insert_reference,
)
from urllib.parse import urlparse


def load_derived_urls(derived_url_pairs, cur):
    """
    Load derived URLs into the priority queue.
    """
    for url, priority in derived_url_pairs:
        if check_url_exists(url, cur):
            continue
        insert_into_priority_queue(url, priority, cur)


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
    """ """
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
        load_derived_urls(derived_url_pairs, cur)
        url_id = load_target_url(target_url, relevance_score, cur)
        for identified_restaurant in identified_restaurants:
            if check_restaurant_exists(identified_restaurant, cur):
                continue
        identified_restaurant_id_list = load_identified_restaurants(
            identified_restaurants, cur
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
