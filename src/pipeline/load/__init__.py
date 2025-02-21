import logging
from urllib.parse import urlparse

from database.db_connector import get_db_connection
from database.db_operations import (
    check_restaurant_exists,
    insert_restaurant,
    insert_reference,
    check_url_exists,
    insert_into_restaurant_priority_queue,
)
from queue_manager.task_queues import url_validate_queue


def extract_domain(url):
    if "://" not in url:
        url = "http://" + url
    parsed = urlparse(url)
    return parsed.netloc.split(":")[0]


def adjust_domain_quality(old_score, relevance):
    if isinstance(relevance, str):
        relevance = float(relevance)
    delta = (relevance - 0.5) * 0.2
    new_score = old_score + delta
    return max(-1.0, min(new_score, 1.0))


def load_identified_restaurants(identified_restaurants, conn):
    ids = []
    for r_dict in identified_restaurants:
        r_name = r_dict.get("name", "")
        r_addr = r_dict.get("address", "")
        logging.info(f"Inserting new restaurant: {r_name}")
        rest_id = insert_restaurant(r_name, r_addr, conn)
        if rest_id:
            ids.append(rest_id)
        else:
            logging.warning(f"Failed to insert restaurant {r_name}")
    return ids


def load_rejected_restaurants(rejected_restaurants, relevance_score, conn):
    for rejected in rejected_restaurants:
        insert_into_restaurant_priority_queue(rejected, relevance_score * 100, conn)
        logging.info(
            f"Rejected restaurant: {rejected}, added to priority queue with priority {relevance_score}"
        )

    return []


def load_reference(restaurant_id, url_id, conn, relevance=0.9):
    logging.info(f"Linking restaurant {restaurant_id} with URL {url_id}")
    ref_id = insert_reference(restaurant_id, url_id, relevance, conn)
    return ref_id


def load_data(payload):
    """
    Load data into the database from the pipeline.

    Args:
        payload (dict): The payload containing the data to load.

    Returns:
        None
    """
    target_url = payload["target_url"]
    relevance_score = payload["relevance_score"]
    derived_url_pairs = payload["derived_url_pairs"]
    identified_restaurants = payload["identified_restaurants"]
    rejected_restaurants = payload["rejected_restaurants"]

    conn = get_db_connection()
    try:
        logging.info("Loading data into DB.")
        url_id = check_url_exists(target_url, conn)
        if not url_id:
            logging.error(f"Failed to load target URL: {target_url}")
            return

        new_rest_list = []
        for r_obj in identified_restaurants:
            name = r_obj.get("name", "")
            exists_id = check_restaurant_exists(name, conn)
            if exists_id:
                logging.info(f"Restaurant '{name}' exists, skipping.")
                continue
            new_rest_list.append(r_obj)

        new_rest_ids = load_identified_restaurants(new_rest_list, conn)
        load_rejected_restaurants(rejected_restaurants, relevance_score, conn)

        for rid in new_rest_ids:
            load_reference(rid, url_id, conn, relevance_score)

        conn.commit()
        logging.info("Data load committed successfully.")

        for new_url, new_rel_score in derived_url_pairs:
            url_validate_queue.put((new_url, new_rel_score))
            logging.info(
                f"Enqueued derived URL: {new_url} with relevance {new_rel_score}"
            )

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        conn.rollback()
    finally:
        conn.close()
