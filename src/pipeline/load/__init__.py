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
from queue_manager.task_queues import validate_queue

PHASE = "LOAD"


def extract_domain(url):
    """Extracts the domain from a URL."""
    if "://" not in url:
        url = "http://" + url
    return urlparse(url).netloc.split(":")[0]


def adjust_domain_quality(old_score, relevance):
    """Adjusts the quality score of a domain based on relevance."""
    if isinstance(relevance, str):
        relevance = float(relevance)
    delta = (relevance - 0.5) * 0.2
    return max(-1.0, min(old_score + delta, 1.0))


def load_identified_restaurants(identified_restaurants, conn):
    """Loads validated restaurants into the database."""
    ids = []
    for r_dict in identified_restaurants:
        r_name = r_dict.get("name", "")
        r_addr = r_dict.get("address", "")
        logging.info(f"[{PHASE}]: Inserting new restaurant: {r_name}")
        rest_id = insert_restaurant(r_name, r_addr, conn)
        if rest_id:
            ids.append(rest_id)
        else:
            logging.warning(f"[{PHASE}]: Failed to insert restaurant {r_name}")
    return ids


def load_rejected_restaurants(rejected_restaurants, relevance_score, conn):
    """Adds rejected restaurants to the priority queue with adjusted scores."""
    for rejected in rejected_restaurants:
        insert_into_restaurant_priority_queue(rejected, relevance_score * 100, conn)
        logging.info(
            f"[{PHASE}]: Rejected restaurant '{rejected}', added to priority queue with priority {relevance_score * 100:.2f}"
        )


def load_reference(restaurant_id, url_id, conn, relevance=0.9):
    """Links a restaurant to a reference URL in the database."""
    logging.info(f"[{PHASE}]: Linking restaurant {restaurant_id} with URL {url_id}")
    return insert_reference(restaurant_id, url_id, relevance, conn)


def load_data(payload):
    """
    Loads extracted and transformed data into the database.

    Args:
        payload (dict): The processed data from the pipeline.

    Returns:
        None
    """
    target_url = payload["target_url"]
    relevance_score = payload["relevance_score"]
    derived_url_pairs = payload["derived_url_pairs"]
    identified_restaurants = payload["identified_restaurants"]
    rejected_restaurants = payload["rejected_restaurants"]

    conn = get_db_connection()
    processed_count = 0

    try:
        logging.info(f"[{PHASE}]: Processing data for {target_url}")

        # Check if target URL exists
        logging.info(f"[{PHASE}]: Checking existence of target URL: {target_url}")
        url_id = check_url_exists(target_url, conn)
        if not url_id:
            logging.error(f"[{PHASE}]: Failed to load target URL: {target_url}")
            return

        # Identify new restaurants
        new_rest_list = []
        skipped_count = 0
        for r_obj in identified_restaurants:
            name = r_obj.get("name", "")
            exists_id = check_restaurant_exists(name, conn)
            if exists_id:
                logging.info(f"[{PHASE}]: Restaurant '{name}' exists, skipping.")
                skipped_count += 1
                continue
            new_rest_list.append(r_obj)

        logging.info(
            f"[{PHASE}]: Identified {len(new_rest_list)} new restaurants, skipping {skipped_count} existing."
        )

        # Insert new restaurants
        new_rest_ids = load_identified_restaurants(new_rest_list, conn)
        logging.info(f"[{PHASE}]: Inserted {len(new_rest_ids)} new restaurants.")

        # Process rejected restaurants
        load_rejected_restaurants(rejected_restaurants, relevance_score, conn)
        logging.info(
            f"[{PHASE}]: Processed {len(rejected_restaurants)} rejected restaurants."
        )

        # Create references for new restaurants
        for rid in new_rest_ids:
            load_reference(rid, url_id, conn, relevance_score)

        conn.commit()
        logging.info(f"[{PHASE}]: Data load committed successfully.")

        # Enqueue derived URLs for validation
        for new_url, new_rel_score in derived_url_pairs:
            validate_queue.put((new_url, new_rel_score))

        logging.info(
            f"[{PHASE}]: Enqueued {len(derived_url_pairs)} derived URLs for validation."
        )
        processed_count += 1

    except Exception as e:
        logging.error(f"[{PHASE}]: Error loading data: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()
        logging.info(f"[{PHASE}]: Completed. Processed {processed_count} tasks.")
        print(f"[{PHASE}]: Completed. Processed {processed_count} tasks.")
