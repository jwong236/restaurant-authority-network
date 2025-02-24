import logging
import time
from urllib.parse import urlparse
from database.db_connector import get_db_connection
from database.db_operations import (
    check_restaurant_exists,
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


def load_rejected_restaurants(rejected_restaurants, relevance_score, conn):
    """Adds rejected restaurants to the priority queue with adjusted scores."""
    for i, rejected in enumerate(rejected_restaurants, 1):
        priority_score = relevance_score * 100
        logging.info(
            f"[{PHASE}]: ({i}/{len(rejected_restaurants)}) Adding '{rejected}' to priority queue with priority {priority_score:.2f}"
        )
        # insert_into_restaurant_priority_queue(rejected, priority_score, conn) TODO: Implement phase to decide if a restaurant mention is actually a restaurant
    logging.info(
        f"[{PHASE}]: Processed {len(rejected_restaurants)} rejected restaurants."
    )


def load_reference(restaurant_id, url_id, conn, relevance=0.9):
    """Links a restaurant to a reference URL in the database."""
    logging.info(
        f"[{PHASE}]: Linking restaurant {restaurant_id} with URL {url_id} (Relevance: {relevance})"
    )
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
    validated_restaurants = payload[
        "identified_restaurants"
    ]  # Valiated restaurants. Guaranteed to be in database
    potential_restaurants = payload[
        "rejected_restaurants"
    ]  # Restaurants that aren't in database. May still be valid

    conn = get_db_connection()
    processed_count = 0

    try:
        logging.info(
            f"[{PHASE}]: {target_url} - {len(validated_restaurants)} mentions found in database, {len(potential_restaurants)} mentions not found in database."
        )
        logging.info(f"[{PHASE}]: Validated mentions: {validated_restaurants}")
        logging.info(f"[{PHASE}]: Potential mentions: {potential_restaurants}")
        if len(validated_restaurants) == 0:
            logging.warning(
                f"[{PHASE}]: {target_url} - No validated mentions. No references will be created."
            )
        url_id = check_url_exists(target_url, conn)
        if not url_id:
            logging.error(
                f"[{PHASE}]: {target_url} - reached load phase without a valid URL ID."
            )
            return
        for i, restaurant in enumerate(validated_restaurants):
            exists_id = check_restaurant_exists(restaurant, conn)
            if exists_id:
                load_reference(exists_id, url_id, conn, relevance_score)
                logging.info(
                    f"[{PHASE}]: {target_url} - Linked to {restaurant} in reference"
                )
            else:
                logging.error(
                    f"[{PHASE}]: {target_url} - Restaurant {restaurant} not found in database."
                )

        # Process rejected restaurants
        load_rejected_restaurants(potential_restaurants, relevance_score, conn)
        # TODO: Issue: Rejected restaurants should be paired with URL they were found in to not lose the potential reference

        # Enqueue derived URLs for validation
        for i, (new_url, new_rel_score) in enumerate(derived_url_pairs, 1):
            logging.info(
                f"[{PHASE}]: ({i}/{len(derived_url_pairs)}) Enqueuing derived URL: {new_url} with relevance {new_rel_score}"
            )
            validate_queue.put((new_url, new_rel_score))

        conn.commit()
        processed_count += 1

    except Exception as e:
        logging.error(f"[{PHASE}]: {target_url} - ERROR: {e}")
        conn.rollback()
    finally:
        conn.close()
        logging.info(f"[{PHASE}]: Connection closed.")
        logging.info(f"[{PHASE}]: Completed. Processed {processed_count} tasks.")
        print(f"[{PHASE}]: Completed. Processed {processed_count} tasks.")
