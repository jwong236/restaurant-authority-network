import logging
import time
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
    start_time = time.time()

    for i, restaurant in enumerate(identified_restaurants, 1):
        logging.info(
            f"[{PHASE}]: ({i}/{len(identified_restaurants)}) Inserting: {restaurant}"
        )
        rest_id = insert_restaurant(restaurant, None, conn)

        if rest_id:
            ids.append(rest_id)
        else:
            logging.warning(f"[{PHASE}]: Failed to insert restaurant {restaurant}")

    elapsed_time = time.time() - start_time
    logging.info(
        f"[{PHASE}]: Inserted {len(ids)} new restaurants in {elapsed_time:.2f}s."
    )
    return ids


def load_rejected_restaurants(rejected_restaurants, relevance_score, conn):
    """Adds rejected restaurants to the priority queue with adjusted scores."""
    start_time = time.time()

    for i, rejected in enumerate(rejected_restaurants, 1):
        priority_score = relevance_score * 100
        logging.info(
            f"[{PHASE}]: ({i}/{len(rejected_restaurants)}) Adding '{rejected}' to priority queue with priority {priority_score:.2f}"
        )
        insert_into_restaurant_priority_queue(rejected, priority_score, conn)

    elapsed_time = time.time() - start_time
    logging.info(
        f"[{PHASE}]: Processed {len(rejected_restaurants)} rejected restaurants in {elapsed_time:.2f}s."
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
    identified_restaurants = payload["identified_restaurants"]
    rejected_restaurants = payload["rejected_restaurants"]

    conn = get_db_connection()
    processed_count = 0

    try:
        logging.info(
            f"[{PHASE}]: ------------------ STARTING LOAD PHASE ------------------"
        )
        logging.info(f"[{PHASE}]: Processing data for {target_url}")
        logging.info(f"[{PHASE}]: BEGIN TRANSACTION")

        # Check if target URL exists
        logging.info(f"[{PHASE}]: Checking if target URL exists: {target_url}")
        url_id = check_url_exists(target_url, conn)
        if not url_id:
            logging.error(f"[{PHASE}]: Failed to load target URL: {target_url}")
            return

        # Identify new restaurants
        new_rest_list = []
        skipped_count = 0
        for i, restaurant in enumerate(identified_restaurants, 1):
            exists_id = check_restaurant_exists(restaurant, conn)
            if exists_id:
                logging.info(
                    f"[{PHASE}]: ({i}/{len(identified_restaurants)}) Skipping existing restaurant '{restaurant}'."
                )
                skipped_count += 1
                continue
            new_rest_list.append(restaurant)

        logging.info(
            f"[{PHASE}]: Identified {len(new_rest_list)} new restaurants, skipping {skipped_count} existing."
        )

        # Insert new restaurants
        new_rest_ids = load_identified_restaurants(new_rest_list, conn)

        # Process rejected restaurants
        load_rejected_restaurants(rejected_restaurants, relevance_score, conn)

        # Create references for new restaurants
        for i, rid in enumerate(new_rest_ids, 1):
            logging.info(
                f"[{PHASE}]: ({i}/{len(new_rest_ids)}) Creating reference for restaurant ID {rid}"
            )
            load_reference(rid, url_id, conn, relevance_score)

        # Enqueue derived URLs for validation
        start_time = time.time()
        for i, (new_url, new_rel_score) in enumerate(derived_url_pairs, 1):
            logging.info(
                f"[{PHASE}]: ({i}/{len(derived_url_pairs)}) Enqueuing derived URL: {new_url} with relevance {new_rel_score}"
            )
            validate_queue.put((new_url, new_rel_score))
        elapsed_time = time.time() - start_time
        logging.info(
            f"[{PHASE}]: Enqueued {len(derived_url_pairs)} derived URLs in {elapsed_time:.2f}s."
        )

        conn.commit()
        logging.info(f"[{PHASE}]: COMMIT TRANSACTION (All data saved successfully)")
        processed_count += 1

    except Exception as e:
        logging.error(f"[{PHASE}]: ERROR: {e}", exc_info=True)
        logging.info(f"[{PHASE}]: ROLLBACK TRANSACTION (Undoing changes due to error)")
        conn.rollback()
    finally:
        conn.close()
        logging.info(f"[{PHASE}]: Connection closed.")
        logging.info(f"[{PHASE}]: Completed. Processed {processed_count} tasks.")
        print(f"[{PHASE}]: Completed. Processed {processed_count} tasks.")
