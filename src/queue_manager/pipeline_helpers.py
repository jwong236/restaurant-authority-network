import logging

from database.db_operations import (
    get_url_priority_queue_length,
    get_restaurant_priority_queue_length,
)
from pipeline.initialize import get_restaurant_batch
from queue_manager.task_queues import search_queue


def print_queue_contents(conn, queues):
    url_count = get_url_priority_queue_length(conn)
    rest_count = get_restaurant_priority_queue_length(conn)
    log_message = (
        "--- Queue States ---\n"
        f"search_queue: {queues['search_queue'].qsize()} tasks\n"
        f"validate_queue: {queues['validate_queue'].qsize()} tasks\n"
        f"extract_queue: {url_count} tasks\n"
        f"transform_queue: {queues['transform_queue'].qsize()} tasks\n"
        f"load_queue: {queues['load_queue'].qsize()} tasks\n"
        f"verify_queue: {rest_count} tasks\n"
        "--------------------"
    )
    logging.info(log_message)


def initialize_restaurants(
    r_json="michelin_restaurants.json", progress="progress_tracker.json"
):
    """
    Fetch a small batch of restaurants and put them on the search_queue.
    """
    ph = "INITIALIZE"
    logging.info(f"[{ph}]: Fetching batch.")
    rlist = get_restaurant_batch(r_json, progress, 10)
    for r in rlist:
        r["initial_search"] = True
        search_queue.put(r)
        logging.info(f"[{ph}]: Added to search queue: {r}")
    logging.info(f"[{ph}]: Done. {len(rlist)} restaurants.")
