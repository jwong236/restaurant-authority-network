import queue
import logging
import time
from dotenv import load_dotenv
from queue_manager.task_queues import (
    restaurant_search_queue,
    url_validate_queue,
    content_extraction_queue,
    text_transformation_queue,
    data_loading_queue,
)
from pipeline.initialize import get_restaurant_batch
from pipeline.search import search_engine_search
from pipeline.validate import validate_url
from pipeline.extract import extract_content
from pipeline.transform import transform_data
from pipeline.load import load_data
from utils.setup_logging import setup_logging
from database.db_operations import get_url_priority_queue_length
from database.db_connector import get_db_connection

load_dotenv()
setup_logging(log_filename="pipeline.log", log_level=logging.INFO, log_to_console=True)

queues = {
    "restaurant_search_queue": restaurant_search_queue,
    "url_validate_queue": url_validate_queue,
    "content_extraction_queue": content_extraction_queue,
    "text_transformation_queue": text_transformation_queue,
    "data_loading_queue": data_loading_queue,
}


def print_queue_contents():
    logging.info(f"--- Queue States ---")
    for name, q in queues.items():
        logging.info(f"{name}: {len(q.queue)} task(s)")
    logging.info(f"--------------------")


def process_extraction_task(func):
    """Processes extraction tasks dynamically from the database priority queue."""
    conn = get_db_connection()
    logging.info("--- Starting Extraction Phase ---")

    while get_url_priority_queue_length(conn) > 0:
        try:
            func()
        except Exception as e:
            logging.error(f"Error processing extraction task: {e}")

    conn.close()
    logging.info("Finished Extraction Phase.\n")


def process_task(phase_name, task_queue, func):
    if phase_name == "Extraction":
        process_extraction_task(func)
        return

    logging.info(f"--- Starting {phase_name} Phase ---")
    if task_queue.empty():
        logging.info(f"No tasks in {phase_name}, skipping.")
        return

    while not task_queue.empty():
        task = task_queue.get(timeout=5)
        func(task)
        task_queue.task_done()
    logging.info(f"Finished {phase_name} Phase.\n")


def initialize(
    restaurant_json_path="michelin_restaurants.json",
    progress_tracker_path="progress_tracker.json",
):
    restaurant_list = get_restaurant_batch(
        restaurant_json_path, progress_tracker_path, 1
    )
    for r in restaurant_list:
        r["initial_search"] = True
        restaurant_search_queue.put(r)
        logging.info(f"Added to search queue: {r}")
    logging.info(f"Initialized with {len(restaurant_list)} restaurants.")


def main():
    restaurant_json_path = "michelin_restaurants.json"
    progress_tracker_path = "progress_tracker.json"

    logging.info("Starting pipeline with one worker per phase.")
    print_queue_contents()
    input("Press Enter to begin...\n")

    initialize(restaurant_json_path, progress_tracker_path)
    print_queue_contents()

    phase_flow = [
        ("Search", restaurant_search_queue, search_engine_search),
        ("Validation", url_validate_queue, validate_url),
        ("Extraction", content_extraction_queue, extract_content),
        ("Transformation", text_transformation_queue, transform_data),
        ("Loading", data_loading_queue, load_data),
    ]

    for phase_name, queue, func in phase_flow:
        input(f"Press Enter to run {phase_name}...\n")
        process_task(phase_name, queue, func)
        print_queue_contents()

    logging.info("All phases complete!")


if __name__ == "__main__":
    main()
