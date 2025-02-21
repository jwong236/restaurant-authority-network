import queue
import logging
import time
import threading
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
    logging.info("--- Queue States ---")
    for name, q in queues.items():
        logging.info(f"{name}: {len(q.queue)} task(s)")
    logging.info("--------------------")


def get_db_queue_size():
    """Example of checking DB queue size for extraction."""
    conn = get_db_connection()
    count = get_url_priority_queue_length(conn)
    conn.close()
    return count


def process_extraction_task():
    """Continuously processes extraction tasks from the DB queue until empty."""
    conn = get_db_connection()
    logging.info("--- Starting Extraction Phase in background thread ---")
    while get_url_priority_queue_length(conn) > 0:
        try:
            extract_content()
        except Exception as e:
            logging.error(f"Error processing extraction task: {e}")
    logging.info("Finished Extraction Phase.\n")
    conn.close()


def process_task(phase_name, task_queue, processor):
    """
    Run a phase by emptying its queue with processor(task).
    """
    logging.info(f"--- Starting {phase_name} Phase in background thread ---")
    if task_queue.empty():
        logging.info(f"No tasks in {phase_name}, skipping.")
        return

    while not task_queue.empty():
        try:
            task = task_queue.get(timeout=5)
            processor(task)
            task_queue.task_done()
        except queue.Empty:
            break
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


def run_phase_in_thread(phase_name, task_queue, func):
    """
    Creates a background thread for the given phase, starts it,
    and returns the Thread object. Main thread can wait for it to finish.
    """
    if phase_name == "Extraction":
        # Special extraction logic that pulls from DB queue
        t = threading.Thread(target=process_extraction_task, daemon=True)
    else:
        t = threading.Thread(
            target=process_task, args=(phase_name, task_queue, func), daemon=True
        )
    t.start()
    return t


def main():
    restaurant_json_path = "michelin_restaurants.json"
    progress_tracker_path = "progress_tracker.json"

    logging.info("Starting pipeline with multithreading.")
    print_queue_contents()

    # Initialization
    input("Press Enter to initialize...\n")
    initialize(restaurant_json_path, progress_tracker_path)
    print_queue_contents()

    phase_flow = [
        ("Search", restaurant_search_queue, search_engine_search),
        ("Validation", url_validate_queue, validate_url),
        ("Extraction", content_extraction_queue, extract_content),
        ("Transformation", text_transformation_queue, transform_data),
        ("Loading", data_loading_queue, load_data),
    ]

    for phase_name, q, func in phase_flow:
        # Start the phase in a background thread
        logging.info(f"Starting {phase_name} in a background thread.")
        thread = run_phase_in_thread(phase_name, q, func)

        # Every minute, or until done, let user see status and optionally press Enter
        # We break once the phase thread completes
        start_time = time.time()
        while thread.is_alive():
            elapsed = time.time() - start_time
            if elapsed > 60:
                # Print queue status & DB queue
                logging.info(f"--- {phase_name} is still running... checking status.")
                print_queue_contents()
                dbsize = get_db_queue_size()
                logging.info(f"DB priority queue size: {dbsize}")
                start_time = time.time()

            # Non-blocking check if user pressed Enter
            # Easiest is to do a small sleep and let user press Ctrl+C or something
            # Or we can do input in a separate thread. For simplicity, we just do:
            time.sleep(2)  # poll every 2s

        logging.info(f"{phase_name} phase thread completed.")
        print_queue_contents()

    logging.info("All phases complete!\n")


if __name__ == "__main__":
    main()
