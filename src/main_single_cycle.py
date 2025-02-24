import logging
import time
from dotenv import load_dotenv
from queue_manager.task_queues import (
    search_queue,
    validate_queue,
    transform_queue,
    load_queue,
)

from pipeline.search import search_engine_search
from pipeline.validate import validate_url
from pipeline.extract import extract_content
from pipeline.transform import transform_data
from pipeline.load import load_data
from utils.setup_logging import setup_logging
from database.db_operations import (
    get_url_priority_queue_length,
)
from database.db_connector import get_db_connection
from queue_manager.pipeline_helpers import print_queue_contents, initialize_restaurants

load_dotenv()
setup_logging(log_filename="pipeline.log", log_level=logging.INFO, log_to_console=True)

queues = {
    "search_queue": search_queue,
    "validate_queue": validate_queue,
    "transform_queue": transform_queue,
    "load_queue": load_queue,
}


def process_extraction_task(func, conn):
    logging.info("[EXTRACT]: Starting DB-based extraction.")
    count = 0
    while get_url_priority_queue_length(conn) > 0:
        try:
            func()
            count += 1
        except Exception as e:
            logging.error(f"[EXTRACT]: Error: {e}")
    logging.info(f"[EXTRACT]: Completed. Processed {count} tasks.\n")


def process_task(phase_name, task_queue, func):
    logging.info(f"[{phase_name.upper()}]: Starting.")
    if task_queue.empty():
        logging.info(f"[{phase_name.upper()}]: No tasks found. Skipping.")
        return
    count = 0
    while not task_queue.empty():
        t = task_queue.get(timeout=5)
        func(t)
        task_queue.task_done()
        count += 1
    logging.info(f"[{phase_name.upper()}]: Completed. Processed {count} tasks.\n")


def main():
    conn = get_db_connection()
    logging.info("[PIPELINE]: Starting pipeline.")
    print_queue_contents(conn, queues)
    input("Press Enter to begin...\n")

    initialize_restaurants()
    print_queue_contents(conn, queues)

    phase_flow = [
        ("Search", search_queue, search_engine_search),
        ("Validate", validate_queue, validate_url),
        ("Extract", None, extract_content),
        ("Transform", transform_queue, transform_data),
        ("Load", load_queue, load_data),
    ]

    for name, q, func in phase_flow:
        # input(f"Press Enter to run {name}...\n")
        if name.lower() == "extract":
            process_extraction_task(func, conn)
        else:
            process_task(name, q, func)
        print_queue_contents(conn, queues)

    conn.close()
    logging.info("[PIPELINE]: All phases complete!")


if __name__ == "__main__":
    main()
