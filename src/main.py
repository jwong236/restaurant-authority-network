import logging
import time
import threading
from dotenv import load_dotenv

from queue_manager.task_queues import (
    search_queue,
    validate_queue,
    transform_queue,
    load_queue,
)
from queue_manager.pipeline_helpers import print_queue_contents, initialize_restaurants
from pipeline.search import search_engine_search
from pipeline.validate import validate_url
from pipeline.extract import extract_content
from pipeline.transform import transform_data
from pipeline.load import load_data
from utils.setup_logging import setup_logging
from database.db_connector import get_db_connection
from queue_manager.worker import worker, extract_worker


def main():
    load_dotenv()
    setup_logging(
        log_filename="pipeline.log", log_level=logging.INFO, log_to_console=False
    )
    conn = get_db_connection()
    logging.info("[PIPELINE]: Starting multi-threaded pipeline.")

    queues = {
        "search_queue": search_queue,
        "validate_queue": validate_queue,
        "transform_queue": transform_queue,
        "load_queue": load_queue,
    }

    print_queue_contents(conn, queues)
    initialize_restaurants()
    print_queue_contents(conn, queues)

    threads = []
    NUM_SEARCH_WORKERS = 3
    NUM_VALIDATE_WORKERS = 3
    NUM_TRANSFORM_WORKERS = 3
    NUM_LOAD_WORKERS = 3
    NUM_EXTRACT_WORKERS = 3

    for i in range(NUM_SEARCH_WORKERS):
        t = threading.Thread(
            target=worker,
            args=(search_queue, search_engine_search, f"SEARCH_WORKER_{i+1}"),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for i in range(NUM_VALIDATE_WORKERS):
        t = threading.Thread(
            target=worker,
            args=(validate_queue, validate_url, f"VALIDATE_WORKER_{i+1}"),
            daemon=True,
        )
        t.start()
        threads.append(t)

    stop_event = threading.Event()
    for i in range(NUM_EXTRACT_WORKERS):
        t = threading.Thread(
            target=extract_worker,
            args=(extract_content, stop_event),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for i in range(NUM_TRANSFORM_WORKERS):
        t = threading.Thread(
            target=worker,
            args=(transform_queue, transform_data, f"TRANSFORM_WORKER_{i+1}"),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for i in range(NUM_LOAD_WORKERS):
        t = threading.Thread(
            target=worker,
            args=(load_queue, load_data, f"LOAD_WORKER_{i+1}"),
            daemon=True,
        )
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(10)
            print_queue_contents(conn, queues)
    except KeyboardInterrupt:
        logging.info("[PIPELINE]: Keyboard interrupt. Shutting down...")
        stop_event.set()

    for q in queues.values():
        q.put(None)

    for t in threads:
        t.join()

    conn.close()
    logging.info("[PIPELINE]: All phases complete! Shutting down.")


if __name__ == "__main__":
    main()
