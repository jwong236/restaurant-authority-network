import logging
from database.db_operations import get_url_priority_queue_length
import time
from database.db_operations import (
    get_url_priority_queue_length,
)
from database.db_connector import get_db_connection


def worker(queue, func, worker_name="WORKER"):
    """
    A generic worker loop that continuously pulls tasks from `queue`,
    calls `pipeline_func(item)`, and acknowledges completion.

    The pipeline function itself is responsible for enqueuing any
    follow-up tasks to subsequent queues.
    """
    while True:
        item = queue.get()
        if item is None:
            queue.task_done()
            logging.info(f"[{worker_name}] Received shutdown signal.")
            break

        try:
            logging.info(f"[{worker_name}] Starting task")
            func(item)
            logging.info(f"[{worker_name}] Task complete.")
        except Exception as e:
            logging.error(f"[{worker_name}] Error: {e}")
        finally:
            queue.task_done()


def extract_worker(func, stop_event, poll_interval=5):
    conn = get_db_connection()
    while not stop_event.is_set():
        try:
            if get_url_priority_queue_length(conn) > 0:
                logging.info("[EXTRACT_WORKER] Starting task")
                func()
                logging.info("[EXTRACT_WORKER] Task complete.")
            else:
                time.sleep(poll_interval)
        except Exception as e:
            logging.error(f"[EXTRACT_WORKER] Error: {e}")
            time.sleep(poll_interval)
    logging.info("[EXTRACT_WORKER] Shutting down gracefully.")
