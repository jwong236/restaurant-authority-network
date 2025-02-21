import logging
import requests
from bs4 import BeautifulSoup
from database.db_connector import get_db_connection
from database.db_operations import (
    get_priority_queue_url,
    update_priority_queue_url,
    remove_from_url_priority_queue,
)
from queue_manager.task_queues import transform_queue

PHASE = "EXTRACT"


def request_url(url):
    """Requests the URL with a user-agent, returns a Response or None."""
    headers = {"User-Agent": "MyUserAgent/1.0 (+https://github.com/jwong236)"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        logging.warning(f"[{PHASE}]: Request failed for {url}: {e}")
        return None


def handle_http_status(conn, url_id, priority, response):
    """Handles HTTP response codes: removes, requeues, or continues processing."""
    status = response.status_code
    if status == 404:
        logging.info(f"[{PHASE}]: Page not found (404). Removing {url_id} from queue.")
        remove_from_url_priority_queue(url_id, conn)
        return False
    elif 400 <= status < 500:
        logging.warning(f"[{PHASE}]: Client error ({status}). Removing {url_id}.")
        remove_from_url_priority_queue(url_id, conn)
        return False
    elif status in {500, 502, 503, 504}:
        new_priority = max(0, priority * 0.75)
        logging.info(
            f"[{PHASE}]: Temporary server error ({status}). Lowering priority to {new_priority}."
        )
        update_priority_queue_url(conn, url_id, new_priority)
        return False
    return True


def extract_content():
    """
    Processes a URL from the priority queue, extracts content, and enqueues for transformation.

    Returns:
        bool: True if a URL was successfully processed, False otherwise.
    """
    conn = get_db_connection()
    processed_count = 0

    try:
        logging.info(f"[{PHASE}]: Starting processing.")

        while True:
            # 1) Get the highest priority URL
            result = get_priority_queue_url(conn)
            if not result:
                logging.info(f"[{PHASE}]: No URLs in priority queue. Exiting.")
                break

            url_id, full_url, priority = result
            logging.info(f"[{PHASE}]: Processing URL: {full_url}")

            # 2) Request the page
            resp = request_url(full_url)
            if not resp:
                logging.info(
                    f"[{PHASE}]: Request failed, removing {url_id} from queue."
                )
                remove_from_url_priority_queue(url_id, conn)
                continue

            # 3) Handle HTTP status
            if not handle_http_status(conn, url_id, priority, resp):
                continue

            # 4) Parse HTML
            soup = BeautifulSoup(resp.text, "html.parser")
            if not soup or not soup.body or len(soup.get_text(strip=True)) < 10:
                logging.info(f"[{PHASE}]: Skipping {full_url} (No meaningful content).")
                remove_from_url_priority_queue(url_id, conn)
                continue

            # 5) Remove from priority queue
            remove_from_url_priority_queue(url_id, conn)

            # 6) Enqueue to transformation phase
            transform_queue.put((full_url, priority, soup))
            logging.info(
                f"[{PHASE}]: Successfully extracted content from {full_url}. Enqueued for transformation."
            )
            processed_count += 1

        logging.info(f"[{PHASE}]: Completed. Processed {processed_count} URLs.")
        print(f"[{PHASE}]: Completed. Processed {processed_count} URLs.")

        return processed_count > 0

    except Exception as e:
        logging.error(f"[{PHASE}]: Error extracting content: {e}", exc_info=True)
        return False
    finally:
        conn.close()
