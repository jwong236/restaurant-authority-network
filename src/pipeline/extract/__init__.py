import requests
from bs4 import BeautifulSoup
from database.db_connector import get_db_connection
from database.db_operations import (
    get_priority_queue_url,
    update_priority_queue_url,
    remove_from_url_priority_queue,
)
from queue_manager.task_queues import text_transformation_queue


def request_url(url):
    """Request the URL with a user-agent, return a Response or None."""
    headers = {"User-Agent": "MyUserAgent/1.0 (+https://github.com/jwong236)"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        print(f"Request failed for {url}: {e}")
        return None


def handle_http_status(conn, url_id, priority, response):
    """Handles HTTP status codes: removes, requeues, or continues processing."""
    status = response.status_code
    if status == 404:
        print(f"Page not found (404). Removing {url_id} from queue.")
        remove_from_url_priority_queue(url_id, conn)
        return False
    elif 400 <= status < 500:
        print(f"Client error ({status}). Removing {url_id}.")
        remove_from_url_priority_queue(url_id, conn)
        return False
    elif status in {500, 502, 503, 504}:
        new_priority = max(0, priority * 0.75)
        print(f"Temporary error ({status}). Lowering priority => {new_priority}")
        update_priority_queue_url(conn, url_id, new_priority)
        return False
    return True


def extract_content():
    """
    Pops a URL from the db queue, extracts content, and enqueues for transformation.

    Returns:
        bool: True if a URL was processed, False if the DB queue was empty or failed early.
    """
    conn = get_db_connection()

    try:
        # 1) Get the highest priority URL
        result = get_priority_queue_url(conn)
        if not result:
            print("No URLs in priority queue.")
            return False

        url_id, full_url, priority = result

        # 2) Request the page
        resp = request_url(full_url)
        if not resp:
            print(f"Request failed, removing {url_id} from queue.")
            remove_from_url_priority_queue(url_id, conn)
            return False
        print(f"Request successful for {full_url}")

        # 3) Check status and re-queue or remove
        if not handle_http_status(conn, url_id, priority, resp):
            return False

        # 4) Parse HTML
        soup = BeautifulSoup(resp.text, "html.parser")
        if not soup or not soup.body or len(soup.get_text(strip=True)) < 10:
            print(f"Skipping {full_url}: No meaningful content found.")
            remove_from_url_priority_queue(url_id, conn)
            return False

        # 5) Successful extraction remove from DB queue
        remove_from_url_priority_queue(url_id, conn)

        # 6) Enqueue to next phase
        text_transformation_queue.put((full_url, priority, soup))
        return True

    except Exception as e:
        print(f"Extraction failed: {e}")
        return False
    finally:
        conn.close()
