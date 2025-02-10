import requests
from bs4 import BeautifulSoup
from database.db_connector import get_db_connection
from database.db_operations import (
    get_priority_queue_url,
    update_priority_queue_url,
    remove_priority_queue_url,
)


def request_url(url):
    """Make a request to the URL and return the response object."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        print(f"Request failed for {url}: {e}")
        return None


def reprioritize_url(url, priority, response, cur):
    """Adjust priority based on response status code."""
    status = response.status_code

    if status == 404:
        print(f"Page not found (404). Removing {url} from queue.")
        remove_priority_queue_url(url, cur)
        return None
    elif status in {403, 500, 502, 503, 504}:
        new_priority = priority * 0.75
        print(f"Temporary error ({status}). Reducing priority for {url}.")
        update_priority_queue_url(cur, url, new_priority)
        return None

    return response


def extract_content():
    """
    Extract content from the URL and return (url, priority, soup).
    Ensures that extracted content is valid and meaningful.
    """
    connect = get_db_connection()
    cur = connect.cursor()

    try:
        result = get_priority_queue_url(cur)
        if not result:
            print("No URLs in priority queue.")
            return None

        url, priority = result

        response = request_url(url)
        response = reprioritize_url(url, priority, response, cur)

        if not response:
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        if not soup or not soup.body or len(soup.get_text(strip=True)) < 100:
            print(f"Skipping {url}: No meaningful content found.")
            return None

        remove_priority_queue_url(url, cur)
        return url, priority, soup
    except Exception as e:
        print(f"Extraction failed: {e}")
        return None
    finally:
        cur.close()
        connect.close()
