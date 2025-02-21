import logging
import os
import time
import requests
from dotenv import load_dotenv
from queue_manager.task_queues import validate_queue

load_dotenv()
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"

PHASE = "SEARCH"


def search_engine_search(restaurant_data, result_size=20):
    """Queries the Brave Web Search API to retrieve relevant URLs for a restaurant."""

    if not BRAVE_API_KEY:
        raise ValueError("BRAVE_API_KEY is missing from .env")

    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }

    restaurant_name = restaurant_data["name"].strip()
    location = restaurant_data["location"].strip()

    queries = [
        f"{restaurant_name} {location} restaurant review",
        f'review "{restaurant_name}" {location}',
    ]

    all_urls = set()
    query_url_counts = []

    logging.info(f"[{PHASE}]: Processing restaurant: '{restaurant_name}' in {location}")

    for query_idx, query in enumerate(queries, start=1):
        logging.info(f"[{PHASE}]: Query {query_idx}: '{query}'")

        params = {"q": query, "count": min(result_size, 20), "text_decorations": False}

        for attempt in range(5):
            try:
                response = requests.get(
                    BRAVE_SEARCH_URL, headers=headers, params=params, timeout=10
                )
                response.raise_for_status()

                search_results = response.json()
                urls = [
                    result["url"]
                    for result in search_results.get("web", {}).get("results", [])
                ]
                all_urls.update(urls)

                query_url_counts.append((query, len(urls)))
                logging.info(
                    f"[{PHASE}]: Query {query_idx} retrieved {len(urls)} URLs."
                )

                time.sleep(2)
                break

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    wait_time = min(2**attempt, 30)
                    logging.warning(
                        f"[{PHASE}]: 429 Too Many Requests. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logging.error(f"[{PHASE}]: HTTP error {response.status_code}: {e}")
                    break
            except requests.exceptions.RequestException as e:
                logging.error(f"[{PHASE}]: Network error: {e}")
                break

    # Enqueue validated URLs
    for url in all_urls:
        validate_queue.put((url, 1.0 if restaurant_data["initial_search"] else 0.99))

    # Final phase summary
    print(f"[{PHASE}]: Completed. Identified {len(all_urls)} total URLs.")
    logging.info(f"[{PHASE}]: Completed. Identified {len(all_urls)} total URLs.")

    # Log individual query results
    for query_text, url_count in query_url_counts:
        logging.info(f"[{PHASE}]: Query '{query_text}' → {url_count} URLs")
