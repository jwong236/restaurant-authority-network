import os
import time
import requests
from dotenv import load_dotenv
from queue_manager.task_queues import url_validate_queue

load_dotenv()
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"


def search_engine_search(restaurant_data, result_size=20):
    """
    Queries the Brave Web Search API to retrieve relevant URLs for a given restaurant.

    Args:
        restaurant_data (dict): A dictionary containing:
            - "name" (str): The name of the restaurant.
            - "location" (str): The restaurant's location (city, state, or address).
        result_size (int, optional): The number of results to request from the API.
                                     Defaults to 20 (Brave's max limit).

    Returns:
        list: A list of pairs of URLs and their respective scores.
    """

    if not BRAVE_API_KEY:
        raise ValueError("BRAVE_API_KEY is missing from .env")

    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }

    restaurant_name = restaurant_data["name"].strip()
    location = restaurant_data["location"].strip()

    # Simplified query variants
    queries = [
        f"{restaurant_name} {location} review",
        f"{restaurant_name} restaurant review",
        f'review "{restaurant_name}" {location}',
    ]

    all_urls = set()

    for query in queries:
        print(f"🔍 Searching for: {query}")
        params = {
            "q": query,
            "count": min(result_size, 20),
            "text_decorations": False,
        }

        for attempt in range(5):
            try:
                response = requests.get(
                    BRAVE_SEARCH_URL, headers=headers, params=params, timeout=10
                )
                response.raise_for_status()
                if not response.content:
                    print(f"⚠️ Empty response for query: {query}")
                    break

                search_results = response.json()
                urls = [
                    result["url"]
                    for result in search_results.get("web", {}).get("results", [])
                ]
                all_urls.update(urls)
                print("🔗 URLs found:", len(urls))
                break

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    wait_time = min(2**attempt, 30)
                    print(f"⏳ Rate limited. Waiting {wait_time}s: {query}")
                    time.sleep(wait_time)
                elif response.status_code == 422:
                    print(f"🚨 Invalid query parameters: {query}")
                    print(f"Debug - Problematic query: {query}")
                    break
                else:
                    print(f"🔴 HTTP Error {response.status_code}: {e}")
                    break

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                print(f"🌐 Network error. Retry {attempt + 1}/5: {query}")
                time.sleep(5)

            except requests.exceptions.JSONDecodeError:
                print(f"📄 JSON decode failed: {response.text[:200]}")
                break
    # If initial_search is True then each url is given a score of 100. Otherwise a score of 99.
    for url in all_urls:
        url_validate_queue.put(
            (url, 1.0 if restaurant_data["initial_search"] else 0.99)
        )
