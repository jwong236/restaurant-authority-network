import os
import requests
from dotenv import load_dotenv

load_dotenv()
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"


def search_engine_search(restaurant, result_size=30):
    """Queries Brave Web Search API and returns a combined list of search results from multiple queries."""

    if not BRAVE_API_KEY:
        raise ValueError("BRAVE_API_KEY is missing from .env")

    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "X-Subscription-Token": BRAVE_API_KEY,
    }

    queries = [
        f"{restaurant} review",
        f"{restaurant} restaurant review",
    ]
    all_urls = set()

    for query in queries:
        params = {"q": query, "count": result_size}

        try:
            response = requests.get(BRAVE_SEARCH_URL, headers=headers, params=params)
            response.raise_for_status()
            search_results = response.json()

            urls = [
                result["url"]
                for result in search_results.get("web", {}).get("results", [])
            ]

            all_urls.update(urls)

        except requests.exceptions.RequestException as e:
            print(f"API request error for query '{query}': {e}")
            raise

    return list(all_urls)
