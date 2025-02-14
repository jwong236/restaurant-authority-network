import requests
import json
import os
from bs4 import BeautifulSoup

TEST_PAGES = {
    "irrelevant": "https://en.wikipedia.org/wiki/Organ_(biology)",
    "simple_review": "https://www.ocregister.com/2021/10/14/review-hanuman-takes-thai-dining-upscale-in-costa-mesa/",
    "aggregated_review_list": "https://la.eater.com/maps/best-essential-restaurants-orange-county-california",
    "simple_review_list": "https://www.quora.com/What-are-the-best-restaurants-in-Orange-County",
    "hub_page": "http://www.consumingla.com/best-of-la/",
}

HEADERS = {
    "User-Agent": "Jacob's Crawler",
}

DATA_DIR = "src/tests/test_data"
os.makedirs(DATA_DIR, exist_ok=True)


def save_soup_as_json(url, filename):
    """Fetches the URL, extracts HTML, and saves it as JSON."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        data = {"url": url, "html": soup.prettify()}

        file_path = os.path.join(DATA_DIR, f"{filename}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print(f"✅ Saved {filename}.json")
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch {url}: {e}")


for category, url in TEST_PAGES.items():
    save_soup_as_json(url, category)

print("\nAll test data has been saved successfully")
