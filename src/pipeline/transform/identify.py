import spacy
import requests
from bs4 import BeautifulSoup
from collections import Counter

nlp = spacy.load("en_core_web_trf")

EXCLUDED_WORDS = {
    "TikTok",
    "OpenTable",
    "Google",
    "Michelin",
    "Mapped",
    "Facebook",
    "Instagram",
    "South Coast Plaza",
    "Pacific Coast Highway",
    "River Jetty Restaurant Group",
    "Anaheim Ducks",
    "Wineworks for Everyone",
    "Sunny Cal Farms",
    "Ramones",
    "Mapped",
    "OC Baking Company",
    "Strauss Creamery",
    "4th Street Market",
    "Diamond Jamboree",
    "Siete Family Foods",
    "Siete Juntos Fund",
    "Angel Stadium",
    "Ritz-Carlton Laguna Niguel",
}

RESTAURANT_KEYWORDS = {
    "restaurant",
    "kitchen",
    "pizzeria",
    "cafe",
    "deli",
    "bar",
    "bistro",
    "steakhouse",
    "grill",
    "tavern",
    "market",
    "bakery",
}


def clean_name(name):
    """Cleans restaurant names by removing extra words and formatting errors."""
    name = name.replace("’s", "").replace("‘s", "").replace("'", "").strip()
    name = " ".join(word for word in name.split() if word.lower() not in EXCLUDED_WORDS)
    return name


def extract_restaurants_from_soup(soup):
    """
    Extracts restaurant names from a BeautifulSoup object.
    Focuses on structured content where restaurant names are likely to appear.
    """
    title = soup.title.string if soup.title else ""
    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True) if h1 else ""
    headings = [h.get_text(strip=True) for h in soup.find_all(["h1", "h2"])]

    article_content = soup.find("article") or soup.find("main") or soup.body
    text_blocks = []
    if article_content:
        text_blocks.extend(article_content.find_all(["p", "li"]))
    else:
        text_blocks.extend(soup.find_all(["p", "li"]))

    text = "\n".join(block.get_text(strip=True) for block in text_blocks)

    doc = nlp(text)

    entity_counts = Counter()
    restaurant_names = set()

    for ent in doc.ents:
        if ent.label_ in {
            "ORG",
            "FAC",
            "PERSON",
        }:
            clean_restaurant = clean_name(ent.text)
            if clean_restaurant and clean_restaurant.lower() not in EXCLUDED_WORDS:
                entity_counts[clean_restaurant] += 1
                restaurant_names.add(clean_restaurant)

    for name in restaurant_names:
        if any(word.lower() in RESTAURANT_KEYWORDS for word in name.split()):
            entity_counts[name] += 2
        if name.lower() in title.lower():
            entity_counts[name] += 5
        if name.lower() in h1_text.lower():
            entity_counts[name] += 6
        if any(name.lower() in h.lower() for h in headings):
            entity_counts[name] += 3

    main_restaurants = entity_counts.most_common(5)

    return {
        "main_restaurant": [name for name, count in main_restaurants],
        "all_restaurants": sorted(list(restaurant_names)),
    }


if __name__ == "__main__":
    url = input("Enter the URL: ").strip()

    try:
        # Fetch the webpage content
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()

        # Parse the webpage content with BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract restaurant names from the parsed soup object
        locations = extract_restaurants_from_soup(soup)

        print("\nMain Restaurant(s) Mentioned:")
        for rest in locations["main_restaurant"]:
            print(f"- {rest}")

        print("\nAll Restaurants Mentioned:")
        for rest in locations["all_restaurants"]:
            print(f"- {rest}")

    except requests.RequestException as e:
        print(f"Error fetching the webpage: {e}")
