"""
Plan: Use a Named Entity Model like SpaCy to extract restaurant names and address from the soup, as well as a classiication model to determine if the soup is related to food and restaurants and is a review with sentiment. 
"""

from transform.classify import is_restaurant_review
from transform.identify import identify


def transform_data(content_tuple):
    """
    Main function for transforming extracted data.
    content_tuple = (url, priority, soup)
    """

    url, priority, soup = content_tuple

    # Get plain text from soup
    text = soup.get_text(separator=" ", strip=True)

    # Step 1: Classify the text
    if not is_restaurant_review(text):
        print(f"Skipping {url}: Not a restaurant review.")
        return None

    # Step 2: Extract structured data
    structured_data = identify(text)
    structured_data["url"] = url
    structured_data["priority"] = priority

    return structured_data
