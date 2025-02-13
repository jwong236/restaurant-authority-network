from collections import Counter


def classify_webpage(soup):
    """
    Classifies a webpage into one of the predefined categories:
    - "simple_review"
    - "aggregated_review_list"
    - "simple_review_list"
    - "hub_page"
    - "irrelevant"

    Parameters:
    - soup (BeautifulSoup): Parsed HTML content of the webpage.

    Returns:
    - (str, int): A tuple containing the webpage classification and a relevance score.
    """

    ### **Step 1: Extract Basic Page Information**
    title = soup.title.get_text(strip=True).lower() if soup.title else ""
    headings = [
        h.get_text(strip=True).lower() for h in soup.find_all(["h1", "h2", "h3"])
    ]
    list_items = [li.get_text(strip=True).lower() for li in soup.find_all("li")]
    num_links = len(soup.find_all("a", href=True))
    body_text = soup.get_text(separator=" ", strip=True)
    word_count = len(body_text.split())

    ### **Step 2: Check for Restaurant-Related Terms**
    restaurant_keywords = {
        "best restaurants",
        "where to eat",
        "top restaurants",
        "must-try",
    }
    review_keywords = {"review", "menu", "ratings", "food", "dining"}
    hub_keywords = {"food blogs", "resources", "explore", "directory"}

    title_contains_restaurant_terms = any(
        keyword in title for keyword in restaurant_keywords
    )
    title_contains_review_terms = any(keyword in title for keyword in review_keywords)

    ### **Step 3: Classification Rules**

    # 1️⃣ **Simple Review (Detailed Review for One Restaurant)**
    if title_contains_review_terms and len(headings) <= 2 and word_count > 500:
        return "simple_review", 90

    # 2️⃣ **Aggregated Review List (Multiple Reviews)**
    if any(keyword in title for keyword in restaurant_keywords) and len(headings) > 3:
        return "aggregated_review_list", 85

    # 3️⃣ **Simple Review List (Just a List of Restaurants)**
    if len(list_items) > 5 and not title_contains_review_terms:
        return "simple_review_list", 80

    # 4️⃣ **Hub Page (Many Outbound Links, No Clear Review Content)**
    if num_links > 30 and any(keyword in title for keyword in hub_keywords):
        return "hub_page", 40

    # 5️⃣ **Irrelevant (Not About Restaurants)**
    if num_links < 5 and len(headings) < 3 and not title_contains_restaurant_terms:
        return "irrelevant", 30

    # Default to Unknown
    return "unknown", 50
