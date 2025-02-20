from database.db_operations import (
    insert_url,
    check_url_exists,
    update_last_crawled,
    insert_into_priority_queue,
    insert_domain,
    update_domain_visit_count,
    insert_source,
    check_domain_exists,
    check_source_exists,
)
from database.db_connector import get_db_connection
import re
from urllib.parse import urlparse


def normalize_url(url):
    """
    Normalize a URL by removing query parameters and fragments.
    """
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"


def calculate_url_score(url):
    """
    Calculate a URL score based on its domain and keywords.
    Higher scores indicate higher credibility for restaurant reviews.
    COULD USE SOME IMPROVEMENT
    """

    base_score = 0.5  # Default base score

    high_quality_keywords = ["news", "magazine", "guide", "review"]
    medium_quality_keywords = [
        "weekly",
        "reader",
        "journal",
        "digest",
        "critic",
        "insider",
    ]

    # Check domain extension
    if re.search(r"\.(org|gov|edu)(/|$)", url):
        base_score += 0.5

    # Check if the URL contains high-quality indicators
    if any(keyword in url.lower() for keyword in high_quality_keywords):
        base_score += 0.5
    elif any(keyword in url.lower() for keyword in medium_quality_keywords):
        base_score += 0.3

    # Ensure the score is between 0 and 1
    return max(0, min(base_score, 1))


def calculate_priority_score(url_score, relevance_score):
    """
    Calculate the priority score for a URL based on its relevance score.
    """
    return relevance_score * 0.65 + url_score * 0.35


def validate_url(url_pair):
    """
    Validate and insert a URL into the database following these steps:

    1. Normalize the URL.
    2. Extract the domain from the URL.
    3. Check if the domain exists:
       - If yes, update its visit count.
       - If no, insert it into the `domain` table.
    4. Insert the source if it doesn’t exist.
    5. Check if the URL exists:
       - If yes, update `last_crawled` and return.
       - If no, insert it into the `url` table, update `last_crawled` and `first_seen`.
    6. Calculate priority score and insert the URL into the `priority_queue`.

    Args:
        url (str): URL to validate.
        relevance_score (float): Relevance score of the URL.

    Returns:
        None
    """
    url, relevance_score = url_pair

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Normalize URL
        normalized_url = normalize_url(url)

        # Extract domain
        domain = normalized_url.split("//")[-1].split("/")[0]

        # Check if domain exists and update visit count
        domain_id = check_domain_exists(domain, cur)
        if domain_id:
            update_domain_visit_count(domain_id, cur)
        else:
            domain_id = insert_domain(domain, cur)

        # Insert source if it does not exist
        if not check_source_exists(domain_id, cur):
            source_id = insert_source(domain_id, relevance_score, cur)

        # Check if URL exists
        url_id = check_url_exists(normalized_url, cur)
        if url_id:
            update_last_crawled(url_id, cur)
            conn.commit()
            return
        url_id = insert_url(normalized_url, source_id, cur)

        # Calculate priority and insert into priority queue
        url_score = calculate_url_score(normalized_url)
        priority = calculate_priority_score(url_score, relevance_score)
        insert_into_priority_queue(url_id, priority, cur)

        conn.commit()

    except Exception as e:
        print(f"❌ Error validating URL: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
