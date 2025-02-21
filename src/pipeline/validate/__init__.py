import logging
import re
from urllib.parse import urlparse
from database.db_connector import get_db_connection
from database.db_operations import (
    check_domain_exists,
    insert_domain,
    update_domain_visit_count,
    update_domain_quality_score,
    check_source_exists,
    insert_source,
    check_url_exists,
    insert_url,
    update_last_crawled,
    insert_into_url_priority_queue,
    get_domain_quality_score,
)

# Phase name for logging consistency
PHASE = "VALIDATE"


def normalize_url(url):
    """Removes query parameters/fragments and standardizes the URL."""
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}{p.path}"


def calculate_url_score(url):
    """Assigns a score to the URL based on its domain and keywords."""
    base = 0.5
    high_kw = ["news", "magazine", "guide", "review"]
    mid_kw = ["weekly", "reader", "journal", "digest", "critic", "insider"]

    if re.search(r"\.(org|gov|edu)(/|$)", url):
        base += 0.5

    lower = url.lower()
    if any(k in lower for k in high_kw):
        base += 0.5
    elif any(k in lower for k in mid_kw):
        base += 0.3

    return max(0, min(base, 1))


def calculate_priority_score(relevance, url_score):
    """Computes final priority score based on relevance and URL trustworthiness."""
    return relevance * 0.65 + url_score * 0.35


def validate_url(url_pair):
    """
    Validates and processes a (url, relevance_score) tuple:
      1. Normalizes the URL.
      2. Checks/updates the domain (or inserts if new).
      3. Checks/updates the source.
      4. Checks if the URL exists:
         - If yes, updates last_crawled.
         - If no, inserts and assigns priority.
      5. Logs and commits changes.
    """
    url, relevance = url_pair
    conn = get_db_connection()

    try:
        norm_url = normalize_url(url)
        domain_str = norm_url.split("//", 1)[-1].split("/", 1)[0]

        logging.info(f"[{PHASE}]: Processing URL: {norm_url}")

        # Step 1: Handle domain
        dom_id = check_domain_exists(domain_str, conn)
        if dom_id:
            update_domain_visit_count(dom_id, conn)
            old_score = get_domain_quality_score(dom_id, conn)
            new_score = (
                max(-1, min(old_score + 0.1 * (relevance - 0.5), 1))
                if old_score is not None
                else 0.0
            )
            update_domain_quality_score(dom_id, new_score, conn)
            logging.info(
                f"[{PHASE}]: Domain '{domain_str}' exists. Updated visit count and quality score ({new_score})."
            )
        else:
            dom_id = insert_domain(domain_str, 0.0, conn)
            update_domain_visit_count(dom_id, conn)
            logging.info(f"[{PHASE}]: Inserted new domain '{domain_str}'.")

        # Step 2: Handle source
        src_id = check_source_exists(dom_id, conn)
        if not src_id:
            src_id = insert_source(dom_id, "webpage", conn)
            logging.info(f"[{PHASE}]: Inserted new source for domain '{domain_str}'.")

        # Step 3: Handle URL
        found_url_id = check_url_exists(norm_url, conn)
        if found_url_id:
            update_last_crawled(found_url_id, conn)
            conn.commit()
            logging.info(
                f"[{PHASE}]: URL '{norm_url}' exists. Updated last_crawled timestamp."
            )
            return

        new_url_id = insert_url(norm_url, src_id, conn)
        url_score = calculate_url_score(norm_url)
        priority = calculate_priority_score(relevance, url_score)
        insert_into_url_priority_queue(new_url_id, priority, conn)

        conn.commit()
        logging.info(
            f"[{PHASE}]: Inserted URL '{norm_url}' with priority {priority:.2f}."
        )

        # High-level summary for console
        print(
            f"[{PHASE}]: Processed {norm_url} (Inserted with priority {priority:.2f})."
        )

    except Exception as e:
        logging.error(f"[{PHASE}]: Error in validate_url: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
