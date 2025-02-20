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
import re
from urllib.parse import urlparse


def normalize_url(url):
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}{p.path}"


def calculate_url_score(url):
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
    return relevance * 0.65 + url_score * 0.35


def validate_url(url_pair):
    """
    Processes a (url, relevance_score) tuple to insert or update records in the database.

    Steps:
      1. Normalize the URL (remove query/fragment).
      2. Extract its domain and check if it exists:
         - If no, create a new domain (default quality_score=0.0).
         - If yes, increment visit_count and adjust quality_score.
      3. Check if source exists for the domain:
         - Insert one with source_type="webpage" if missing.
      4. Check if the URL is already in the DB:
         - If found, update last_crawled and stop.
         - Otherwise, insert a new URL record.
      5. Calculate a combined priority (using relevance_score and url_score) and insert into the URL priority queue.
      6. Commit or rollback on error.
    """

    url, relevance = url_pair
    conn = get_db_connection()

    try:
        norm_url = normalize_url(url)
        domain_str = norm_url.split("//", 1)[-1].split("/", 1)[0]

        dom_id = check_domain_exists(domain_str, conn)
        if dom_id:
            update_domain_visit_count(dom_id, conn)
            old_score = get_domain_quality_score(dom_id, conn)
            if old_score is not None:
                new_score = max(-1, min(old_score + 0.1 * (relevance - 0.5), 1))
                update_domain_quality_score(dom_id, new_score, conn)
        else:
            dom_id = insert_domain(domain_str, 0.0, conn)
            update_domain_visit_count(dom_id, conn)

        src_id = check_source_exists(dom_id, conn)
        if not src_id:
            src_id = insert_source(dom_id, "webpage", conn)

        found_url_id = check_url_exists(norm_url, conn)
        if found_url_id:
            update_last_crawled(found_url_id, conn)
            conn.commit()
            return

        new_url_id = insert_url(norm_url, src_id, conn)
        url_score = calculate_url_score(norm_url)
        priority = calculate_priority_score(relevance, url_score)
        insert_into_url_priority_queue(new_url_id, priority, conn)
        conn.commit()

    except Exception as e:
        print(f"❌ Error in validate_url: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
