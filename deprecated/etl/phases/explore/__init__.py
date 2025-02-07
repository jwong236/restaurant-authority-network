import time
import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")


def execute_phase(logger, config, shared_state, cur):
    """Main entry point for the exploration phase."""

    urls = fetch_queue_batch(logger, config, shared_state, cur)
    update_processing_status(logger, urls, cur)
    queries = generate_queries_from_urls(urls)
    search_results = run_queries(logger, queries)
    discovered_links = extract_new_links(search_results, cur)

    # Store discovered links in shared state
    if "discovered_links" not in shared_state:
        shared_state["discovered_links"] = []
    shared_state["discovered_links"].extend(discovered_links)

    logger.info(f"Added {len(discovered_links)} discovered links to shared state.")


def fetch_queue_batch(logger, config, shared_state, cur):
    """Atomic batch selection with locking"""
    batch_size = config.get("discovery_batch_size", 50)

    try:
        cur.execute("BEGIN;")
        cur.execute(
            """
            SELECT url FROM priority_queue
            WHERE status = 'pending'
            ORDER BY priority DESC, date_entered ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED;
            """,
            (batch_size,),
        )

        urls = [row[0] for row in cur.fetchall()]
        if urls:
            cur.execute(
                """
                UPDATE priority_queue
                SET status = 'processing'
                WHERE url = ANY(%s);
                """,
                (urls,),
            )

        cur.execute("COMMIT;")
        return urls

    except Exception as e:
        cur.execute("ROLLBACK;")
        logger.error(f"Batch fetch failed: {str(e)}")
        return []


def update_processing_status(logger, urls, cur):
    """Add retry tracking"""
    if not urls:
        return

    try:
        cur.execute(
            """
            UPDATE priority_queue
            SET 
                status = 'processing',
                attempts = attempts + 1,
                last_attempt = NOW()
            WHERE url = ANY(%s);
            """,
            (urls,),
        )
    except Exception as e:
        logger.error(f"Status update failed: {str(e)}")
        raise


# ✅ Fix: Query using actual restaurant data
def generate_queries_from_urls(cur, urls):
    cur.execute(
        """
        SELECT r.name, r.address 
        FROM restaurants r
        JOIN priority_queue pq ON r.source_url = pq.url
        WHERE pq.url = ANY(%s)
    """,
        (urls,),
    )
    return [f"{name} {address} review" for name, address in cur.fetchall()]


def run_queries(logger, queries):
    """Run search engine queries using Brave Search API."""
    results = []
    url = "https://api.search.brave.com/res/v1/web/search"
    headers = {"X-Subscription-Token": BRAVE_API_KEY}

    for query in queries:
        params = {"q": query, "count": 20}
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                results.extend(data.get("web", {}).get("results", []))
                time.sleep(0.5)
            else:
                logger.error(f"Brave API Error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")

    return results


def extract_new_links(search_results, cur):
    """Filter out URLs that already exist in the database."""
    new_links = []
    for result in search_results:
        url = result.get("url")
        if url and not is_url_in_database(cur, url):
            new_links.append(url)
    return new_links


def is_url_in_database(cur, url):
    """Check if a URL is already in the database."""
    cur.execute(
        """
        SELECT 1 FROM restaurant_references WHERE reference_url = %s
        UNION
        SELECT 1 FROM priority_queue WHERE url = %s;
        """,
        (url, url),
    )
    return cur.fetchone() is not None
