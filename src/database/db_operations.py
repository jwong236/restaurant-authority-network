import logging

# Setup logging
logging.basicConfig(level=logging.ERROR, filename="db_errors.log")


# ---------------- DOMAIN TABLE ----------------
def check_domain_exists(domain_name, conn):
    """Check if the domain exists in the database and return its ID."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM domain WHERE domain_name = %s", (domain_name,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logging.error(f"Error checking domain existence: {e}")
        return None


def insert_domain(domain_name, quality_score, conn):
    """Insert a domain into the database and return its ID."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO domain (domain_name, visit_count, quality_score) VALUES (%s, 0, %s) RETURNING id",
                (domain_name, quality_score),
            )
            domain_id = cur.fetchone()[0]
            conn.commit()
            return domain_id
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting domain: {e}")
        return None


def get_domain_quality_score(domain_id, conn):
    """Get the quality score of a domain."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT quality_score FROM domain WHERE id = %s", (domain_id,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logging.error(f"Error getting domain quality score: {e}")
        return None


def update_domain_visit_count(domain_id, conn):
    """Update the visit count of a domain."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE domain SET visit_count = visit_count + 1 WHERE id = %s",
                (domain_id,),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating domain visit count: {e}")


def update_domain_quality_score(domain_id, quality_score, conn):
    """Update the quality score of a domain."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE domain SET quality_score = %s WHERE id = %s",
                (quality_score, domain_id),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating domain quality score: {e}")


# ---------------- SOURCE TABLE ----------------
def insert_source(domain_id, source_type, conn):
    """Insert a source into the source table and return its ID."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO source (domain_id, source_type) VALUES (%s, %s) RETURNING id",
                (domain_id, source_type),
            )
            source_id = cur.fetchone()[0]
            conn.commit()
            return source_id
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting source: {e}")
        return None


def check_source_exists(domain_id, conn):
    """
    Check if a source exists for the given domain_id.
    Returns the source ID if it exists, else None.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM source WHERE domain_id = %s", (domain_id,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logging.error(f"Error checking source existence: {e}")
        return None


# ---------------- URL TABLE ----------------
def insert_url(url, source_id, conn):
    """Insert a URL into the url table and return its ID."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO url (full_url, source_id, first_seen, last_crawled) "
                "VALUES (%s, %s, NOW(), NOW()) RETURNING id",
                (url, source_id),
            )
            url_id = cur.fetchone()[0]
            conn.commit()
            return url_id
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting URL: {e}")
        return None


def check_url_exists(url, conn):
    """
    Check if a URL exists in the database by full_url.
    Return its ID if found, else None.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM url WHERE full_url = %s", (url,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logging.error(f"Error checking URL existence: {e}")
        return None


def update_last_crawled(url_id, conn):
    """
    Update the last_crawled timestamp of a URL to NOW().
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE url SET last_crawled = NOW() WHERE id = %s",
                (url_id,),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating last_crawled: {e}")


# ---------------- RESTAURANT TABLE ----------------
def insert_restaurant(name, address, conn):
    """Insert a restaurant into the database and return its ID."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO restaurant (name, address) VALUES (%s, %s) RETURNING id",
                (name, address),
            )
            restaurant_id = cur.fetchone()[0]
            conn.commit()
            return restaurant_id
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting restaurant: {e}")
        return None


def check_restaurant_exists(name, conn):
    """Check if a restaurant exists in the database by name."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM restaurant WHERE name = %s", (name,))
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logging.error(f"Error checking restaurant existence: {e}")
        return None


# ---------------- REFERENCE TABLE ----------------
def insert_reference(restaurant_id, url_id, relevance_score, conn):
    """Insert a reference with a relevance score."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO reference (restaurant_id, url_id, relevance_score, discovered_at) "
                "VALUES (%s, %s, %s, NOW()) RETURNING id",
                (restaurant_id, url_id, relevance_score),
            )
            reference_id = cur.fetchone()[0]
            conn.commit()
            return reference_id
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting reference: {e}")
        return None


# ---------------- PRIORITY QUEUES ----------------
def get_url_priority_queue_length(conn):
    """Get the number of URLs in the priority queue."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM url_priority_queue")
            result = cur.fetchone()
            return result[0] if result else 0
    except Exception as e:
        logging.error(f"Error getting URL priority queue length: {e}")
        return 0


def insert_into_url_priority_queue(url_id, priority, conn):
    """Insert a URL into the priority queue or update its priority."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO url_priority_queue (url_id, priority) "
                "VALUES (%s, %s) ON CONFLICT (url_id) DO UPDATE SET priority = EXCLUDED.priority",
                (url_id, priority),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting into URL priority queue: {e}")


def insert_into_restaurant_priority_queue(name, priority, conn):
    """Insert a restaurant into the priority queue or update its priority."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO restaurant_priority_queue (name, priority) "
                "VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET priority = EXCLUDED.priority",
                (name, priority),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting into restaurant priority queue: {e}")


def get_priority_queue_url(conn):
    """Get the URL with the highest priority along with its full URL from the url table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT url.id, url.full_url, url_priority_queue.priority
                FROM url_priority_queue
                JOIN url ON url.id = url_priority_queue.url_id
                ORDER BY url_priority_queue.priority DESC
                LIMIT 1
                FOR UPDATE
                """
            )
            result = cur.fetchone()
            return result if result else None
    except Exception as e:
        logging.error(f"Error getting URL from priority queue: {e}")
        return None


def get_priority_queue_restaurant(conn):
    """Get the restaurant with the highest priority from the priority queue."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT name, priority FROM restaurant_priority_queue ORDER BY priority DESC LIMIT 1 FOR UPDATE"
            )
            result = cur.fetchone()
            return result if result else None
    except Exception as e:
        logging.error(f"Error getting restaurant from priority queue: {e}")
        return None


def update_priority_queue_url(conn, url, new_priority):
    """Update the priority of a URL in the priority queue."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE url_priority_queue SET priority = %s WHERE url_id = %s",
                (new_priority, url),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating URL priority: {e}")


def update_priority_queue_restaurant(conn, name, new_priority):
    """Update the priority of a restaurant in the priority queue."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE restaurant_priority_queue SET priority = %s WHERE name = %s",
                (new_priority, name),
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error updating restaurant priority: {e}")


def remove_from_url_priority_queue(url_id, conn):
    """Remove a URL from the priority queue."""
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM url_priority_queue WHERE url_id = %s", (url_id,))
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error removing from URL priority queue: {e}")


def remove_from_restaurant_priority_queue(name, conn):
    """Remove a restaurant from the priority queue."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM restaurant_priority_queue WHERE name = %s", (name,)
            )
            conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Error removing from restaurant priority queue: {e}")


# ---------------- CUSTOM FUNCTIONS ----------------
def fuzzy_search_restaurant_name(search_term, conn):
    """Calls the PostgreSQL stored function fuzzy_search_restaurant_name to find the best matching restaurant."""
    try:
        with conn.cursor() as cur:
            query = "SELECT id, name, address, confidence FROM fuzzy_search_restaurant_name(%s)"
            cur.execute(query, (search_term,))
            result = cur.fetchone()

            if result:
                return {
                    "id": result[0],
                    "name": result[1],
                    "address": result[2],
                    "confidence": result[3],
                }
        return None
    except Exception as e:
        logging.error(f"Error in fuzzy search: {e}")
        return None
