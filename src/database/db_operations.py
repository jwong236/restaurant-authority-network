# Validate phase functions
def check_domain_exists(domain, cur):
    """
    Check if the domain exists in the database and return its ID.
    """
    cur.execute("SELECT id FROM domain WHERE domain_name = %s", (domain,))
    result = cur.fetchone()
    return result[0] if result else None


def insert_domain(domain, cur):
    """
    Insert a domain into the database and return its ID.
    """
    cur.execute(
        "INSERT INTO domain (domain_name, visit_count) VALUES (%s, 1) RETURNING id",
        (domain,),
    )
    return cur.fetchone()[0]


def update_domain_visit_count(domain_id, cur):
    """
    Increment the visit count for a given domain.
    """
    cur.execute(
        "UPDATE domain SET visit_count = visit_count + 1 WHERE id = %s", (domain_id,)
    )


def check_source_exists(domain_id, cur):
    """
    Check if a source exists for the given domain.
    """
    # This is not working as intended
    cur.execute("SELECT id FROM source WHERE domain_id = %s", (domain_id,))
    result = cur.fetchone()
    return result is not None


def insert_source(domain_id, credibility_score, cur):
    """
    Insert a source into the source table and return it's ID.
    """
    cur.execute(
        """
        INSERT INTO source (domain_id, source_type, credibility_score)
        VALUES (%s, 'webpage', %s)
        ON CONFLICT (domain_id, source_type) DO NOTHING
        RETURNING id
        """,
        (domain_id, credibility_score),
    )
    result = cur.fetchone()
    return result[0] if result else None


def check_url_exists(url, cur):
    """
    Check if a URL exists in the database and return its ID.
    """
    cur.execute("SELECT id FROM url WHERE full_url = %s", (url,))
    result = cur.fetchone()
    return result[0] if result else None


def update_last_crawled(url_id, cur):
    """
    Update the last_crawled timestamp for an existing URL.
    """
    cur.execute("UPDATE url SET last_crawled = NOW() WHERE id = %s", (url_id,))


def insert_url(url, source_id, cur):
    """
    Insert a URL into the url table and return its ID.
    """
    cur.execute(
        """
        INSERT INTO url (full_url, source_id, first_seen, last_crawled)
        VALUES (%s, %s, NOW(), NOW()) RETURNING id
        """,
        (url, source_id),
    )
    return cur.fetchone()[0]


def insert_into_priority_queue(url_id, priority, cur):
    """
    Insert a URL into the priority queue.
    """
    cur.execute(
        """
        INSERT INTO priority_queue (url_id, priority, queued_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (url_id) DO UPDATE SET priority = EXCLUDED.priority
        """,
        (url_id, priority),
    )


# Extract phase functions


def get_priority_queue_url(cur):
    """
    Get the highest priority URL from the priority queue.
    """
    cur.execute(
        """
        SELECT url.full_url, priority_queue.priority
        FROM priority_queue
        JOIN url ON url.id = priority_queue.url_id
        ORDER BY priority_queue.priority DESC
        LIMIT 1
        """
    )
    result = cur.fetchone()
    if result is None:
        print("No URLs in the priority queue.")
    return result


def update_priority_queue_url(url, priority, cur):
    """
    Update the priority of a URL in the priority queue.
    """
    cur.execute("SELECT id FROM url WHERE full_url = %s", (url,))
    url_id = cur.fetchone()

    if url_id:
        cur.execute(
            """
            UPDATE priority_queue
            SET priority = %s
            WHERE url_id = %s
            """,
            (priority, url_id[0]),
        )
    else:
        print(f"⚠️ URL not found in database: {url}")


def remove_priority_queue_url(url, cur):
    """
    Remove a URL from the priority queue.
    """
    cur.execute("SELECT id FROM url WHERE full_url = %s", (url,))
    url_id = cur.fetchone()

    if url_id:
        cur.execute("DELETE FROM priority_queue WHERE url_id = %s", (url_id[0],))
    else:
        print(f"URL not found in database: {url}")


# Transform phase functions


def check_restaurant_exists(name, cur):
    """
    Check if a restaurant exists in the database.
    """
    cur.execute("SELECT id FROM restaurant WHERE name = %s", (name,))
    result = cur.fetchone()
    return result[0] if result else None


def fuzzy_search_restaurant_name(search_term, cur):
    """
    Calls the PostgreSQL stored function fuzzy_search_restaurant_name to find the best matching restaurant.
    """
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


# Load phase functions


def insert_restaurant(name, address, cur):
    """
    Insert a restaurant into the database.
    """
    cur.execute(
        """
        INSERT INTO restaurant (name, address)
        VALUES (%s, %s)
        RETURNING id
        """,
        (name, address),
    )
    return cur.fetchone()[0]


def insert_reference(restaurant_id, url_id, cur):
    """
    Insert a reference between a restaurant and a URL.
    """
    cur.execute(
        """
        INSERT INTO reference (restaurant_id, url_id)
        VALUES (%s, %s)
        ON CONFLICT (restaurant_id, url_id) DO NOTHING
        """,
        (restaurant_id, url_id),
    )
