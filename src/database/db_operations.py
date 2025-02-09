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
        VALUES (%s, 'news', %s)
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
