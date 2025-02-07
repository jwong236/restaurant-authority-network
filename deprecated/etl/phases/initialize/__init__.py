import json
import os
import subprocess
from urllib.parse import urlparse

PRIORITY_SCORE = 100


def execute_phase(logger, config, shared_state, cur):
    """Main entry point for the initialization phase"""

    if not shared_state.get("initialized", False):
        logger.info("Running Scrapy to seed Michelin restaurants")
        start_crawl(logger, config)
        shared_state["initialized"] = True

    if not shared_state.get("michelin_loaded", False):
        logger.info("Reading Michelin restaurant data from JSON")
        restaurants = read_michelin_data(logger, config)

        if restaurants:
            logger.info("Loading Michelin restaurant data into database")
            load_michelin_data(logger, restaurants, cur)
            shared_state["michelin_loaded"] = True


def start_crawl(logger, config):
    """Use Scrapy to crawl Michelin Guide and seed initial targets"""
    output_file = config.get("output_file", "michelin_restaurants.json")

    if not os.path.exists(output_file):
        logger.info("🕷️ Starting Scrapy crawl...")
        subprocess.run(
            ["scrapy", "crawl", "seed_sites", "-o", output_file, "-t", "json"],
            check=True,
        )
        logger.info("✅ Scrapy completed, Michelin data collected.")
    else:
        logger.info(
            f"📂 Michelin data already exists in {output_file}. Skipping crawl."
        )


def read_michelin_data(logger, config):
    """Read Michelin restaurant data from JSON file"""
    output_file = config.get("output_file", "michelin_restaurants.json")

    if not os.path.exists(output_file):
        logger.error("⚠️ Michelin data file not found! Cannot load restaurants.")
        return []

    with open(output_file, "r", encoding="utf-8") as file:
        restaurants = json.load(file)

    logger.info(f"📂 Loaded {len(restaurants)} restaurants from {output_file}")
    return restaurants


def load_michelin_data(logger, restaurants, cur):
    """Load Michelin restaurant data into the database and priority queue"""
    try:
        logger.info("Populating source types...")
        populate_source_types(cur)

        for entry in restaurants:
            name = entry["name"]
            address = entry["location"]
            source_url = entry["source_url"]
            domain = urlparse(source_url).netloc

            restaurant_id = get_or_create_restaurant(cur, name, address)
            source_id = get_or_create_source(cur, domain)
            insert_reference(cur, restaurant_id, source_id, source_url)
            insert_into_priority_queue(cur, source_url, PRIORITY_SCORE)

        logger.info("✅ Michelin restaurants successfully loaded into the database.")

    except Exception as e:
        logger.error(f"Error loading Michelin data: {e}")
        raise


def populate_source_types(cur):
    """Ensure source_types table has required entries"""
    cur.execute(
        """
        INSERT INTO source_types (type_name)
        VALUES ('blog'), ('video'), ('social')
        ON CONFLICT (type_name) DO NOTHING;
        """
    )


def get_or_create_restaurant(cur, name, address):
    """Insert restaurant if not exists or update its backlink_count and is_michelin status"""
    cur.execute(
        """
        INSERT INTO restaurants (name, address, backlink_count, is_michelin)
        VALUES (%s, %s, 1, TRUE)
        ON CONFLICT (name, address) DO UPDATE
        SET is_michelin = TRUE,
            backlink_count = restaurants.backlink_count + 1
        RETURNING restaurant_id;
        """,
        (name, address),
    )
    return cur.fetchone()[0]


def get_or_create_source(cur, domain):
    """Insert source if not exists and return source_id"""
    cur.execute(
        """
        INSERT INTO sources (source_type, domain)
        VALUES ('blog', %s)
        ON CONFLICT (source_type, domain) DO NOTHING
        RETURNING source_id;
        """,
        (domain,),
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            SELECT source_id FROM sources
            WHERE source_type = 'blog' AND domain = %s;
            """,
            (domain,),
        )
    return cur.fetchone()[0]


def insert_reference(cur, restaurant_id, source_id, reference_url):
    """Insert into restaurant_references if not exists"""
    cur.execute(
        """
        INSERT INTO restaurant_references 
            (restaurant_id, source_id, reference_url, sentiment_score)
        VALUES (%s, %s, %s, 1)
        ON CONFLICT (restaurant_id, source_id, reference_url) 
        DO NOTHING;
        """,
        (restaurant_id, source_id, reference_url),
    )


def insert_into_priority_queue(cur, url, priority):
    """Insert Michelin restaurant URLs into the priority queue with high priority"""
    cur.execute(
        """
        INSERT INTO priority_queue (url, priority, status, source_type)
        VALUES (%s, %s, 'pending', 'michelin')
        ON CONFLICT (url) DO NOTHING;
        """,
        (url, priority),
    )
