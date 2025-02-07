import json
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

load_dotenv()

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

with open("seed_restaurants_michelin.json", "r", encoding="utf-8") as file:
    data = json.load(file)

conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()


def populate_source_types():
    """Ensure source_types table has required entries"""
    cur.execute(
        """
        INSERT INTO source_types (type_name)
        VALUES ('blog'), ('video'), ('social')
        ON CONFLICT (type_name) DO NOTHING;
        """
    )


def get_or_create_restaurant(name, address):
    """Insert restaurant if not exists or update its backlink_count and is_michelin status"""
    cur.execute(
        """
        INSERT INTO restaurants (name, address, backlink_count, is_michelin)
        VALUES (%s, %s, 1, TRUE)
        ON CONFLICT (name, address) DO UPDATE
        SET is_michelin = EXCLUDED.is_michelin,
            backlink_count = restaurants.backlink_count + 1
        RETURNING restaurant_id;
        """,
        (name, address),
    )
    return cur.fetchone()[0]


def get_or_create_source(domain):
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


def insert_reference(restaurant_id, source_id, reference_url):
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


populate_source_types()

for entry in data:
    name = entry["name"]
    address = entry["location"]
    source_url = entry["source_url"]

    domain = urlparse(source_url).netloc

    restaurant_id = get_or_create_restaurant(name, address)
    source_id = get_or_create_source(domain)
    insert_reference(restaurant_id, source_id, source_url)

conn.commit()
cur.close()
conn.close()

print("Data inserted successfully!")
