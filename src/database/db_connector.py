import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}


def get_db_connection():
    """Establishes and returns a new database connection."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        raise e
