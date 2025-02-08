# ./src/main.py
import signal
import threading
from dotenv import load_dotenv

from queue_manager.worker_pool import start_workers
from queue_manager.monitoring import monitor_queues
from queue_manager.task_queues import (
    restaurant_search_queue,
    url_validate_queue,
    content_extraction_queue,
    text_transformation_queue,
    data_loading_queue,
)

from pipeline.initialize import get_restaurant_batch
from pipeline.search import search_engine_search
from pipeline.validate import validate_url
from pipeline.extract import extract_content
from pipeline.transform import transform_data
from pipeline.load import load_data

load_dotenv()


def main():
    # Define paths for restaurant JSON file and progress tracker
    restaurant_json_path = "michelin_restaurants.json"
    progress_tracker_path = "progress_tracker.json"

    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT)

    # Initialize pipeline queues
    queues = {
        "restaurant_search_queue": restaurant_search_queue,
        "url_validate_queue": url_validate_queue,
        "content_extraction_queue": content_extraction_queue,
        "text_transformation_queue": text_transformation_queue,
        "data_loading_queue": data_loading_queue,
    }

    # Start monitoring thread (prints queue sizes periodically)
    monitor_thread = threading.Thread(
        target=monitor_queues, args=(queues,), daemon=True
    )
    monitor_thread.start()

    # ------------------------------------------------------------
    # ✅ STEP 1: Load restaurants from JSON file into `restaurant_search_queue`
    # ------------------------------------------------------------
    restaurant_list = get_restaurant_batch(
        restaurant_json_path, progress_tracker_path, 10
    )
    for r in restaurant_list:
        restaurant_search_queue.put(r)

    print("restaurant_search_queue size:", restaurant_search_queue.qsize())

    # ------------------------------------------------------------
    # ✅ STEP 2: Search for each restaurant → Output URLs to `url_validate_queue`
    # ------------------------------------------------------------
    start_workers(
        restaurant_search_queue,
        search_engine_search,  # Searches for restaurants
        url_validate_queue,  # Sends found URLs to `url_validate_queue`
        num_workers=3,
        is_search_worker=True,  # Terminates workers if queue is empty
    )

    # ------------------------------------------------------------
    # ✅ STEP 3: Validate URLs → Send valid ones to `content_extraction_queue`
    # ------------------------------------------------------------
    start_workers(
        url_validate_queue,
        validate_url,  # Extracts content from URLs
        content_extraction_queue,  # Sends extracted content for processing
        num_workers=5,
    )

    # ------------------------------------------------------------
    # ✅ STEP 4: Extract content → Send extracted data to `text_transformation_queue`
    # ------------------------------------------------------------
    start_workers(
        content_extraction_queue,
        extract_content,  # Transforms extracted content
        text_transformation_queue,  # Sends transformed data for loading
        num_workers=5,
    )

    # ------------------------------------------------------------
    # ✅ STEP 5: Transform data → Send processed content to `data_loading_queue`
    # ------------------------------------------------------------
    start_workers(
        text_transformation_queue,
        transform_data,  # Loads data into database
        data_loading_queue,  # Sends results to `data_loading_queue`
        num_workers=3,
    )

    # ------------------------------------------------------------
    # ✅ STEP 6: Load data → Decide whether to send to `restaurant_search_queue` or `url_validate_queue`
    # ------------------------------------------------------------
    start_workers(
        data_loading_queue,
        load_data,
        {
            "restaurant_search_queue": restaurant_search_queue,
            "url_validate_queue": url_validate_queue,
        },
        num_workers=3,
    )

    print("🚀 ETL pipeline started! Press Ctrl+C to stop.")


if __name__ == "__main__":
    main()
