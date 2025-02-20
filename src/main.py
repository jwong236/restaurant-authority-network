# ./src/main.py
import queue
import signal
import threading
import logging
import time
from dotenv import load_dotenv
from queue_manager.worker_pool import start_workers, QueueLogger
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
from queue_manager.monitoring import monitor_queues


load_dotenv()

# Add names to queues for better logging
queues = {
    "restaurant_search_queue": restaurant_search_queue,
    "url_validate_queue": url_validate_queue,
    "content_extraction_queue": content_extraction_queue,
    "text_transformation_queue": text_transformation_queue,
    "data_loading_queue": data_loading_queue,
}
for name, q in queues.items():
    q._name = name  # Add queue name attribute for logging


def main():
    # Initialize with progress tracking
    restaurant_json_path = "michelin_restaurants.json"
    progress_tracker_path = "progress_tracker.json"

    # Start monitoring thread
    monitor_thread = threading.Thread(
        target=monitor_queues, args=(queues,), daemon=True
    )
    monitor_thread.start()
    logging.info("🔍 Started queue monitoring daemon")

    # ------------------------------------------------------------
    # Initialize Pipeline
    # ------------------------------------------------------------
    try:
        restaurant_list = get_restaurant_batch(
            restaurant_json_path, progress_tracker_path, 10
        )
        for idx, r in enumerate(restaurant_list):
            r["id"] = f"R{idx:04d}"  # Add unique identifier
            restaurant_search_queue.put(r)
            QueueLogger.log_task_progress(r, restaurant_search_queue, "ENQUEUED")

        logging.info(
            f"🍴 Initialized with {len(restaurant_list)} restaurants in search queue"
        )

        # ------------------------------------------------------------
        # Pipeline Stages
        # ------------------------------------------------------------
        pipeline_stages = [
            {
                "name": "Search",
                "input": restaurant_search_queue,
                "processor": search_engine_search,
                "output": url_validate_queue,
                "workers": 1,
                "search_worker": True,
            },
            {
                "name": "Validation",
                "input": url_validate_queue,
                "processor": validate_url,
                "output": content_extraction_queue,
                "workers": 2,
            },
            {
                "name": "Extraction",
                "input": content_extraction_queue,
                "processor": extract_content,
                "output": text_transformation_queue,
                "workers": 2,
            },
            {
                "name": "Transformation",
                "input": text_transformation_queue,
                "processor": transform_data,
                "output": data_loading_queue,
                "workers": 2,
            },
            {
                "name": "Loading",
                "input": data_loading_queue,
                "processor": load_data,
                "output": {
                    "restaurant_search_queue": restaurant_search_queue,
                    "url_validate_queue": url_validate_queue,
                },
                "workers": 2,
            },
        ]

        for stage in pipeline_stages:
            logging.info(f"🚀 Starting {stage['name']} stage workers")
            start_workers(
                task_queue=stage["input"],
                process_function=stage["processor"],
                output_queues=stage["output"],
                num_workers=stage["workers"],
                is_search_worker=stage.get("search_worker", False),
            )

        # Keep main thread alive
        while True:
            time.sleep(3600)  # Sleep for 1 hour

    except KeyboardInterrupt:
        logging.info("\n🛑 Received shutdown signal. Cleaning up...")
        for q in queues.values():
            while not q.empty():
                try:
                    q.get_nowait()
                    q.task_done()
                except queue.Empty:
                    continue
        logging.info("🧹 Queues cleared. Exiting safely.")


if __name__ == "__main__":
    main()
