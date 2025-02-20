import queue
import logging
import time
from dotenv import load_dotenv
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

logging.basicConfig(level=logging.INFO, format="%(message)s")

queues = {
    "restaurant_search_queue": restaurant_search_queue,
    "url_validate_queue": url_validate_queue,
    "content_extraction_queue": content_extraction_queue,
    "text_transformation_queue": text_transformation_queue,
    "data_loading_queue": data_loading_queue,
}


def print_queue_contents():
    """Displays the contents of each queue."""
    for name, q in queues.items():
        logging.info(f"📥 {name}: {list(q.queue)}")


def process_queue(stage_name, task_queue, processor):
    """
    Processes the tasks in the given queue using the provided processor function.
    Each phase is responsible for enqueuing its own results into the next queue.
    """
    input(f"🔹 Press Enter to start the '{stage_name}' phase...")

    if task_queue.empty():
        logging.info(f"⚠️ No tasks in {stage_name} queue. Skipping...")
        return

    logging.info(f"🚀 Processing {stage_name} phase...")

    while not task_queue.empty():
        try:
            task = task_queue.get_nowait()
            processor(task)
            task_queue.task_done()

        except queue.Empty:
            break

    logging.info(f"✅ Finished {stage_name} phase.")


def initialize(
    restaurant_json_path="michelin_restaurants.json",
    progress_tracker_path="progress_tracker.json",
):

    restaurant_list = get_restaurant_batch(
        restaurant_json_path, progress_tracker_path, 1
    )

    for r in restaurant_list:
        r["initial_search"] = True
        restaurant_search_queue.put(r)
        logging.info(f"🍽️ Added to search queue: {r}")

    logging.info(f"✅ Initialized with {len(restaurant_list)} restaurants.")


def main():
    restaurant_json_path = "michelin_restaurants.json"
    progress_tracker_path = "progress_tracker.json"

    logging.info("🟢 Starting test mode with 1 worker per phase.")

    print_queue_contents()

    input("🔹 Press Enter to start the pipeline...")

    # 🔹 Initialization Phase
    initialize(restaurant_json_path, progress_tracker_path)
    print_queue_contents()
    input("🔹 Press Enter to proceed to the next phase...")

    # 🔹 Search Phase - Functional
    process_queue("Search", restaurant_search_queue, search_engine_search)
    print_queue_contents()
    input("🔹 Press Enter to proceed to the next phase...")

    # 🔹 Validation Phase -
    process_queue("Validation", url_validate_queue, validate_url)
    print_queue_contents()
    input("🔹 Press Enter to proceed to the next phase...")

    # 🔹 Extraction Phase
    process_queue("Extraction", content_extraction_queue, extract_content)
    print_queue_contents()
    input("🔹 Press Enter to proceed to the next phase...")

    # 🔹 Transformation Phase
    process_queue("Transformation", text_transformation_queue, transform_data)
    print_queue_contents()
    input("🔹 Press Enter to proceed to the next phase...")

    # 🔹 Loading Phase
    process_queue("Loading", data_loading_queue, load_data)
    print_queue_contents()

    logging.info("🏁 Test completed successfully!")


if __name__ == "__main__":
    main()
