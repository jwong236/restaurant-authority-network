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
from pipeline.search import search_engine_process
from pipeline.extract import extract_content
from pipeline.transform import transform_data
from pipeline.load import load_data


load_dotenv()

# Initialize pipeline queues
queues = {
    "restaurant_search_queue": restaurant_search_queue,
    "url_validate_queue": url_validate_queue,
    "content_extraction_queue": content_extraction_queue,
    "text_transformation_queue": text_transformation_queue,
    "data_loading_queue": data_loading_queue,
}

# Initialize monitoring thread
monitor_thread = threading.Thread(target=monitor_queues, args=(queues,), daemon=True)
monitor_thread.start()

# Begin passes restaurants to the search engine
restaurant_list = []
restaurant_list.extend(get_restaurant_batch())
for r in restaurant_list:
    restaurant_search_queue.put(r)

# Start ETL Pipeline Workers
start_workers(
    restaurant_search_queue,
    search_engine_process,
    url_validate_queue,
    num_workers=3,
    is_search_worker=True,
)
start_workers(
    url_validate_queue, extract_content, content_extraction_queue, num_workers=5
)
start_workers(
    content_extraction_queue, transform_data, text_transformation_queue, num_workers=5
)
start_workers(text_transformation_queue, load_data, data_loading_queue, num_workers=3)
start_workers(
    data_loading_queue,
    load_data,
    {
        "restaurant_search_queue": restaurant_search_queue,
        "url_validate_queue": url_validate_queue,
    },
    num_workers=3,
)

print("✅ ETL pipeline started!")
