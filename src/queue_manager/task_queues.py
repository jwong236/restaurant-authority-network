import queue

MAX_QUEUE_SIZE = 10000

restaurant_search_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
url_validate_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
content_extraction_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
text_transformation_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
data_loading_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
