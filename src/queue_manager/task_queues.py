import queue

MAX_QUEUE_SIZE = 10000

search_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
validate_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
transform_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
load_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
