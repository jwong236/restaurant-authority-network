# ./src/queue_manager/worker_pool.py
from concurrent.futures import ThreadPoolExecutor
import queue
import threading

# ✅ Define shutdown_flag globally in worker_pool.py
shutdown_flag = threading.Event()


def worker(task_queue, process_function, output_queues=None, is_search_worker=False):
    """Worker function that processes tasks from the task_queue.

    - Output queues can either be a **single queue** or a **dictionary of multiple possible output queues**.
    - Search workers exit when the queue is empty.
    - Other workers keep running unless `shutdown_flag` is set.
    """
    while not shutdown_flag.is_set():
        try:
            task = task_queue.get(timeout=1 if is_search_worker else None)
            result = process_function(task)

            if output_queues:
                if isinstance(output_queues, dict):
                    if isinstance(result, dict):
                        for queue_name, data in result.items():
                            if queue_name in output_queues and data:
                                output_queues[queue_name].put(data)
                    else:
                        list(output_queues.values())[0].put(result)
                else:
                    output_queues.put(result)

            task_queue.task_done()

        except queue.Empty:
            if is_search_worker or shutdown_flag.is_set():
                break
            continue

        except KeyboardInterrupt:
            print("\n🛑 Worker received shutdown signal. Exiting...")
            shutdown_flag.set()
            break

        except Exception as e:
            print(f"❌ Error processing task: {e}")
            task_queue.task_done()
            break


def start_workers(
    task_queue,
    process_function,
    output_queues=None,
    num_workers=5,
    is_search_worker=False,
):
    """Starts a pool of worker threads to process tasks from the task_queue."""
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        for _ in range(num_workers):
            executor.submit(
                worker, task_queue, process_function, output_queues, is_search_worker
            )


def stop_workers():
    """Sets the shutdown flag to stop all workers."""
    shutdown_flag.set()
