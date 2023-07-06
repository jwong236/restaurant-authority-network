from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker

class Crawler(object):
    def __init__(self, config, resume, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, resume)
        self.workers = list()
        self.worker_factory = worker_factory

    def start_async(self):
        self.logger.info("Starting asynchronous crawling...")
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.THREAD_COUNT)]
        for worker in self.workers:
            worker.start()
        self.logger.info(f"Started {self.config.THREAD_COUNT} workers.")

    def start(self):
        try:
            self.start_async()
            self.join()
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt. Stopping workers...")
            for worker in self.workers:
                worker.stop()
            self.logger.info("Workers have been stopped.")
        finally:
            self.join()

    def join(self):
        self.logger.info("Joining worker threads...")
        for worker in self.workers:
            worker.join()
        self.logger.info("Worker threads have finished their jobs.")