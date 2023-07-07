from threading import Thread
from crawler.utils.download import download
from crawler.utils import is_valid, get_urlhash
from utils import get_logger
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"WORKER-{worker_id}", "WORKER")
        self.config = config
        self.frontier = frontier
        self.stop_worker = False
        super().__init__()
        
    def run(self):
        while not self.stop_worker:
            try:
                tbd_url = self.frontier.get_tbd_url()
                if not tbd_url:
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
                
                self.logger.info(f"Attempting to download {tbd_url}")
                resp = download(tbd_url, self.config, self.logger)
                self.logger.info(f"Marking url as completed: {tbd_url}")
                time.sleep(self.config.POLITENESS)
                if self.stop_worker:
                    break
                if resp.error is not None:
                    self.logger.error(f"Download failed {tbd_url}, error <{resp.error}>, path {resp.filename}")
                    continue

                self.logger.info(f"Attempting to write JSON for {tbd_url}")
                resp.to_json(self.config.CORPUS_DIRECTORY)
                self.logger.info(f"Downloaded {tbd_url}, status <{resp.status_code}>, path {resp.filename}")
                self.logger.info(f"Scraping links from {tbd_url}")
                scraped_urls =  self.scrape(resp)
                self.logger.info(f"Found {len(scraped_urls)} links from {tbd_url}")
                for scraped_url in scraped_urls:
                    if is_valid(scraped_url):
                        self.logger.info(f"Adding {scraped_url} to frontier")
                        self.frontier.add_url(scraped_url)
                self.frontier.mark_url_complete(tbd_url)
                
            except Exception as e:
                self.logger.error(f"An error occurred: {e}")

    def stop(self):
        self.stop_worker = True

    def scrape(self, resp):
        links = self.extract_next_links(resp)
        return [link for link in links if is_valid(link)]

    def extract_next_links(self, resp):
        try:
            content_type = resp.headers.get('Content-Type', '')

            if 'xml' in content_type:
                soup = BeautifulSoup(resp.text, features="xml")
                sitemap_tags = soup.find_all('loc')
                extracted_urls = [tag.text for tag in sitemap_tags]

            elif 'html' in content_type:
                soup = BeautifulSoup(resp.text, 'html.parser')
                a_tags = soup.find_all('a')
                extracted_urls = [urljoin(resp.raw_url, a.get('href')) for a in a_tags]

            else:
                self.logger.warning(f"Unexpected content type {content_type} for URL {resp.url}")
                return []

            valid_urls = []
            for url in extracted_urls:
                if is_valid(url):
                    urlhash = get_urlhash(url)
                    if urlhash not in self.frontier.save_file:
                        valid_urls.append(url)
                    else:
                        self.logger.info(f"{url} is already in the frontier")

            return valid_urls

        except Exception as e:
            self.logger.error(f"An error occurred while extracting links from {resp.url}: {e}")
            return []

