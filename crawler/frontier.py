import os
import shelve

from crawler.utils import get_urlhash, normalize, is_valid
from utils import get_logger

class Frontier(object):
    def __init__(self, config, resume):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.save_file = None

        
        if not os.path.exists(self.config.save_path) and not resume:
            # Save file doesn't exist and user 
            self.logger.info(f"Save file does not exist {self.config.save_path}, starting from seed.")
        elif os.path.exists(self.config.save_path) and resume:
            # Save file does exists, but request to start from seed.
            self.logger.info(f"Found save file {self.config.save_path}, deleting it.")
            os.remove(self.config.save_path)
    
        # Load existing save file, or create one if it does not exist.
        self.save_file = shelve.open(self.config.save_path)
        if resume:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save_file:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save_file)
        tbd_count = 0
        for url, completed in self.save_file.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(f"Found {tbd_count} urls to be downloaded from {total_count} total urls discovered.")

    def get_tbd_url(self):
        try:
            return self.to_be_downloaded.pop()
        except IndexError:
            return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save_file:
            self.save_file[urlhash] = (url, False)
            self.save_file.sync()
            self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save_file:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")
        self.save_file[urlhash] = (url, True)
        self.save_file.sync()