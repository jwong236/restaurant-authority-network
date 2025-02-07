import os
import shelve
import shutil
from crawler.utils import get_urlhash, is_valid
from utils import get_logger

class Frontier(object):
    def __init__(self, config, resume):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.save_file = None

        if not resume:
            self.logger.info(f"Starting from scratch... clearing directories")
            self.clear_data()
            self.save_file = shelve.open(self.config.SAVE_PATH)
            for url in self.config.SEED_URLS:
                self.add_url(url)
        elif resume:
            if (os.path.exists(self.config.SAVE_PATH + ".dat") and 
                os.path.exists(self.config.SAVE_PATH + ".dir") and
                os.path.exists(self.config.SAVE_PATH + ".bak")):
                self.logger.info(f"Resuming crawl, reading save file {self.config.SAVE_PATH}")
                self.save_file = shelve.open(self.config.SAVE_PATH)
                self._parse_save_file()
            else:
                self.logger.info(f"Resuming crawl, save file not found {self.config.SAVE_PATH}. Starting from scratch...")
                self.clear_data()
                self.save_file = shelve.open(self.config.SAVE_PATH)
                for url in self.config.SEED_URLS:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save_file)
        tbd_count = 0
        for url, completed in self.save_file.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(f"Parsed save file. Found {tbd_count} urls to be downloaded from {total_count} total urls discovered.")

    def get_tbd_url(self):
        try:
            tbd_url = self.to_be_downloaded.pop()
            self.logger.info(f"Removed {tbd_url} from frontier")
            return tbd_url
        except IndexError:
            return None

    def add_url(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save_file:
            self.logger.info(f"Adding {urlhash} to frontier")
            self.save_file[urlhash] = (url, False)
            self.save_file.sync()
            self.to_be_downloaded.append(url)
        else:
            self.logger.info(f"urlhash {urlhash} is already in frontier! (Duplicate)")
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save_file:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")
        self.save_file[urlhash] = (url, True)
        self.logger.info(f"Marked {urlhash} as complete")
        self.save_file.sync()

    def clear_data(self):
        # Clear the save directory
        save_dir = os.path.dirname(self.config.SAVE_PATH)
        if os.path.exists(save_dir):
            shutil.rmtree(save_dir)
            self.logger.info(f"Removed save file directory {save_dir}.")

        # Clear the corpus directory
        if os.path.exists(self.config.CORPUS_DIRECTORY):
            shutil.rmtree(self.config.CORPUS_DIRECTORY)
            self.logger.info(f"Removed corpus directory {self.config.CORPUS_DIRECTORY}.")

        # Create the save directory if it does not exist
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
            self.logger.info(f"Created save file directory {save_dir}.")

        # Create the corpus directory if it does not exist
        if not os.path.exists(self.config.CORPUS_DIRECTORY):
            os.makedirs(self.config.CORPUS_DIRECTORY)
            self.logger.info(f"Created corpus directory {self.config.CORPUS_DIRECTORY}.")

