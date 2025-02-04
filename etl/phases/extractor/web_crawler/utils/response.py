import os
import json
from crawler.utils import get_urlhash

class Response(object):
    def __init__(self, raw_url=None, url=None, status_code=None, headers=None, content=None, text=None, error=None):
        self.raw_url = raw_url
        self.url = url
        self.status_code = status_code
        self.headers = headers
        self.content = content
        self.text = text
        self.error = error
        self.json_data = {"url": url, "text": text}
        self.filename = None

    def to_json(self, directory="corpus_directory"):
        os.makedirs(directory, exist_ok=True)
        self.filename = os.path.join(directory, f'{get_urlhash(self.raw_url)}.json')
        try:
            with open(self.filename, 'w') as file:
                json.dump(self.json_data, file)
        except Exception as e:
            print(f"An error occurred when writing to file: {e}")
