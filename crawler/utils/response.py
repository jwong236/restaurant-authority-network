import json
import os

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

    def to_json(self, directory):
        self.filename = os.path.join(directory, f'{self.raw_url.replace("/", "_")}.json')
        with open(self.filename, 'w') as file:
            json.dump(self.json_data, file)
