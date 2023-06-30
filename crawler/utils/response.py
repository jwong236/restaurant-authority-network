import pickle

class Response(object):
    def __init__(self, resp):
        self.status = resp.status_code if hasattr(resp, 'status_code') else None
        self.headers = dict(resp.headers) if hasattr(resp, 'headers') else None
        self.content = resp.content if hasattr(resp, 'content') else None
        self.text = resp.text if hasattr(resp, 'text') else None
        self.error = resp.reason if hasattr(resp, 'reason') else None
        try:
            self.raw_response = (
                pickle.loads(resp["response"])
                if "response" in resp else
                None)
        except TypeError:
            self.raw_response = None