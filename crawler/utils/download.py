import requests
from crawler.utils.response import Response

def download(url, config, logger=None):
    headers = {"User-Agent": config.user_agent}
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            return Response(resp)
        else:
            logger.error(f"Error downloading {url} - HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return None
