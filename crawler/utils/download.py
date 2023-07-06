import requests
from crawler.utils.response import Response

def download(raw_url, config, logger=None):
    error = None
    resp = None
    try:
        resp = requests.get(raw_url, headers={"User-Agent": config.USER_AGENT})
        if resp.status_code == 200:
            if logger is not None:
                logger.info(f"Successfully downloaded: {raw_url}")
            return Response(raw_url=raw_url, url=resp.url, status_code=resp.status_code, headers=resp.headers, content=resp.content, text=resp.text)
        else:
            error = f"Error downloading {raw_url}: HTTP {resp.status_code}"
            if logger is not None:
                logger.error(error)
            return Response(status_code=resp.status_code, error=error)
    except Exception as e:
        error = f"Error downloading {raw_url}: {e}"
        if resp is not None:
            if logger is not None:
                logger.error(error)
            return Response(status_code=resp.status_code, error=error)
        else:
            if logger is not None:
                logger.error(error)
            return Response(error=error)
