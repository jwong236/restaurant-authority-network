# ./src/pipeline/transform/url_utils.py
import re
from urllib.parse import urlparse, urljoin


def extract_homepage(target_url):
    """
    Extracts the homepage from the given target URL.
    Example:
        Input: "http://www.consumingla.com/index/page1"
        Output: "https://www.consumingla.com/"
    """
    parsed_url = urlparse(target_url)
    homepage = f"{parsed_url.scheme}://{parsed_url.netloc}/"
    return homepage


def identify_urls_from_soup(soup, base_url):
    """
    Extracts relevant URLs only from the main content, headers, navigation, and now <div><p> sections.
    """
    urls = set()
    main_content = soup.find("main") or soup.find("div", class_="content")
    navbar = soup.find("nav")
    headers = soup.find_all(["h1", "h2", "h3"])
    div_paragraphs = soup.find_all("div")

    for section in [main_content, navbar] + headers + div_paragraphs:
        if section:
            for link in section.find_all("a", href=True):
                url = urljoin(
                    base_url, link["href"]
                )  # Convert relative URLs to absolute
                if url.startswith("http"):
                    urls.add(url)

    return list(urls)
