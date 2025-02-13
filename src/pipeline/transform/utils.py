def identify_urls_from_soup(soup):
    urls = set()
    for link in soup.find_all("a", href=True):
        url = link["href"]
        if url.startswith("http"):
            urls.add(url)
    return list(urls)
