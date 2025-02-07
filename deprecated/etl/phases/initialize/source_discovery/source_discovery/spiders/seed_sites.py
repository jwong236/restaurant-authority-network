import scrapy
from scrapy.spiders import SitemapSpider
from source_discovery.items import RestaurantItem


class SeedSitesSpider(SitemapSpider):
    name = "seed_sites"
    allowed_domains = ["guide.michelin.com"]

    sitemap_urls = ["https://guide.michelin.com/sitemap.xml"]

    sitemap_rules = [(r"/us/.*/restaurant/", "parse_michelin")]

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "USER_AGENT": "LearningBot/1.0 (+mailto:jacobsunsetbluff@gmail.com)",
        "DOWNLOAD_DELAY": 3,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS": 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 2,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 2,
        "RETRY_HTTP_CODES": [500, 502, 503, 504],
        "LOG_FILE": "scrapy_logs.txt",
        "LOG_LEVEL": "DEBUG",
        "CLOSESPIDER_ERRORCOUNT": 5,
    }

    def sitemap_filter(self, entries):
        """Final filter before requests are created"""
        for entry in entries:
            url = entry["loc"]
            if "michelin" in url:
                if "/us/" in url and "/restaurant/" in url:
                    yield entry

    def parse_michelin(self, response):
        """Extract restaurant details from Michelin Guide pages (only U.S. restaurants)."""

        name = response.css("h1.data-sheet__title::text").get(default="").strip()
        location = (
            response.css("div.data-sheet__block--text::text").get(default="").strip()
        )

        if not location.endswith(", USA"):
            self.logger.warning(f"Skipping non-US restaurant: {name} - {location}")
            return

        item = RestaurantItem()
        item["name"] = name
        item["location"] = location
        item["source_url"] = response.url
        yield item
