# source_discovery/items.py
import scrapy


class RestaurantItem(scrapy.Item):
    name = scrapy.Field()
    location = scrapy.Field()
    source_url = scrapy.Field()
