# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewListingsItem(scrapy.Item):
    id = scrapy.Field()
    title = scrapy.Field()
    summary = scrapy.Field()
    publish_ts = scrapy.Field()  # UTC Timestamp
    trading_ts = scrapy.Field()  # UTC Timestamp
    is_new = scrapy.Field()
    path = scrapy.Field()
    hot = scrapy.Field()
    ticker = scrapy.Field()
    tags = scrapy.Field()
