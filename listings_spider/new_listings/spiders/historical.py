from new_listings.items import NewListingsItem
import scrapy
import json
from datetime import datetime
from scrapy.selector import Selector


class HistoricalSpider(scrapy.Spider):
    name = "historical"
    allowed_domains = ["kucoin.com", "assets.staticimg.com"]
    url_base = (
        "https://www.kucoin.com/_api/cms/articles?page={}&pageSize=100&category=listing"
    )
    start_urls = [
        "https://www.kucoin.com/_api/cms/articles?page=1&pageSize=100&category=listing"
    ]
    child_base_url = "https://assets.staticimg.com/cms/articles"
    current_page = 1
    max_page = 7  # Confirm on Kucoin

    def parse(self, response):
        self.logger.info("Parse function called on {}".format(response.url))

        formatted_resp = json.loads(response.text)

        print(len(formatted_resp["items"]))
        for coin_details in formatted_resp["items"]:
            item = NewListingsItem()

            title = coin_details["title"]
            item["id"] = coin_details["id"]
            item["title"] = title
            item["summary"] = coin_details["summary"]
            item["publish_ts"] = coin_details["publish_ts"]
            item["is_new"] = coin_details["is_new"]
            item["path"] = coin_details["path"]
            item["hot"] = coin_details["hot"]
            item["ticker"] = title[title.find("(") + 1 : title.find(")")]

            child_url = self.child_base_url + coin_details["path"] + ".json"
            yield response.follow(
                child_url, callback=self.parse_child, meta={"listing": item}
            )

        self.current_page = self.current_page + 1
        if self.current_page < self.max_page:
            abs_url = self.url_base.format(self.current_page)
            yield scrapy.Request(url=abs_url, callback=self.parse)

    def parse_child(self, response):
        prefix = "Trading:"
        response_body = json.loads(response.text)
        raw_resp = response_body["content"]
        raw_trading_date_str = raw_resp.split(prefix)[1].split("</span>")[0]
        raw_trading_date_str = raw_trading_date_str.split("</strong>")[-1]
        trading_date_str = raw_trading_date_str.split("(UTC)")[0].strip()
        trading_date = datetime.strptime(trading_date_str, "%H:%M on %B %d, %Y")
        listing_data = response.meta["listing"]
        listing_data["trading_ts"] = int(trading_date.timestamp())
        listing_data["tags"] = self.fetch_tags(response_body["content"])

        yield listing_data

    def fetch_tags(self, raw_response):
        start = "Tags:"
        end = "Project Summary"
        tags = list()
        try:
            tags_row = raw_response.split(start)[1].split(end)[0]
            tag_row_selector = Selector(text=tags_row)
            tags_text = tag_row_selector.xpath("//span/text()").getall()
            tags = filter(lambda x: ("," not in x), tags_text)
            tags = list(tags)
        except:
            print("tags not preset in listing")

        return tags
