import scrapy


class GetQuotesSpider(scrapy.Spider):
    name = 'get_quotes'
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["https://quotes.toscrape.com"]

    def parse(self, response):
        pass
