import json
import scrapy
from scrapy.crawler import CrawlerProcess
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# Підключення до бази даних MongoDB через MongoDB Atlas
client = MongoClient(
    "mongodb+srv://serg_T:mongoDB_1234@sadatabgoit.pgfb1vk.mongodb.net/?retryWrites=true&w=majority&appName=SAdataBgoIT",
    server_api=ServerApi('1')
)
# Вибір бази даних та колекції або створення
db = client.quotes

# Клас, що відповідає за парсинг веб-сторінок з цитатами
class QuotesSpider(scrapy.Spider):
    name = 'get_quotes'
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["https://quotes.toscrape.com/"]
    
    # Ініціалізація списків для зберігання даних про цитати та авторів
    def __init__(self):
        self.authors = []
        self.quotes = []

    def parse(self, response):
        
        for q in response.xpath("/html//div[@class='quote']"):
            # Отримання тексту цитати, автора та тегів
            quote_text = q.xpath(".//span[@class='text']/text()").get()
            author = q.xpath(".//small[@class='author']/text()").get()
            tags = q.xpath(".//div[@class='tags']/a[@class='tag']/text()").getall()
            
            # Зберігання даних про цитату у відповідний словник
            self.quotes.append({
                "tags": tags,
                "author": author,
                "quote": quote_text
            })
        
        
        next_page = response.css('li.next a::attr(href)').get()
        # Якщо наступна сторінка є, то запускаємо її парсінг 
        if next_page:
            yield response.follow(next_page, callback=self.parse)

        
        author_urls = response.css('a[href^="/author/"]::attr(href)').getall()
        
        for author_url in author_urls:
            yield response.follow(author_url, callback=self.parse_author)

    # Метод для парсингу сторінок авторів
    def parse_author(self, response):
        author_title = response.css("h3.author-title::text").get().strip()
        born = response.css('span.author-born-date::text').get().strip()
        description = response.css('div.author-description::text').get().strip()
        born_location = response.css('span.author-born-location::text').get().strip()

        # Зберігання даних про автора у відповідний словник
        self.authors.append({
            "fullname": author_title,
            "born_date": born,
            "born_location": born_location,
            "description" : description
        })

    # Метод, який викликається після завершення парсингу
    def closed(self, reason):


        # Збереження даних у JSON-файли
        with open('quotes.json', 'w', encoding='utf-8') as quotes_file:
            json.dump(self.quotes, quotes_file, indent=4, ensure_ascii=False)
        
        
        with open('authors.json', 'w', encoding='utf-8') as authors_file:
            json.dump(self.authors, authors_file, indent=4, ensure_ascii=False)

        # Збереження даних у базу даних MongoDB     
        quotes_collection = db.quotes
        quotes_collection.insert_many(self.quotes)

        
        authors_collection = db.authors
        authors_collection.insert_many(self.authors)

if __name__ == "__main__":
    # Запуск процесу скрапінгу
    process = CrawlerProcess()
    process.crawl(QuotesSpider)
    process.start()



