import scrapy
import logging
import json
import os
import sqlite3
from jsonschema import validate, ValidationError

class OzoneSpider(scrapy.Spider):
    name = "ozone"
    allowed_domain = ["ozone.bg"]
    start_urls = ["https://www.ozone.bg/"]

    def __init__(self):
        self.conn = sqlite3.connect('smartphone_data.db')
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS smartphone
                            (url TEXT PRIMARY KEY, title TEXT, price TEXT)''')
        self.conn.commit()

        schema_path = os.path.join(os.path.dirname(__file__), '..', 'schema.json')
        with open(schema_path) as f:
             self.schema = json.load(f)

    def close(self):
         self.conn.close()

    def parse(self, response):
        smartphone_link = response.xpath('((//div[@class="category-list-box"])[6]//a)[4]/@href').get()
        yield response.follow(smartphone_link, callback=self.parse_apple)

    def parse_apple(self, response):
        smartphone_link = response.xpath('(//div[@class="col-xs-6 widget-banner-xs-6"])[2]//a/@href').get()
        yield response.follow(smartphone_link, callback=self.parse_smartphone_links)

    def parse_smartphone_links(self, response):
        smartphone_links = response.xpath('//div[@class="col-xs-3 five-on-a-row"]//a/@href').getall()
        for smartphone_link in smartphone_links:
            yield response.follow(smartphone_link, callback=self.parse_smartphone_page)

    def parse_smartphone_page(self, response):
            url = response.url
            title = response.xpath('//h1[@itemprop="name"]/text()').get()
            price = response.xpath('//meta[@itemprop="price"]/@content').get() + " " + response.xpath('//meta[@itemprop="priceCurrency"]/@content').get()

            product_data = {
                'url': url,
                'title': title,
                'price': price
            }
            
            try:
                 validate(instance=product_data, schema=self.schema)
            except ValidationError as e:
                 logging.error("Validation error: %s", str(e))
                 return
            
            self.cursor.execute("SELECT * FROM smartphone WHERE url=?", (url,))
            products = self.cursor.fetchone()

            if not products:
                try:
                    self.cursor.execute("INSERT INTO smartphone (url, title, price) VALUES (?, ?, ?)",
                                        (url,
                                        title,
                                        price))
                    self.conn.commit()
                    logging.info("Data inserted into SQLite successfuly.")
                except Exception as e:
                    logging.error("Error parsing news element: %s", str(e))

            yield product_data

                    
