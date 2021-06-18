# -*- coding: utf-8 -*-
# @Time    : 2021/6/11 上午7:40
# @Author  : daiyu
# @File    : main.py
# @Software: PyCharm


import scrapy
from scrapy.crawler import CrawlerProcess


class MySpider(scrapy.Spider):
    name = 'example'
    url = "http://www.baidu.com"

    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse)

    def parse(self, response, **kwargs):
        print(response)
        yield response


process = CrawlerProcess()

process.crawl(MySpider)
process.start()
