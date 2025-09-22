# Install asyncio reactor before importing scrapy
from twisted.internet import asyncioreactor
try:
    asyncioreactor.install()
except Exception:
    pass

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from bookscraper.spiders.bookspider import BookspiderSpider

def main():
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(BookspiderSpider)
    process.start()  # blocks until crawling is finished

if __name__ == "__main__":
    main()
