import scrapy
from typing import List
import re
import scrapy
from typing import List
import re
import logging
from datetime import datetime
from typing import List
from itemadapter import ItemAdapter
import json
import os
from scrapy.crawler import CrawlerProcess
import requests
import requests
import logging

# These two lines enable debugging at httplib level (requests->urllib3->http.client)
# You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# The only thing missing will be the response.body which is not logged.
try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client
http_client.HTTPConnection.debuglevel = 1

# You must initialize logging, otherwise you'll not see debug output.
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sp_logger = logging.getLogger('scrapy')
sp_logger.setLevel(logging.DEBUG)


OUTPUT_FILENAME = 'avvo.csv'
class AvvoSpider(scrapy.Spider):
    name = "avvo_spider"
    headers = {
        'User-Agent': 'NotAHeader',
        'Accept-Language': 'en-GB,en;q=0.5',
        'Referer': 'www.google.com', 
        'Accept': '*/*',
        'TE': 'trailers',
        'Upgrade-Insecure-Requests': '1',
        'Accept-Encoding': None,
    }
    items = {}

    def start_requests(self) -> List[scrapy.Request]:
        """
        Gets the starting page, which is a single search that returns all the laywers stored on avvo
        """
        logger.info('Creating scrapy request to the first page')
        category_pages: List[scrapy.Request] = [
            scrapy.Request(
                "https://www.avvo.com/all-lawyers/sitemap.xml?page=0",
                callback=self.extract_laywer_pages,
                headers=self.headers
            )
        ]
        print(f'the headers are {category_pages[0].headers}')
        return category_pages

    def extract_laywer_pages(self, response):
        """
        Extracts the total number of laywers from the page and automatically creates the paginated search pages.
        """
        print(response.text)
        links = response.xpath('//*[@id="title-total-count"]/text()').get()
        print(f'extracted {links}')
        """for link in links:
            new_request = scrapy.Request(
                link, callback=self.extract_laywers,
                headers=self.headers
            )
        """

    def extract_laywers(self, response):
        """
        From a page with N lawyers extract the indvidual links to each lawyer
        """
        pb = response.xpath(
            '//ul[@class="product-list grid"]//a[@data-auto="product-tile--title"]/@href'
        ).getall()

        return ""

    def parse(self, response):
        """
        Parse each lawyers page individually
        """
        logger.debug("Parsing Item")
        item = AvvoItem()

        self.items[response.url] = item

class AvvoPipeline:
    """Pipeline class for processing Avvo lawyer data in a Scrapy spider."""
    no_lawyers_saved = 0
    save_frequency = 10
    def __init__(self, *args, **kwargs):
        self.lawyer_dict = {}
        super().__init__(*args, **kwargs)
    def process_item(self, item, spider):
        """Process a scraped item from the Avvo spider.

        Parameters:
        - item (dict): A dictionary representing a lawyer's data, as scraped by the spider.
        - spider (Spider): The Avvo spider instance that scraped the item.
        """
        self.no_lawyers_saved += 1
        self.lawyer_dict[item['url']] = item
        if self.no_lawyers_saved % self.save_frequency:
            self.save_lawyer_dict()

    def open_spider(self, spider):
        """Initialize the pipeline when the Avvo spider starts.

        Parameters:
        - spider (Spider): The Avvo spider instance that is starting up.
        """
        if not os.path.exists(OUTPUT_FILENAME):
            self.lawyer_dict = {}
        else:
            self.lawyer_dict = json.loads(open(OUTPUT_FILENAME, 'r').read())

    def close_spider(self, spider):
        """Perform final tasks when the Avvo spider closes.

        Parameters:
        - spider (Spider): The Avvo spider instance that is closing.
        """
        self.save_lawyer_dict()
        
    def save_lawyer_dict(self):
        """Save the lawyer data dictionary to a JSON file."""
        with open(OUTPUT_FILENAME, 'w') as f:
            json.dump(self.lawyer_dict, f)


class AvvoItem(scrapy.Item):
    date_scraped = scrapy.Field()
    data = scrapy.Field()

def main(event, context):
    req = requests.get("https://www.avvo.com/all-lawyers/sitemap.xml?page=0",
                headers=AvvoSpider.headers)
    print(req.text)
    process = CrawlerProcess(
        {
            "BOT_NAME": "avvo_spider",
            "ROBOTSTXT_OBEY": False,
            "DOWNLOAD_DELAY": 0.1,
            "CONCURRENT_REQUESTS_PER_IP": 16,
            "COOKIES_ENABLED": True,
            "ITEM_PIPELINES": {
                AvvoPipeline: 300,
            },
            "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
            "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
            "DOWNLOAD_HANDLERS": {
                'https': 'scrapy.core.downloader.handlers.http2.H2DownloadHandler',
                'http': 'scrapy.core.downloader.handlers.http2.H2DownloadHandler',
            }
        }
    )

    process.crawl(AvvoSpider)
    process.start() 


if __name__ == "__main__":
    main("", "")
