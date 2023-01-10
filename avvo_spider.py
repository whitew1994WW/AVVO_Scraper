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
import logging
from tqdm import tqdm
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)



OUTPUT_FILENAME = 'lawyer_details.json'
LINK_OUTPUT_FILENAME = 'lawyer_linke.json'
NO_LAWYERS_PER_PAGE = 20
CURRENT_PAGES_SCRAPED_FILENAME = 'pages_scraped.txt'

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
    lawyers_scraped = 0 
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
        #self.pages_scraped = self.get_pages_scraped()
        
        return category_pages

    def extract_laywer_pages(self, response):
        """
        Extracts the total number of laywers from the page and automatically creates the paginated search pages.
        """
        number_of_links = response.xpath('//*[@id="title-total-count"]/text()').get()
        match = re.search(r'\d+', number_of_links)
        if match:
            number_links = int(match.group(0))
            print(f'Total number of lawyers found is {number_links}')
        else:
            raise ValueError('Failed to scrape the number of links from the match')
        no_pages = (number_links // NO_LAWYERS_PER_PAGE) + 1
        print(f'There will be {no_pages} pages of lawyers to scrape')
        # Extract all the lawyer links first
        for i in range(1, 500 + 1):
            link = f"https://www.avvo.com/all-lawyers/sitemap.xml?page={i}"
            # If the page has already been scraped then skip it
            #if i in self.pages_scraped:
            #    continue
            new_request = scrapy.Request(
                link, 
                headers=self.headers,
                cb_kwargs={'page_no': i}
            )
            yield new_request
            
    
    def get_pages_scraped(self):
        if os.path.exists(CURRENT_PAGES_SCRAPED_FILENAME):
            with open(CURRENT_PAGES_SCRAPED_FILENAME, 'r') as f:
                return json.load(f)
        else:
            return []
    """ 
    def save_current_pages_scraped(self):
        if len(self.pages_scraped) % 100:
            print(f'Scraped {len(self.pages_scraped)} pages')
        with open(CURRENT_PAGES_SCRAPED_FILENAME, 'w') as f:
           json.dump(self.pages_scraped, f) 
    """
    def parse(self, response, page_no = None):
        """
        From a page with N lawyers extract the indvidual links to each lawyer
        """
        
        lawyer_sections = response.xpath('//ul[contains(@class, "lawyer-search-results")]/li')
        lawyers = []
        for section in lawyer_sections:
            lawyer_item = self.parse_lawyer_section(section) 
            self.lawyers_scraped += 1
            if (self.lawyers_scraped % 2000) == 0:
                print(f'Scraped {self.lawyers_scraped} lawyers')
            yield lawyer_item

        #self.pages_scraped.append(page_no)
        #self.save_current_pages_scraped()

   
    def parse_lawyer_section(self, lawyer_section):
        lawyer_item = AvvoItem()
        lawyer_item['timestamp'] = time.time()
        lawyer_item['lawyer_name'] = lawyer_section.xpath('.//a[contains(@class, "search-result-lawyer-name")]/text()').get()
        lawyer_item['bio'] = lawyer_section.xpath('.//div[contains(@class, "lawyer-search-result-intro")]/text()').get()
        lawyer_item['phone_number'] = lawyer_section.xpath('.//span[contains(@class, "overridable-lawyer-phone-copy")]/text()').get()
        lawyer_item['website'] = lawyer_section.xpath('.//a[contains(@class, "v-cta-organic-desktop-website")]/@href').get()
        lawyer_item['liscence'] = lawyer_section.xpath('.//div[contains(@id, "expanded-preview-data")]/section[3]/div[2]/text()').get()
        lawyer_item['practice_area'] = lawyer_section.xpath('.//div[contains(@id, "expanded-preview-data")]/section[1]/div[2]//text()').getall()
        lawyer_item['cost'] = lawyer_section.xpath('.//div[contains(@id, "expanded-preview-data")]/section[2]/div[2]//text()').getall()
        lawyer_item['number_of_reviews'] = lawyer_section.xpath('.//div[@class="rating-flex"]/section/a/small/text()').get()
        lawyer_item['rating'] = lawyer_section.xpath('.//div[@class="rating-flex"]/section/a/span/span[6]/text()').get()
        lawyer_item['years_liscenced'] = lawyer_section.xpath('.//time/text()').get()
        lawyer_item['avvo_rating'] = lawyer_section.xpath('.//div[@class="v-organic-rating-section"]/small/strong/text()').get()
        lawyer_item['avvo_url'] = 'https://www.avvo.com' + lawyer_section.xpath('.//a[contains(@class, "search-result-lawyer-name")]/@href').get() 
        lawyer_item['lawyer_additional_json'] = lawyer_section.xpath('.//div/div/script/text()').get()
        return lawyer_item




class AvvoItem(scrapy.Item):
    timestamp = scrapy.Field()
    lawyer_name = scrapy.Field()
    bio = scrapy.Field()
    phone_number = scrapy.Field()
    address = scrapy.Field()
    website = scrapy.Field()
    liscence = scrapy.Field()
    practice_area = scrapy.Field()
    cost = scrapy.Field()
    number_of_reviews = scrapy.Field()
    rating = scrapy.Field()
    years_liscenced = scrapy.Field()
    avvo_url = scrapy.Field()
    avvo_rating = scrapy.Field()
    lawyer_additional_json = scrapy.Field()

class AvvoLawyerPipeline:
    first = True

    def process_item(self, item, spider):  # default method
        # calling dumps to create json data.
        if self.first:
            line = "\n" + json.dumps(dict(item))
            self.first = False
        else:
            line = ",\n" + json.dumps(dict(item))
        # converting item to dict above, since dumps only intakes dict.
        self.file.write(line)  # writing content in output file.
        return item

    def open_spider(self, spider):
        if os.path.exists(OUTPUT_FILENAME):
            self.file = open(
                OUTPUT_FILENAME,
                "a+",
            )
        else:
            self.file = open(
                OUTPUT_FILENAME,
                "w",
            )
            self.file.write("[")

    def close_spider(self, spider):
        self.file.write("]")
        self.file.close()


def main(event, context):
    process = CrawlerProcess(
        {
            "BOT_NAME": "avvo_spider",
            "ROBOTSTXT_OBEY": False,
            "CONCURRENT_REQUESTS": 4,
            "COOKIES_ENABLED": True,
            "ITEM_PIPELINES": {
                AvvoLawyerPipeline: 300,
            },
            "DOWNLOAD_DELAY": 1,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
            "CONCRRENT_ITEMS": 16,
            "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
            "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
            "DOWNLOAD_HANDLERS": {
                #'https': 'scrapy_h2_proxy.H2DownloadHandler',
                'https': 'scrapy.core.downloader.handlers.http2.H2DownloadHandler',
                'http': 'scrapy.core.downloader.handlers.http2.H2DownloadHandler',
            },
            "DOWNLOADER_MIDDLEWARES": {
                'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
                'rotating_proxies.middlewares.BanDetectionMiddleware': 620,            
            },
            "ROTATING_PROXY_LIST": [
                "https://163.116.158.183:8081",
                "https://61.220.170.133:8000",
                "https://141.147.158.11:8080",
                "https://163.116.158.142:8081",
                "https://163.116.158.115:8081",
                "https://163.116.177.30:808",
                "https://174.70.1.210:8080",
                "https://158.69.53.98:9300",
                "https://163.116.158.23:8081",
                "https://163.116.177.47:808",
                "https://163.116.158.25:8081",
                "https://163.116.177.48:808",
                "https://80.169.156.52:80",
                "https://200.105.215.22:33630",
                "https://212.80.213.94:8080",
                "https://163.116.177.44:808",
                "https://154.239.1.77:1981",
                "https://163.116.177.31:808",
                "https://47.243.180.142:808",
                "https://194.87.188.114:8000",
                "https://187.130.139.197:8080",
                "https://163.116.177.51:808",
                "https://163.116.158.118:8081",
                "https://163.116.177.42:808",
                "https://109.206.252.234:80",
                "https://163.116.158.117:8081",
                "https://163.116.248.33:808",
                "https://90.114.27.196:3128",
                "https://206.127.254.245:3129",
                "https://52.23.175.222:8118",
                "https://45.92.94.190:9090",
                "https://163.116.248.49:808",
                "https://163.116.248.46:808",
                "https://213.59.156.119:3128",
                "https://51.79.50.22:9300",
                "https://185.15.172.212:3128",
                "https://107.172.73.179:7890",
                "https://89.218.186.134:3128",
                "https://123.202.82.245:3128",
                "https://54.207.220.66:80",
                "https://198.27.74.6:9300",
                "https://87.245.186.149:8090",
                "https://177.82.85.209:3128",
                "https://161.77.218.105:3129",
                "https://161.77.218.197:3129",
                "https://179.50.16.46:8111",
                "https://111.225.153.254:8089",
                "https://13.114.216.75:80",
                "https://185.198.61.146:3128",
                "https://168.138.33.70:8080",
                "https://162.211.181.130:808",
                "https://5.189.184.6:80",
                "https://45.91.133.137:8080",
                "https://163.116.177.39:808",
                "https://163.116.177.43:808",
                "https://163.116.177.33:808",
                "https://49.0.2.242:8090",
                "https://47.243.55.21:8080",
                "https://158.69.52.218:9300",
                "https://217.6.28.219:80",
                "https://163.116.177.50:808",
                "https://163.116.177.45:808",
                "https://163.116.177.49:808",
                "https://44.230.152.143:80",
                "https://181.94.197.42:8080",
                "https://163.116.177.46:808",
                "https://190.61.88.147:8080",
                "https://163.116.177.32:808",
                "https://163.116.158.182:8081",
                "https://163.116.158.28:8081",
                "https://163.116.177.34:808",
                "https://212.14.243.29:8080",
                "https://163.116.248.40:808",
                "https://45.250.163.19:8081",
                "https://103.92.26.190:4002",
                "https://163.116.248.55:808",
                "https://163.116.158.143:8081",
                "https://163.116.248.45:808",
                "https://163.116.158.27:8081",
                "https://31.59.12.126:8080",
                "https://115.144.101.200:10000",
                "https://104.223.135.178:10000",
                "https://46.225.237.146:3128",
                "https://91.206.15.125:3128",
                "https://47.244.2.19:3128",
                "https://47.243.121.74:3128",
                "https://143.198.166.215:3128",
                "https://178.47.139.151:35102",
                "https://82.66.75.98:49400",
                "https://102.177.192.84:3128",
                "https://163.116.248.51:808",
                "https://157.245.27.9:3128",
                "https://59.15.154.69:13128",
                "https://198.59.191.234:8080",
                "https://13.127.4.162:3128",
                "https://134.238.252.143:8080",
                "https://163.116.248.56:808",
                "https://205.185.126.246:3128",
                "https://34.84.142.87:3128",
                "https://163.116.248.39:808",
            ]
            #'LOG_ENABLED': False
        }
    )

    sp_logger = logging.getLogger('scrapy')
    sp_logger.setLevel(logging.DEBUG)
    process.crawl(AvvoSpider)
    process.start() 


if __name__ == "__main__":
    main("", "")
