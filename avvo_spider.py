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
CURRENT_PAGE_FILENAME = 'current_page.txt'

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
    no_lawyer_links_processed = {}
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
        self.lawyers_pages_pipeline = AvvoLawyerPagesPipeline()
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
        for i in tqdm(range(1, no_pages + 1)):
            link = f"https://www.avvo.com/all-lawyers/sitemap.xml?page={i}"
            # If the link has already been scraped then skip it
            if link in self.lawyers_pages_pipeline.link_dict:
                continue
            new_request = scrapy.Request(
                link, 
                callback=self.extract_laywers,
                headers=self.headers,
                cb_kwargs={'page_no': i}
            )
            if i == 5:
                break
            yield new_request
            
        self.lawyers_pages_pipeline.save_link_dict()


        # Dont repeat the pages already scraped
        current_page  = self.get_current_page()
            
        print(f'Resuming from page {current_page}')
        for i in tqdm(range(current_page, no_pages+1)):
            page_links = self.lawyers_pages_pipeline.link_dict[i]
            for link in page_links:
                new_request = scrapy.Request(
                    link, 
                    headers=self.headers,
                    cb_kwargs={'page_no': i}
                )
                yield new_request
    
    def get_current_page(self):
        if os.path.exists(CURRENT_PAGE_FILENAME):
            with open(CURRENT_PAGE_FILENAME, 'r') as f:
                return int(f.read())
        else:
            return 1
    
    def save_current_page(self, current_page):
        print(f'Scraped page {current_page}')
        with open(CURRENT_PAGE_FILENAME, 'w') as f:
            f.write(str(current_page))

    def extract_laywers(self, response, page_no = None):
        """
        From a page with N lawyers extract the indvidual links to each lawyer
        """
        logger.debug(f'On page {response.url}, extracting the lawyer pages')
        print('Extracting lawyers from page')
        lawyer_links = response.xpath(
            '//section/a[contains(@class, "search-result-lawyer-name")]/@href'
        ).getall()
        lawyer_links = ['https://www.avvo.com' + link for link in lawyer_links]
        print(f'The links are {lawyer_links}')
        self.lawyers_pages_pipeline.process_link(response.url, lawyer_links, page_no)

    def parse(self, response, page_no):
        """
        Parse each lawyers page individually
        """
        # If the total amount of lawyers has been scraped the save the current page
        if page_no in self.no_lawyer_links_processed:
            self.no_lawyer_links_processed[page_no] += 1
            if self.no_lawyer_links_processed[page_no] == 20:
                self.save_current_page(page_no)
        else:
            self.no_lawyer_links_processed[page_no] = 1

        logger.debug("Parsing Item")
        item = AvvoItem()

        item['timestamp'] = time.time() 
        item['lawyer_name'] = response.xpath('//h1[@class = "lawyer-name"]/span/text()').get() 
        item['bio'] = [response.xpath('//div[@class="about-tagline"]/span/text()').get()] + response.xpath('//div[@id="bioExpandCollapse"]/p/text()').getall()
        item['phone_number'] = response.xpath('//span[@class="overridable-lawyer-phone-copy"]/text()').get() 
        item['addresses'] = self.get_addresses(response) 
        item['state'] = response.xpath('//li[@data-name="state"]/a/span/text()').get() 
        item['city'] = response.xpath('//li[@data-name="city"]/a/span/text()').get() 
        item['practice_area'] = response.xpath('//li[@data-name="practice_area"]/a/span/text()').get() 
        item['number_of_reviews'] = response.xpath('//div[@class="client-reviews"]/a/text()').get() 
        item['rating'] = response.xpath('//span[@class="rating-value"]/text()').get() 
        item['years_liscenced'] = response.xpath('//p[contains(@class, "header-licensed")]/text()').get() 
        item['states_registered'] = self.get_states_registered(response)
        item['resume'] = self.get_resume(response) 
        item['avvo_url'] = response.url 
        yield item
        

    def get_addresses(self, response):
        address_sections = response.xpath('//div[@class="contact-item"]')
        addresses = []
        for address in address_sections:
            new_address = {}
            new_address['law_office'] = address.xpath('//div[@class="contact-firm"]/text()').get()
            new_address['phone_number'] = address.xpath('//span[@class="overridable-lawyer-phone-copy"]/text()').get()
            new_address['address'] = address.xpath('//div[@class="contact-address"]/div/div/text()').getall()
            new_address['website'] = address.xpath('//div[contains(@class, "contact-website")]/span/a/text()').get()
            addresses.append(new_address)
        return addresses 

    def get_states_registered(self, response):
        states_sections = response.xpath('//div[@class="single-license"]')
        states = []
        for state_section in states_sections:
            state = {}
            state['state'] = state_section.xpath('//div[@class="license-state"]/span[@class="value"]/text()').get() 
            state['status'] = state_section.xpath('//div[@class="license-status"]/span[@class="value"]/text()').get()
            state['acquired'] = state_section.xpath('//div[@class="license-acquired"]/span[@class="value"]/text()').get()
            state['updated'] = state_section.xpath('//div[@class="license-updated"]/span[@class="value"]/text()').get()
            states.append(state)
        return states

    def get_resume(self, response):
        resume_sections = response.xpath('//div[@class="resume-section"]')
        resume = []
        for section in resume_sections:
            resume_section = {}
            resume_section['title'] = section.xpath('//h2/text()').get()
            resume_sub_sections = section.xpath('//ul')
            sub_sections = []
            for sub_section in resume_sub_sections:
                sub_sections.append(sub_section.xpath('//li/text()').getall())
            resume_section['subsections'] = sub_sections
            resume.append(resume_section)
        return resume


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


class AvvoLawyerPagesPipeline:
    """Pipeline class for saving lawyer link pages """
    no_pages_saved = 0
    save_frequency = 10
    def __init__(self):
        """Initialize the pipeline when the Avvo spider starts.

        Parameters:
        - spider (Spider): The Avvo spider instance that is starting up.
        """
        if not os.path.exists(LINK_OUTPUT_FILENAME):
            self.link_dict = {} 
        else:
            self.link_dict = json.loads(open(LINK_OUTPUT_FILENAME, 'r').read())

    def process_link(self, link, lawyer_links, page_no):
        """Process a scraped item from the Avvo spider.

        Parameters:
        - item (dict): A dictionary representing a lawyer's data, as scraped by the spider.
        - spider (Spider): The Avvo spider instance that scraped the item.
        """
        self.no_pages_saved += 1
        self.link_dict[page_no] = lawyer_links
        if self.no_pages_saved % self.save_frequency:
            self.save_link_dict()

    def save_link_dict(self):
        """Save the lawyer data dictionary to a JSON file."""
        with open(LINK_OUTPUT_FILENAME, 'w') as f:
            json.dump(self.link_dict, f)

class AvvoItem(scrapy.Item):
    timestamp = scrapy.Field()
    lawyer_name = scrapy.Field()
    bio = scrapy.Field()
    phone_number = scrapy.Field()
    addresses = scrapy.Field()
    website = scrapy.Field()
    state = scrapy.Field()
    city = scrapy.Field()
    practice_area = scrapy.Field()
    number_of_reviews = scrapy.Field()
    rating = scrapy.Field()
    years_liscenced = scrapy.Field()
    states_registered = scrapy.Field()
    resume = scrapy.Field()
    avvo_url = scrapy.Field()


def main(event, context):
    process = CrawlerProcess(
        {
            "BOT_NAME": "avvo_spider",
            "ROBOTSTXT_OBEY": False,
            "CONCURRENT_REQUESTS": 16,
            "COOKIES_ENABLED": True,
            "ITEM_PIPELINES": {
                AvvoLawyerPipeline: 300,
            },
            "CONCURRENT_REQUESTS_PER_DOMAIN": 32,
            "CONCRRENT_ITEMS": 64,
            "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
            "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
            "DOWNLOAD_HANDLERS": {
                'https': 'scrapy.core.downloader.handlers.http2.H2DownloadHandler',
                'http': 'scrapy.core.downloader.handlers.http2.H2DownloadHandler',
            },
            #'LOG_ENABLED': False
        }
    )

    sp_logger = logging.getLogger('scrapy')
    sp_logger.setLevel(logging.INFO)
    process.crawl(AvvoSpider)
    process.start() 


if __name__ == "__main__":
    main("", "")
