# AVVO_Scraper

Scraper for AVVO lawyer website. Uses headers to spoof cloudflare, and rotating proxies to prevent being blocked by rate limiting. There is an sh command for testing included.

Does not work currently as scrapy does not support HTTP 2.0 with proxies. 

To run:

`pip install -r requirements.txt`
`python avvo_spider.py`
