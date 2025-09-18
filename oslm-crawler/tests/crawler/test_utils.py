import pytest
import time
from pprint import pprint
from oslm_crawler.crawler.utils import WebDriverPool
from concurrent.futures import ThreadPoolExecutor, as_completed

def scrape_website(url, drivers_pool):
    with drivers_pool.get_driver() as driver:
        driver.get(url)
        time.sleep(1)
        return {
            "url": url,
            "title": driver.title,
            "source_length": len(driver.page_source)
        }


def test_webdriver_pool():
    with (WebDriverPool(size=4) as driver_pool, ThreadPoolExecutor(4) as executor):
        urls = [
            "https://www.python.org",
            "https://www.selenium.dev",
            "https://www.google.com",
            "https://www.github.com",
            "https://www.stackoverflow.com",
            "https://www.wikipedia.org",
            "https://www.mozilla.org",
            "https://www.apache.org",
            "https://www.linux.org",
            "https://www.docker.com"
        ]
        
        futures = [executor.submit(scrape_website, url, driver_pool) for url in urls]
        
        for future in as_completed(futures):
            result = future.result()
            pprint(result)
