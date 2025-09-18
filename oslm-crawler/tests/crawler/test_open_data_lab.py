import pytest
from oslm_crawler.crawler.utils import init_driver
from oslm_crawler.crawler.open_data_lab import (
    OpenDataLabPage
)
    

@pytest.fixture(scope='class')
def driver(request):
    web_driver = init_driver()
    request.cls.driver = web_driver
    yield
    web_driver.quit()
    
@pytest.mark.parametrize("link", [
    "https://opendatalab.com/?createdBy=12199&pageNo=0&pageSize=12&sort=downloadCount",
    "https://opendatalab.com/?createdBy=11828&pageNo=0&pageSize=12&sort=downloadCount",
    "https://opendatalab.com/?createdBy=12157&pageNo=0&pageSize=12&sort=downloadCount",
    "https://opendatalab.com/?createdBy=12589&pageNo=0&pageSize=12&sort=downloadCount",
    "https://opendatalab.com/?createdBy=1678533&pageNo=0&pageSize=12&sort=downloadCount"
])
@pytest.mark.usefixtures("driver")
class TestOpenDataLabPage:
    
    def test_crawl(self, link):
        page = OpenDataLabPage(self.driver, link)
        res = page.scrape()
        assert isinstance(res, list)
