import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webdriver import WebDriver
from loguru import logger
from datetime import datetime
from typing import Optional, Union
from dataclasses import dataclass, field
from .utils import str2int


@dataclass
class OpenDataLabInfo:
    org: str = field(init=False, default="ShanghaiAILab")
    repo: str = field(init=False)
    dataset_name: str = field(init=False)
    total_downloads: Optional[int] = field(init=False, default=None)
    likes: Optional[int] = field(init=False, default=None)
    date_crawl: str = field()
    link: str = field()
    metadata: Optional[dict] = field(default=None)
    
    def __post_init__(self):
        self.dataset_name = self.link.rstrip('/').split('/')[-1]
        self.repo = self.link.rstrip('/').split('/')[-2]
        if self.metadata is not None:
            self.total_downloads = str2int(self.metadata['downloads'])
            self.likes = str2int(self.metadata['likes'])


class OpenDataLabPage:

    _main_parts = [
        (By.XPATH, '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[2]/div[2]/div'),
    ]
    _total_count = (By.CSS_SELECTOR, r'#root > div > div > main > div > div.layout.pt-8.flex.flex-col.bg-clip-content > div.flex-1.pt-\[57px\].mt-0 > div > div > div.flex.mb-4.items-center > div > div.text-base.mr-auto.flex.items-center.ml-4 > div')
    _page_elements = (By.XPATH, '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[2]/div[2]/div/div')
    _page_first_element = (By.XPATH, '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[2]/div[2]/div/div[1]/a')
    _page_navigation = (By.XPATH, '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[2]/div[2]/ul')

    def __init__(self, driver: WebDriver, link: str):
        self.driver = driver
        self.link = link
        
    def scrape(self) -> Union[list[OpenDataLabInfo], Exception]:
        date_crawl = str(datetime.today().date())
        try:
            infos = self.get_infos()
        except Exception as e:
            # TODO Improve the exception handling here
            # logger.exception(f"OpenDataLabPage(link={self.link})::scrape")
            error_msg = e
            return error_msg
        res = []
        
        for info in infos:
            res.append(OpenDataLabInfo(
                date_crawl, info[0], {
                    "downloads": info[1],
                    "likes": info[2],
                }
            ))
        
        return res
        
    def _get_total_pages(self) -> int:
        total_page = 1
        try:
            navigation = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self._page_navigation)
            )
            total_page = navigation.find_element(By.XPATH, './li[last()-2]').get_attribute('title')
        except Exception:
            pass
        return str2int(total_page)
    
    def _get_total_count(self) -> int:
        total_count = 0
        try:
            total_count_text = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self._total_count)
            ).text
            m = re.search(r'([\d,]+)\s*数据集' ,total_count_text)
            if m:
                total_count = m.group(1)
            return str2int(total_count)
        except Exception:
            raise
    
    def _get_info_on_current_page(self) -> tuple[list[tuple], Optional[str]]:
        try:
            elem_divs = WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located(self._page_elements)
            )
            first_link = None
            res = []
            
            for div in elem_divs:
                link = div.find_element(By.XPATH, './a').get_attribute('href')
                downloads = div.find_element(By.XPATH, './a/div[2]/div[2]/span[last()]').text
                likes = div.find_element(By.XPATH, './a/div[1]/div[2]/div[1]/div/span').text
                if first_link is None:
                    first_link = link
                res.append((link, downloads, likes))
            return res, first_link
        except Exception:
            raise
        
    def _next_page(self, page: int, total_page: int, old_link: str) -> None:
        if page >= total_page:
            return
        try:
            navigation = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self._page_navigation)
            )
            next_button = navigation.find_element(By.XPATH, './li[last()-1]')
            next_button.click()
            WebDriverWait(self.driver, 10).until(
                lambda d: d.find_element(*self._page_first_element)
                .get_attribute('href') != old_link
            )
        except Exception:
            raise
        
    def get_infos(self) -> list[tuple]:
        res = []
        self.driver.get(self.link)
        try:
            for part in self._main_parts:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located(part)
                )
        except Exception:
            raise
        
        try:
            total_count = self._get_total_count()
            if total_count > 12:
                total_pages = self._get_total_pages()
            else:
                total_pages = 1
            
            for page in range(1, total_pages + 1):
                infos, old_link = self._get_info_on_current_page()
                res.extend(infos)
                self._next_page(page, total_pages, old_link)
            assert len(res) == total_count
        except AssertionError as e:
            logger.exception(f"depulicated links at {self.link}")
            raise RuntimeError(f"depulicated links at {self.link}") from e
        except Exception:
            raise
        
        return res
