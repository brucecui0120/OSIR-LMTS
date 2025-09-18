import re
from pathlib import Path
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.remote.webdriver import WebDriver
from datetime import datetime
from typing import Literal, Optional
from dataclasses import dataclass, field
from .utils import str2int


@dataclass
class MSRepoInfo:
    repo: str
    repo_url: str
    category: Literal['datasets', 'models']
    detail_urls: Optional[list[str]] = None
    total_links: Optional[int] = None
    error_msg: Optional[str] = None
    
    
@dataclass
class MSModelInfo:
    repo: str = field(init=False)
    model_name: str = field(init=False)
    total_downloads: Optional[int] = field(init=False, default=None)
    likes: Optional[int] = field(init=False, default=None)
    community: Optional[int] = field(init=False, default=None)
    date_crawl: str = field(default=None)
    link: str = field(default=None)
    img_path: Optional[str] = field(default=None)
    error_msg: Optional[str] =field(default=None)
    metadata: Optional[dict] = field(default=None)
    
    def __post_init__(self):
        self.model_name = self.link.rstrip('/').split('/')[-1]
        self.repo = self.link.split('/')[-2]
        if self.metadata is not None:
            self.total_downloads = str2int(self.metadata['downloads'])
            self.likes = str2int(self.metadata['likes'])
            self.community = str2int(self.metadata['community'])


@dataclass
class MSDatasetInfo:
    repo: str = field(init=False)
    dataset_name: str = field(init=False)
    total_downloads: Optional[int] = field(init=False, default=None)
    likes: Optional[int] = field(init=False, default=None)
    community: Optional[int] = field(init=False, default=None)
    date_crawl: str = field()
    link: str = field()
    img_path: Optional[str] = field(default=None)
    error_msg: Optional[str] = field(default=None)
    metadata: Optional[dict] = field(default=None)
    
    def __post_init__(self):
        self.dataset_name = self.link.rstrip('/').split('/')[-1]
        self.repo = self.link.split('/')[-2]
        if self.metadata is not None:
            self.total_downloads = str2int(self.metadata['downloads'])
            self.likes = str2int(self.metadata['likes'])
            self.community = str2int(self.metadata['community'])


class MSRepoPage:

    _model_tab = (By.XPATH, '//*[@id="organization_rightContent"]/div/div/div[1]/div/div[3]')
    _page_navigation_bar = (By.XPATH, '//*[@id="organization_rightContent"]/div/div[3]/div/div[3]/div/div/div/div[2]/ul')
    _page_elem_divs = (By.XPATH, '//*[@id="organization_rightContent"]/div/div[3]/div/div[3]/div/div/div/div[1]/div')
    _page_first_element = (By.XPATH, '//*[@id="organization_rightContent"]/div/div[3]/div/div[3]/div/div/div/div[1]/div[1]/a')
    _dataset_tab = (By.XPATH, '//*[@id="organization_rightContent"]/div/div[1]/div/div/div[4]')

    def __init__(self, driver: WebDriver, link: str):
        self.driver = driver
        self.link = link
        
    def scrape(self, category: Literal["models", "datasets"]) -> MSRepoInfo:
        repo = self.link.rstrip('/').split()[-1]
        assert category in ['models', 'datasets']
        
        try:
            detail_urls = self.get_links(category)
            total_links = len(detail_urls)
            assert len(detail_urls) == len(set(detail_urls))
            error_msg = None
        except Exception as e:
            error_msg = e
            detail_urls = None
            total_links = None
            # logger.exception(f"MSRepoPage(link={self.link})::scrape")
            
        return MSRepoInfo(
            repo=repo,
            repo_url=self.link,
            category=category,
            detail_urls=detail_urls,
            total_links=total_links,
            error_msg=error_msg
        )

    def _click_tab(self, category: Literal["models", "datasets"]) -> None:
        if category == "models":
            tab_xpath = self._model_tab
        else:
            tab_xpath = self._dataset_tab
        try:
            tab = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(tab_xpath)
            )
            tab.click()
        except Exception:
            raise
        
    def _get_total_pages(self) -> int:
        total_page = 1
        try:
            navigation = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self._page_navigation_bar)
            )
            total_page = navigation.find_element(By.XPATH, './li[last()-2]').get_attribute('title')
        except Exception:
            pass
        return str2int(total_page)
    
    def _get_total_count(self, category: Literal["models", "datasets"]) -> int:
        if category == "models":
            tab_xpath = self._model_tab
        else:
            tab_xpath = self._dataset_tab
        try:
            tab = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(tab_xpath)
            )
            count_text = tab.find_element(By.XPATH, './span[2]').text
            return str2int(count_text)
        except NoSuchElementException:
            return 0
        except Exception:
            raise
        
    def _get_links_on_current_page(self, add_likes: bool=False) -> tuple[list[str], Optional[str]]:
        try: 
            elem_divs = WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located(self._page_elem_divs)
            )
            first_link = None
            res = []
            for div in elem_divs:
                try:
                    curr_link = div.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    # TODO wait until modelscope dataset page shows number of likes
                    if add_likes:
                        try:
                            likes = div.find_element(By.XPATH, './a/div/div[3]/div/div[3]/div[3]')
                            curr_link = curr_link.rstrip('/') + f'/{likes.text}'
                        except NoSuchElementException as e:
                            raise RuntimeError(f"likes not found for {curr_link} of {self.link}") from e
                except NoSuchElementException:
                    continue
                if first_link is None:
                    first_link = curr_link
                    # TODO wait until modelscope dataset page shows number of likes
                    if add_likes:
                        first_link = '/'.join(first_link.split('/')[:-1])
                res.append(curr_link)
            return res, first_link
        except Exception:
            raise
    
    def _next_page(self, page: int, total_page: int, old_link: str) -> None:
        if page >= total_page:
            return 
        try:
            navigation = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self._page_navigation_bar)
            )
            next_button = navigation.find_element(By.XPATH, './li[last()-1]')
            next_button.click()
            WebDriverWait(self.driver, 10).until(
                lambda d: d.find_element(*self._page_first_element)
                .get_attribute('href') != old_link
            )
        except Exception:
            raise
        
    def get_links(self, category: Literal["models", "datasets"]) -> list[str]:
        res = []
        self.driver.get(self.link)
        
        try:
            total_count = self._get_total_count(category)
            self._click_tab(category)
            total_pages = self._get_total_pages()
            
            for page in range(1, total_pages + 1):
                # TODO wait until modelscope dataset page shows number of likes
                links, old_elem = self._get_links_on_current_page(category == "datasets")
                res.extend(links)
                self._next_page(page, total_pages, old_elem)
            assert len(res) == total_count
        except Exception:
            raise
        
        return res

        
class MSModelPage:
    
    _main_parts = [
        (By.XPATH, '//*[@id="root"]/div/div/main/div[1]/div/div[1]/div[1]/div/div'),
        (By.XPATH, '//*[@id="modelDetail_bottom"]/div/div[1]/div'),
        # (By.XPATH, '//*[@id="modelDetail_bottom"]/div/div[2]/div[1]/div'),
    ]
    _downloads = (By.XPATH, '//*[@id="root"]/div/div/main/div[1]/div/div[1]/div[1]/div/div/div[3]/div[1]')
    _navigation_tabs = (By.XPATH, '//*[@id="root"]/div/div/main/div[1]/div/div[1]/div[2]/div/div/div/div[1]/div[1]/div')
    _likes = (By.XPATH, '//*[@id="root"]/div/div/main/div[1]/div/div[1]/div[1]/div/div/div[1]/div/div/div[3]/div[1]')
    
    def __init__(self, driver: WebDriver, link: str, screenshot_path: Optional[str]=None):
        self.driver = driver
        self.link = link
        self.screenshot_path = screenshot_path
        
    def scrape(self) -> MSModelInfo:
        date_crawl = str(datetime.today().date())
        
        try: 
            metadata = self.get_model_info()
            info = MSModelInfo(
                date_crawl, self.link, self.screenshot_path, metadata=metadata
            )
        except Exception as e:
            error_msg = e
            # logger.exception(f"MSModelPage(link={self.link}, screenshot_path={self.screenshot_path})::scrape")
            info = MSModelInfo(
                date_crawl, self.link, self.screenshot_path, error_msg
            )
        return info
        
    def get_model_info(self) -> Optional[dict]:
        self.driver.get(self.link)
        try:
            for part in self._main_parts:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located(part)
                )
        except Exception:
            raise
        
        metadata = {}
        try:
            metadata['downloads'] = self._get_downloads()
            metadata['likes'] = self._get_likes()
            metadata['community'] = self._get_community()
            
            if self.screenshot_path:
                self.screenshot_path = Path(self.screenshot_path)
                repo_name = self.link.rstrip('/').split('/')[-2]
                model_name = self.link.rstrip('/').split('/')[-1]
                file_name = repo_name + '_' + model_name + '_' + str(datetime.today().date()) + '.png'
                self.driver.save_screenshot(self.screenshot_path / file_name)
                self.screenshot_path = str(self.screenshot_path / file_name)
        except Exception:
            raise
        
        return metadata
        
    def _get_downloads(self) -> str:
        try:
            downloads = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self._downloads)
            ).text
        except Exception:
            raise
        
        m = re.search(r'([\d,]+)下载', downloads)
        if m:
            downloads = m.group(1)
        else:
            downloads = '0'

        return downloads
    
    def _get_community(self) -> str:
        try:
            navigation_tabs = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self._navigation_tabs)
            )
            tabs = navigation_tabs.find_elements(By.XPATH, './div')
        except Exception:
            raise
            
        for tab in tabs:
            m = re.search(r'交流反馈([\d,]+)', tab.text)
            if m:
                return m.group(1)
        return '0'
        
    def _get_likes(self) -> str:
        try:
            likes = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self._likes)
            ).text
        except Exception:
            raise

        m = re.search(r'([\d,]+)', likes)
        if m:
            return m.group(1)
        else: 
            return '0'


class MSDatasetPage:
    
    _main_parts = [
        (By.XPATH, '//*[@id="root"]/div/div/main/div[1]/div[1]/div[1]/div/div'),
        (By.XPATH, '//*[@id="modelDetail_bottom"]/div/div/div'),
        # (By.XPATH, '//*[@id="modelDetail_bottom"]/div/div[1]/div'),
        # (By.XPATH, '//*[@id="modelDetail_bottom"]/div/div[2]/div[1]/div'),
    ]
    _downloads = (By.XPATH, '//*[@id="root"]/div/div/main/div[1]/div[1]/div[1]/div/div/div[3]')
    _navigation_tabs = (By.XPATH, '//*[@id="root"]/div/div/main/div[1]/div[1]/div[2]/div/div/div/div[1]/div[1]/div')
    _likes = (By.XPATH, '') # TODO wait until modelscope dataset page shows number of likes
    
    def __init__(self, driver: WebDriver, link: str, screenshot_path: Optional[str]=None):
        self.driver = driver
        # TODO wait until modelscope dataset page shows number of likes
        self.link = '/'.join(link.split('/')[:-1])
        self.likes = link.split('/')[-1]
        self.screenshot_path = screenshot_path
        
    def scrape(self) -> MSDatasetInfo:
        date_crawl = str(datetime.today().date())
        
        try:
            metadata = self.get_dataset_info()
            info = MSDatasetInfo(
                date_crawl, self.link, self.screenshot_path, metadata=metadata
            )
        except Exception as e:
            error_msg = e
            # logger.exception(f"MSDatasetPage(link={self.link}, screenshot_path={self.screenshot_path})::scrape")
            info = MSDatasetInfo(
                date_crawl, self.link, self.screenshot_path, error_msg
            )
        return info
        
    def get_dataset_info(self) -> Optional[dict]:
        self.driver.get(self.link)
        try:
            for part in self._main_parts:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located(part)
                )
        except Exception:
            raise
        
        metadata = {}
        try:
            metadata['downloads'] = self._get_downloads()
            # TODO wait until modelscope dataset page shows number of likes
            metadata['likes'] = self.likes 
            metadata['community'] = self._get_community()
            
            if self.screenshot_path:
                self.screenshot_path = Path(self.screenshot_path)
                repo_name = self.link.rstrip('/').split('/')[-2]
                dataset_name = self.link.rstrip('/').split('/')[-1]
                file_name = repo_name + '_' + dataset_name + '_' + str(datetime.today().date()) + '.png'
                self.driver.save_screenshot(self.screenshot_path / file_name)
                self.screenshot_path = str(self.screenshot_path / file_name)
        except Exception:
            raise
        
        return metadata
        
    def _get_downloads(self) -> str:
        try:
            downloads = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self._downloads)
            ).text
        except Exception:
            raise
        
        m = re.search(r'([\d,]+)下载', downloads)
        if m:
            downloads = m.group(1)
        else:
            downloads = '0'
        
        return downloads

    def _get_community(self) -> str:
        try:
            navigation_tabs = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self._navigation_tabs)
            )
            tabs = navigation_tabs.find_elements(By.XPATH, './div')
        except Exception:
            raise
        
        for tab in tabs:
            m = re.search(r'交流反馈([\d,]+)', tab.text)
            if m:
                return m.group(1)
        return '0'

    def _get_likes(self) -> str:
        # TODO wait until modelscope dataset page shows number of likes
        raise NotImplementedError
