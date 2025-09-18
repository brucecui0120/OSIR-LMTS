import re
from pathlib import Path
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.remote.webdriver import WebDriver
from datetime import datetime
from typing import Literal, Optional
from dataclasses import dataclass, field
from .utils import str2int


@dataclass
class HFRepoInfo:
    repo: str
    repo_url: str
    category: Literal['datasets', 'models']
    detail_urls: Optional[list[str]] = None
    total_links: Optional[int] = None
    error_msg: Optional[Exception] = None


@dataclass
class HFModelInfo:
    repo: str = field(init=False)
    model_name: str = field(init=False)
    downloads_last_month: Optional[int] = field(init=False, default=None)
    likes: Optional[int] = field(init=False, default=None)
    community: Optional[int] = field(init=False, default=None)
    descendants: Optional[int] = field(init=False, default=None)
    date_crawl: str = field()
    link: str = field()
    img_path: Optional[str] = field(default=None)
    error_msg: Optional[Exception] = field(default=None)
    metadata: Optional[dict] = field(default=None)

    def __post_init__(self):
        self.model_name = self.link.rstrip('/').split("/")[-1]
        self.repo = self.link.rstrip('/').split("/")[-2]
        if self.metadata is not None:
            self.downloads_last_month = str2int(self.metadata["downloads_last_month"])
            self.likes = str2int(self.metadata["likes"])
            self.community = str2int(self.metadata["community"])
            self.descendants = sum(str2int(x) for x in self.metadata["tree"])


@dataclass
class HFDatasetInfo:
    repo: str = field(init=False)
    dataset_name: str = field(init=False)
    downloads_last_month: Optional[int] = field(init=False, default=None)
    likes: Optional[int] = field(init=False, default=None)
    community: Optional[int] = field(init=False, default=None)
    dataset_usage: Optional[int] = field(init=False, default=None)
    date_crawl: str = field()
    link: str = field()
    img_path: Optional[str] = field(default=None)
    error_msg: Optional[Exception] = field(default=None)
    metadata: Optional[dict] = field(default=None)

    def __post_init__(self):
        self.dataset_name = self.link.rstrip('/').split("/")[-1]
        self.repo = self.link.rstrip('/').split("/")[-2]
        if self.metadata is not None:
            self.downloads_last_month = str2int(self.metadata["downloads_last_month"])
            self.likes = str2int(self.metadata["likes"])
            self.community = str2int(self.metadata["community"])
            self.dataset_usage = self.metadata["dataset_usage"]


class HFRepoPage(object):
    _model_expand_button = (By.XPATH, '//*[@id="models"]/div/div[2]/div/a')
    _page_navigation_bar = (
        By.XPATH,
        "/html/body/div/main/div/div/section[2]/div/div/nav/ul",
    )
    _expand_articles = (
        By.XPATH,
        "/html/body/div/main/div/div/section[2]/div/div/div/article",
    )
    _page_first_element = (
        By.XPATH,
        "/html/body/div/main/div/div/section[2]/div/div/div/article[1]/a",
    )
    _model_woexpand_articles = (By.XPATH, '//*[@id="models"]/div/div/article')
    _dataset_expand_button = (By.XPATH, '//*[@id="datasets"]/div/div[2]/div/a')
    _dataset_woexpand_articles = (By.XPATH, '//*[@id="datasets"]/div/div/article')
    _total_models = (By.XPATH, '//*[@id="models"]/h3/a/span[2]')
    _total_datasets = (By.XPATH, '//*[@id="datasets"]/h3/a/span[2]')

    def __init__(self, driver: WebDriver, link: str):
        self.driver = driver
        self.link = link

    def scrape(
        self, category: Literal["models", "datasets"]
    ) -> HFRepoInfo:
        repo = self.link.rstrip("/").split("/")[-1]
        assert category in ["models", "datasets"]
        
        try:
            detail_urls = self.get_links(category)
            total_links = len(detail_urls)
            assert len(detail_urls) == len(set(detail_urls))
            error_msg = None
        except Exception as e:
            error_msg = e
            detail_urls = None
            total_links = None
            # logger.exception(f"HFRepoPage(link={self.link})::scrape")

        return HFRepoInfo(
            repo=repo,
            repo_url=self.link,
            category=category,
            detail_urls=detail_urls,
            total_links=total_links,
            error_msg=error_msg
        )

    def _expand_all(self, category: Literal["models", "datasets"]) -> bool:
        if category == "models":
            button_xpath = self._model_expand_button
        else:
            button_xpath = self._dataset_expand_button
        try:
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(button_xpath)
            ).click()
            return True
        except TimeoutException:
            return False
        except Exception:
            raise

    def _get_total_count(self, category: Literal["models", "datasets"]) -> int:
        if category == "models":
            count_xpath = self._total_models
        else:
            count_xpath = self._total_datasets
        try:
            count_text = (
                WebDriverWait(self.driver, 5)
                .until(EC.presence_of_element_located(count_xpath))
                .text
            )
            return str2int(count_text)
        except Exception:
            raise

    def _get_total_pages(self) -> int:
        total_page = 1
        try:
            bar = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self._page_navigation_bar)
            )
            total_page = bar.find_element(By.XPATH, "./li[last()-1]").text
            total_page = str2int(total_page)
            return total_page
        except Exception:
            return 1

    def _get_links_on_current_page(
        self, category: Literal["models", "datasets"]
    ) -> tuple[list[str], Optional[str]]:
        if self.expand:
            try:
                articles = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_all_elements_located(self._expand_articles)
                )
            except Exception:
                raise
        else:
            if category == "models":
                articles_xpath = self._model_woexpand_articles
            else:
                articles_xpath = self._dataset_woexpand_articles
            try:
                articles = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_all_elements_located(articles_xpath)
                )
            except TimeoutException:
                return [], None
            except Exception:
                raise 

        res = []
        first_link = None
        try:
            for article in articles:
                curr_link = article.find_element(By.TAG_NAME, "a").get_attribute("href")
                if first_link is None:
                    first_link = curr_link
                res.append(curr_link)
        except Exception:
            raise

        return res, first_link

    def _next_page(self, page: int, total_page: int, old_link: str | None) -> None:
        if page >= total_page:
            return
        try:
            bar = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self._page_navigation_bar)
            )
            bar.find_element(By.XPATH, "./li[last()]").click()
            WebDriverWait(self.driver, 10).until(
                lambda d: d.find_element(*self._page_first_element)
                .get_attribute("href") != old_link
            )
        except Exception:
            raise

    def get_links(self, category: Literal["models", "datasets"]) -> list[str]:
        res = []
        self.driver.get(self.link)
        total_count = self._get_total_count(category)
        self.expand = self._expand_all(category)

        if self.expand:
            try:
                total_page = self._get_total_pages()
                for page in range(1, total_page + 1):
                    links, old_elem = self._get_links_on_current_page(category)
                    res.extend(links)
                    self._next_page(page, total_page, old_elem)
            except Exception:
                raise
        else:
            try:
                res, _ = self._get_links_on_current_page(category)
            except Exception:
                raise

        assert len(res) == total_count
        return res


class HFModelPage(object):
    _main_part = (By.XPATH, "/html/body/div[1]/main/div[2]/section[2]")
    _downloads_last_month = (
        By.XPATH,
        "/html/body/div/main/div[2]/section[2]/div[1]/dl/dd",
    )
    _downloads_last_month_optional = (
        By.XPATH,
        "/html/body/div/main/div[2]/section[2]/dl/div/dd[1]/div/p",
    )
    _likes = (By.XPATH, "/html/body/div/main/div[1]/header/div/h1/div[3]/button[2]")
    _model_tree = (By.XPATH, "/html/body/div/main/div[2]/section[2]")
    _community_navigation = (
        By.XPATH,
        "/html/body/div/main/div[1]/header/div/div[2]/div[1]",
    )

    def __init__(
        self, driver: WebDriver, link: str, screenshot_path: Optional[str] = None
    ):
        self.driver = driver
        self.link = link
        self.screenshot_path = screenshot_path

    def scrape(self) -> HFModelInfo:
        date_crawl = str(datetime.today().date())
        try:
            metadata = self.get_model_info()
            info = HFModelInfo(
                date_crawl, self.link, self.screenshot_path, metadata=metadata
            )
        except Exception as e:
            error_msg = e
            # logger.exception(f"HFModelPage(link={self.link}, screenshot_path={self.screenshot_path})::scrape")
            info = HFModelInfo(
                date_crawl, self.link, self.screenshot_path, error_msg
            )
        return info

    def get_model_info(self) -> Optional[dict]:
        self.driver.get(self.link)
        try:
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self._main_part)
            )
        except Exception:
            raise

        metadata = {}
        try:
            metadata["downloads_last_month"] = self._get_downloads_last_month()
            metadata["likes"] = self._get_likes()
            metadata["tree"] = self._get_model_tree_leaves()
            metadata["community"] = self._get_community()

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

    def _get_downloads_last_month(self) -> str:
        try:
            downloads_last_month = (
                WebDriverWait(self.driver, 5)
                .until(EC.presence_of_element_located(self._downloads_last_month))
                .text
            )
        except Exception:
            try:
                downloads_last_month = (
                    WebDriverWait(self.driver, 5)
                    .until(
                        EC.presence_of_element_located(
                            self._downloads_last_month_optional
                        )
                    )
                    .text
                )
            except Exception:
                raise

        return downloads_last_month

    def _get_likes(self) -> str:
        try:
            likes = (
                WebDriverWait(self.driver, 5)
                .until(EC.presence_of_element_located(self._likes))
                .text
            )
        except Exception:
            raise

        return likes

    def _get_model_tree_leaves(self) -> list[str]:
        try:
            model_tree = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self._model_tree)
            )
            divs = model_tree.find_elements(By.TAG_NAME, "div")

            total = []
            for div in divs:
                matches = re.findall(r"(\d+)\s+models?", div.text)
                if matches:
                    for m in matches:
                        total.append(m)
                    break
            return total

        except Exception:
            raise

    def _get_community(self):
        try:
            navigation = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self._community_navigation)
            )
            tabs = navigation.find_elements(By.TAG_NAME, "a")
            m = None
            for tab in tabs:
                if "Community" in tab.text:
                    m = re.search(r"\d+", tab.text)
            if m:
                return m.group(0)
            return "0"
        except Exception:
            raise


class HFDatasetPage:
    _main_part = (By.XPATH, "/html/body/div/main/div[2]/section[2]")
    _downloads_last_month = (By.XPATH, "/html/body/div/main/div[2]/section[2]/dl/dd")
    _likes = (By.XPATH, "/html/body/div/main/div[1]/header/div/h1/div[3]/button[2]")
    _community = (
        By.XPATH,
        "/html/body/div/main/div[1]/header/div/div[2]/div/a[last()]",
    )
    _dataset_usage = (By.CSS_SELECTOR, "div.space-y-3")

    def __init__(
        self, driver: WebDriver, link: str, screenshot_path: Optional[str] = None
    ):
        self.driver = driver
        self.link = link
        self.screenshot_path = screenshot_path

    def scrape(self) -> HFDatasetInfo:
        date_crawl = str(datetime.today().date())
        try:
            metadata = self.get_dataset_info()
            info = HFDatasetInfo(
                date_crawl, self.link, self.screenshot_path, metadata=metadata
            )
        except Exception as e:
            error_msg = e
            # logger.exception(f"HFDatasetPage(link={self.link}, screenshot_path={self.screenshot_path})")
            info = HFDatasetInfo(date_crawl, self.link, self.screenshot_path, error_msg)
        return info

    def get_dataset_info(self) -> Optional[dict]:
        self.driver.get(self.link)
        try:
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self._main_part)
            )
        except Exception:
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div/main/div[2]/section/div/a'))
                ).click()
            except Exception:
                raise

        metadata = {}
        try:
            metadata["downloads_last_month"] = self._get_downloads_last_month()
            metadata["likes"] = self._get_likes()
            metadata["community"] = self._get_community()
            metadata["dataset_usage"] = self._get_dataset_usage()

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

    def _get_downloads_last_month(self) -> str:
        try:
            downloads_last_month = (
                WebDriverWait(self.driver, 5)
                .until(EC.presence_of_element_located(self._downloads_last_month))
                .text
            )
        except Exception:
            raise

        return downloads_last_month

    def _get_likes(self) -> str:
        try:
            likes = (
                WebDriverWait(self.driver, 5)
                .until(EC.presence_of_element_located(self._likes))
                .text
            )
        except Exception:
            raise

        return likes

    def _get_dataset_usage(self) -> int:
        try:
            main_part = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self._main_part)
            )
            dataset_usage_div = main_part.find_element(*self._dataset_usage)
            try:
                expand_text = dataset_usage_div.find_element(By.XPATH, "./a").text
                m = re.search(r"(\d+)\s+models?", expand_text)
                if m:
                    return str2int(m.group(1))
                else:
                    raise RuntimeError("Error when parse integer in dataset usage")
            except NoSuchElementException:
                divs = dataset_usage_div.find_elements(By.XPATH, "./div")
                return len(divs)
        except NoSuchElementException:
            return 0
        except Exception:
            raise

    def _get_community(self) -> str:
        try:
            community = (
                WebDriverWait(self.driver, 5)
                .until(EC.presence_of_element_located(self._community))
                .text
            )
            m = re.search(r"\d+", community)
            if m:
                return m.group()
            else:
                return "0"
        except Exception:
            raise
