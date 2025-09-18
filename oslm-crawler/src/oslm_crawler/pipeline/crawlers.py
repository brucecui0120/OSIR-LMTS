import os
import traceback
from time import sleep
from typing import Literal
from dataclasses import asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger
from .base import PipelineStep, PipelineResult, PipelineData
from ..crawler.huggingface import HFRepoPage, HFRepoInfo
from ..crawler.huggingface import HFDatasetPage, HFDatasetInfo
from ..crawler.huggingface import HFModelPage, HFModelInfo
from ..crawler.modelscope import MSRepoPage, MSRepoInfo
from ..crawler.modelscope import MSDatasetPage, MSDatasetInfo
from ..crawler.modelscope import MSModelPage, MSModelInfo
from ..crawler.open_data_lab import OpenDataLabPage, OpenDataLabInfo
from ..crawler.baai_data import BAAIDataPage
from ..crawler.utils import WebDriverPool


class HFRepoPageCrawler(PipelineStep):
    
    ptype = "🐞 CRAWLER"
    required_keys = ['HuggingFace', 'target_sources']
    
    def __init__(
        self, 
        category: Literal['datasets', 'models'] | None = None,
        threads: int = 1,
        max_retries: int =20,
    ):
        self.category = category
        self.threads = threads
        self.max_retries = max_retries
        
    def parse_input(self, input_data: PipelineData | None = None):
        self.data = input_data.data.copy()
        self.input = {"link-category": []}
        required_data = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            required_data[k] = self.data.pop(k)
        for src in required_data['target_sources']:
            self.data.pop(src, None)

        if self.category is None:
            self.input['link-category'].extend([
                (link, "models") for link in required_data['HuggingFace']
            ] + [
                (link, "datasets") for link in required_data['HuggingFace']
            ])
        elif self.category == 'datasets':
            self.input['link-category'].extend([
                (link, 'datasets') for link in required_data['HuggingFace']
            ])
        else:
            self.input['link-category'].extend([
                (link, 'models') for link in required_data['HuggingFace']
            ])
        
    def run(self) -> PipelineResult:
        with ThreadPoolExecutor(self.threads) as executor, WebDriverPool(self.threads) as p:
            task_retries = {lc: 0 for lc in self.input['link-category']}
            completed_tasks = set()
            retry_tasks: list[tuple[str, str]] = list()
            futures = {
                executor.submit(HFRepoPageCrawler._scrape, self, lc[0], lc[1], p): lc
                for lc in self.input['link-category']}
            
            while futures or retry_tasks:
                for lc in retry_tasks:
                    sleep(5)
                    futures.update({
                        executor.submit(HFRepoPageCrawler._scrape, self, lc[0], lc[1], p): lc
                    })
                    task_retries[lc] += 1
                retry_tasks.clear()
                    
                for future in as_completed(futures):
                    lc = futures[future]
                    info = future.result()
                    if info.error_msg is None:
                        data = {
                            "category": info.category,
                            "detail_urls": info.detail_urls
                        }
                        msg = {
                            "repo": info.repo,
                            "repo_url": info.repo_url,
                            "category": info.category,
                            "total_links": info.total_links
                        }
                        data.update(self.data)
                        yield PipelineData(data, msg, None)
                    else:
                        if task_retries[lc] < self.max_retries:
                            retry_tasks.append(lc)
                        else:
                            logger.opt(exception=info.error_msg).error(f"HFRepoPage Error with repo_link: {lc[0]} and category: {lc[1]}")
                            e = info.error_msg
                            error_msg = "".join(
                                traceback.format_exception(type(e), e, e.__traceback__)
                            )
                            yield PipelineData(None, None, {
                                "repo_link": lc[0],
                                "category": lc[1],
                                "error_msg": error_msg,
                            })
                            
                    completed_tasks.add(future)
                
                futures = {f: tp for f, tp in futures.items() if f not in completed_tasks}
                completed_tasks.clear()
        
    def _scrape(
        self, 
        repo_link: str, 
        category: Literal['datasets', 'models'],
        driver_pool: WebDriverPool
    ) -> HFRepoInfo:
        with driver_pool.get_driver() as driver:
            page = HFRepoPage(driver, repo_link)
            info = page.scrape(category)
        return info
    
    
class HFDetailPageCrawler(PipelineStep):
    
    ptype = "🐞 CRAWLER"
    required_keys = ['category', 'detail_urls']
    
    def __init__(
        self,
        threads: int = 1,
        max_retries: int = 10,
        screenshot_path: str | None = None,
    ):
        self.threads = threads
        self.max_retries = max_retries
        self.screenshot_path = screenshot_path
        if self.screenshot_path:
            os.makedirs(self.screenshot_path, exist_ok=True)
        
    def parse_input(self, input_data: PipelineData | None = None):
        self.data = input_data.data.copy()
        required_data = {}
        self.input = {"link-category": []}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            required_data[k] = self.data.pop(k)
        
        self.input['link-category'].extend(
            [(link, required_data['category']) for link in required_data['detail_urls']]
        )
        
    def run(self) -> PipelineResult:
        with ThreadPoolExecutor(self.threads) as executor, WebDriverPool(self.threads) as p:
            task_retries = {lc: 0 for lc in self.input['link-category']}
            completed_tasks = set()
            retry_tasks: list[tuple[str, str]] = list()
            futures = {
                executor.submit(HFDetailPageCrawler._scrape, self, lc[0], lc[1], p): lc
                for lc in self.input['link-category']}
            
            while futures or retry_tasks:
                for lc in retry_tasks:
                    sleep(5)
                    futures.update({
                        executor.submit(HFDetailPageCrawler._scrape, self, lc[0], lc[1], p): lc
                    })
                    task_retries[lc] += 1
                retry_tasks.clear()
                    
                for future in as_completed(futures):
                    lc = futures[future]
                    info = future.result()
                    if info.error_msg is None:
                        data = asdict(info)
                        msg = data.copy()
                        msg.pop('metadata')
                        data.update(self.data)
                        yield PipelineData(data, msg, None)
                    else:
                        if task_retries[lc] < self.max_retries:
                            retry_tasks.append(lc)
                        else:
                            logger.opt(exception=info.error_msg).error(f"HFDetailPage Error with detail_link: {lc[0]} and category: {lc[1]}")
                            e = info.error_msg
                            error_msg = "".join(
                                traceback.format_exception(type(e), e, e.__traceback__)
                            )
                            yield PipelineData(None, None, {
                                "detail_link": lc[0],
                                "category": lc[1],
                                "error_msg": error_msg,
                            })
                    
                    completed_tasks.add(future)
                    
                futures = {f: tp for f, tp in futures.items() if f not in completed_tasks}
                completed_tasks.clear()
    
    def _scrape(
        self,
        detail_link: str,
        category: Literal['datasets', 'models'],
        driver_pool: WebDriverPool
    ) -> HFModelInfo | HFDatasetInfo:
        with driver_pool.get_driver() as driver:
            if category == 'datasets':
                page = HFDatasetPage(driver, detail_link, self.screenshot_path)
            else:
                page = HFModelPage(driver, detail_link, self.screenshot_path)
            info = page.scrape()
        return info
    

class MSRepoPageCrawler(PipelineStep):
    
    ptype = "🐞 CRAWLER"
    required_keys = ['ModelScope', 'target_sources']
    
    def __init__(
        self, 
        category: Literal['datasets', 'models'] | None = None,
        threads: int = 1,
        max_retries: int = 10,
    ):
        self.category = category
        self.threads = threads
        self.max_retries = max_retries
        
    def parse_input(self, input_data: PipelineData | None = None):
        self.data = input_data.data.copy()
        self.input = {"link-category": []}
        required_data = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            required_data[k] = self.data.pop(k)
        for src in required_data['target_sources']:
            self.data.pop(src, None)
            
        if self.category is None:
            self.input['link-category'].extend([
                (link, 'models') for link in required_data['ModelScope']
            ] + [
                (link, 'datasets') for link in required_data['ModelScope']
            ])
        elif self.category == 'datasets':
            self.input['link-category'].extend([
                (link, 'datasets') for link in required_data['ModelScope']
            ])
        else:
            self.input['link-category'].extend([
                (link, 'models') for link in required_data['ModelScope']
            ])
        
    def run(self) -> PipelineResult:
        with ThreadPoolExecutor(self.threads) as executor, WebDriverPool(self.threads) as p:
            task_retries = {lc: 0 for lc in self.input['link-category']}
            completed_tasks = set()
            retry_tasks: list[tuple[str, str]] = list()
            futures = {
                executor.submit(MSRepoPageCrawler._scrape, self, lc[0], lc[1], p): lc
                for lc in self.input['link-category']}
            
            while futures or retry_tasks:
                for lc in retry_tasks:
                    sleep(5)
                    futures.update({
                        executor.submit(HFRepoPageCrawler._scrape, self, lc[0], lc[1], p): lc
                    })
                    task_retries[lc] += 1
                retry_tasks.clear()
                
                for future in as_completed(futures):
                    lc = futures[future]
                    info = future.result()
                    if info.error_msg is None:
                        data = {
                            "category": info.category,
                            "detail_urls": info.detail_urls
                        }
                        msg = {
                            "repo": info.repo,
                            "repo_url": info.repo_url,
                            "category": info.category,
                            "total_links": info.total_links
                        }
                        data.update(self.data)
                        yield PipelineData(data, msg, None)
                    else:
                        if task_retries[lc] < self.max_retries:
                            retry_tasks.append(lc)
                        else:
                            logger.opt(exception=info.error_msg).error(f"MSRepoPage Error with repo_link: {lc[0]} and category: {lc[1]}")
                            e = info.error_msg
                            error_msg = "".join(
                                traceback.format_exception(type(e), e, e.__traceback__)
                            )
                            yield PipelineData(None, None, {
                                "repo_link": lc[0],
                                "category": lc[1],
                                "error_msg": error_msg,
                            })
                    
                    completed_tasks.add(future)
                    
                futures = {f: lc for f, lc in futures.items() if f not in completed_tasks}
                completed_tasks.clear()
            
    def _scrape(
        self,
        repo_link: str,
        category: Literal['datasets', 'models'],
        driver_pool: WebDriverPool
    ) -> MSRepoInfo:
        with driver_pool.get_driver() as driver:
            page = MSRepoPage(driver, repo_link)
            info = page.scrape(category)
        return info
    
    
class MSDetailPageCrawler(PipelineStep):
    
    ptype = "🐞 CRAWLER"
    required_keys = ['category', 'detail_urls']
    
    def __init__(
        self, 
        threads: int = 1,
        max_retries: int = 10,
        screenshot_path: str | None = None,
    ):
        self.threads = threads
        self.max_retries = max_retries
        self.screenshot_path = screenshot_path
        if self.screenshot_path:
            os.makedirs(self.screenshot_path, exist_ok=True)
        
    def parse_input(self, input_data: PipelineData | None = None):
        self.data = input_data.data.copy()
        required_data = {}
        self.input = {"link-category": []}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not fount in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            required_data[k] = self.data.pop(k)
        
        self.input['link-category'].extend(
            [(link, required_data['category']) for link in required_data['detail_urls']]
        )
        
    def run(self) -> PipelineResult:
        with ThreadPoolExecutor(self.threads) as executor, WebDriverPool(self.threads) as p:
            task_retries = {lc: 0 for lc in self.input['link-category']}
            completed_tasks = set()
            retry_tasks: list[tuple[str, str]] = list()
            futures = {
                executor.submit(MSDetailPageCrawler._scrape, self, lc[0], lc[1], p): lc 
                for lc in self.input['link-category']}
            
            while futures or retry_tasks:
                for lc in retry_tasks:
                    sleep(5)
                    futures.update({
                        executor.submit(MSDetailPageCrawler._scrape, self, lc[0], lc[1], p): lc
                    })
                    task_retries[lc] += 1
                retry_tasks.clear()
                
                for future in as_completed(futures):
                    lc = futures[future]
                    info = future.result()
                    if info.error_msg is None:
                        data = asdict(info)
                        msg = data.copy()
                        msg.pop('metadata')
                        data.update(self.data)
                        yield PipelineData(data, msg, None)
                    else:
                        if task_retries[lc] < self.max_retries:
                            retry_tasks.append(lc)
                        else:
                            logger.opt(exception=info.error_msg).error(f"MSDetailPage Error with detail_link: {lc[0]} and category: {lc[1]}")
                            e = info.error_msg
                            error_msg = "".join(
                                traceback.format_exception(type(e), e, e.__traceback__)
                            )
                            yield PipelineData(None, None, {
                                "detail_link": lc[0],
                                "category": lc[1],
                                "error_msg": error_msg,
                            })
                    
                    completed_tasks.add(future)
                
                futures = {f: lc for f, lc in futures.items() if f not in completed_tasks}
                completed_tasks.clear()
            
    def _scrape(
        self,
        detail_link: str,
        category: Literal['datasets', 'models'],
        driver_pool: WebDriverPool
    ) -> MSModelInfo | MSDatasetInfo:
        with driver_pool.get_driver() as driver:
            if category == "datasets":
                page = MSDatasetPage(driver, detail_link, self.screenshot_path)
            else: 
                page = MSModelPage(driver, detail_link, self.screenshot_path)
            info = page.scrape()
        return info
        

class OpenDataLabCrawler(PipelineStep):
    
    ptype = "🐞 CRAWLER"
    required_keys = ["OpenDataLab", "target_sources"]
    
    def __init__(
        self,
        threads: int = 1,
        max_retries: int = 10,
    ):
        self.threads = threads
        self.max_retries = max_retries
        
    def parse_input(self, input_data: PipelineData | None = None):
        self.data = input_data.data.copy()
        self.data.pop("repo_org_mapper", None)
        self.input = {"links": []}
        required_data = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            required_data[k] = self.data.pop(k)
        for src in required_data['target_sources']:
            self.data.pop(src, None)
            
        self.input['links'].extend(required_data['OpenDataLab'])
    
    def run(self) -> PipelineResult:
        with ThreadPoolExecutor(self.threads) as executor, WebDriverPool(self.threads) as p:
            task_retries = {link: 0 for link in self.input['links']}
            completed_tasks = set()
            retry_tasks: list[str] = list()
            futures = {
                executor.submit(OpenDataLabCrawler._scrape, self, link, p): link
                for link in self.input['links']}
            
            while futures or retry_tasks:
                for link in retry_tasks:
                    sleep(5)
                    futures.update({
                        executor.submit(OpenDataLabCrawler._scrape, self, link, p)
                    })
                    task_retries[link] += 1
                retry_tasks.clear()
                
                for future in as_completed(futures):
                    link = futures[future]
                    infos = future.result()
                    if not isinstance(infos, list):
                        if task_retries[link] < self.max_retries:
                            retry_tasks.append(link)
                        else:
                            logger.opt(exception=infos).error(f"OpenDataLab error with link={link}")
                            e = infos
                            error_msg = "".join(
                                traceback.format_exception(type(e), e, e.__traceback__)
                            )
                            yield PipelineData(None, None, {
                                "link": link,
                                "error_msg": error_msg,
                            })
                    else:
                        for info in infos:
                            data = asdict(info)
                            msg = data.copy()
                            msg.pop('metadata')
                            data.update(self.data)
                            yield PipelineData(data, msg, None)
                    
                    completed_tasks.add(future)
                
                futures = {f: link for f, link in futures.items() if f not in completed_tasks}
                completed_tasks.clear()
    
    def _scrape(
        self,
        link: str,
        driver_pool: WebDriverPool
    ) -> list[OpenDataLabInfo] | str:
        with driver_pool.get_driver() as driver:
            page = OpenDataLabPage(driver, link)
            infos = page.scrape()
        return infos
        

class BAAIDatasetsCrawler(PipelineStep):
    
    ptype = "🐞 CRAWLER"
    required_keys = ['BAAI Data', 'target_sources']
    
    def __init__(self, max_retries: int = 10):
        self.max_retries = max_retries

    def parse_input(self, input_data: PipelineData | None = None):
        self.data = input_data.data.copy()
        self.data.pop("repo_org_mapper", None)
        required_data = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            required_data[k] = self.data.pop(k)
        for src in required_data['target_sources']:
            self.data.pop(src, None)
        self.input = required_data['BAAI Data']
        
    def run(self) -> PipelineResult:
        for i in range(self.max_retries):
            page = BAAIDataPage()
            infos = page.scrape()
            if isinstance(infos, str):
                continue
            for info in infos:
                data = asdict(info)
                msg = data.copy()
                data.update(self.data)
                yield PipelineData(data, msg, None)
            break
        if i == self.max_retries:
            yield PipelineData(None, None, {"error_msg": infos})
                    