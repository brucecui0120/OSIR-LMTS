import re
import jsonlines
import sys
import pandas as pd
import numpy as np
from collections import defaultdict
from typing import Literal
from loguru import logger
from oslm_crawler.pipeline.base import PipelineData
from oslm_crawler.pipeline.processors import HFInfoProcessor
from oslm_crawler.pipeline.processors import MSInfoProcessor
from oslm_crawler.pipeline.processors import OpenDataLabInfoProcessor
from oslm_crawler.pipeline.processors import BAAIDataInfoProcessor
from tqdm import tqdm
from pathlib import Path
from .pipeline.readers import OrgLinksReader, JsonlineReader
from .pipeline.crawlers import HFRepoPageCrawler, MSRepoPageCrawler
from .pipeline.crawlers import HFDetailPageCrawler, MSDetailPageCrawler
from .pipeline.crawlers import OpenDataLabCrawler, BAAIDatasetsCrawler
from .pipeline.writers import ModelDatasetJsonlineWriter, JsonlineWriter
from datetime import datetime, timedelta


class HFPipeline:
    
    def __init__(
        self, 
        task_name: str,
        load_dir: str | None = None,
        save_dir: str | None = None,
        log_path: str | None = None,
    ):
        self.task_name = task_name
        self.crawl_date = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        if load_dir:
            self.load_dir = Path(load_dir)
        else:
            self.load_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/HuggingFace'
        if save_dir:
            self.save_dir = Path(save_dir)
            if self.save_dir.name != 'HuggingFace':
                self.save_dir = self.save_dir / 'HuggingFace'
        else:
            self.save_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/HuggingFace'
        if log_path is None:
            log_path = Path(__file__).parents[2] / f'logs/{task_name}-{self.crawl_date}'/'running.log'
        else:
            log_path = Path(log_path)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        self.error_f = log_path.parent
        
    def step_all(
        self,
        save: bool,
    ):
        return self._init_org_links(
            save
        )._crawl_repo_page(
            save        
        )._crawl_detail_page(
            save
        )._post_process(
            save
        )
    
    def step(
        self, 
        stage: Literal[
            'init_org_links',
            'crawl_repo_page',
            'crawl_detail_page',
            'post_process',
        ],
        save: bool,
        **kargs,
    ):
        match stage:
            case 'init_org_links':
                return self._init_org_links(save, **kargs)
            case 'crawl_repo_page':
                return self._crawl_repo_page(save, **kargs)
            case 'crawl_detail_page':
                return self._crawl_detail_page(save, **kargs)
            case 'post_process':
                return self._post_process(save, **kargs)
                
    def done(self):
        logger.success(f"HFPipeline {self.task_name} done.")
    
    def _init_org_links(self, save, **kargs):
        logger.info("Init org links of HuggingFace")
        kargs = {k: v for k, v in kargs.items() if k in ['path', 'orgs']}
        kargs['sources'] = ['HuggingFace']
        reader = OrgLinksReader(**kargs)
        reader.parse_input()
        res = next(reader.run())
        logger.info(f"Target orgs: {res.message['target_orgs']}")
        logger.info(f"Total links: {res.message['total_links']}")
        if save:
            save_path = self.save_dir / "org-links.jsonl"
            writer = JsonlineWriter(save_path)
            writer.parse_input(res)
            res = next(writer.run())
        writer.close()
        self._init_org_links_res = res
        return self
    
    def _crawl_repo_page(self, save, **kargs):
        error_f = self.error_f / 'org-links.jsonl'
        error_f = open(error_f, 'a')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Crawl repo page of HuggingFace")
        if not hasattr(self, "_init_org_links_res"):
            logger.info("Missing the running result of the previous step (init_org_links)")
            logger.info(f"Trying load required data from {self.load_dir}")
            reader = JsonlineReader(self.load_dir/'org-links.jsonl')
            error_list = next(reader.run()).data.get('content')
            self._init_org_links_res = PipelineData({
                "HuggingFace": [err['repo_link'] for err in error_list],
                "target_sources": ["HuggingFace"],
            }, None, None)
        inp = self._init_org_links_res
        kargs = {k: v for k, v in kargs.items() if k in ['category', 'threads', 'max_retries']}
        crawler = HFRepoPageCrawler(**kargs)
        crawler.parse_input(inp)
        count = len(crawler.input['link-category'])
        pbar = tqdm(total=count, desc="Crawling repo infos from HuggingFace...")
        res = []
        if save:
            save_path = self.save_dir / "repo-page.jsonl"
            writer = JsonlineWriter(save_path, drop_keys=['repo_org_mapper'])
        for data in crawler.run():
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                pbar.update(1)
                continue
            pbar.write(f"{data.message['repo']} huggingface has {data.message['total_links']} {data.message['category']}.")
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
            pbar.update(1)
        
        writer.close()
        pbar.close()
        self.error_writer.close()
        error_f.close()
        self._crawl_repo_page_res = res
        return self

    def _crawl_detail_page(self, save, **kargs):
        error_f = self.error_f / 'repo-page.jsonl'
        error_f = open(error_f, 'a')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Crawl detail page of HuggingFace")
        if not hasattr(self, "_crawl_repo_page_res"):
            logger.info("Missing the running result of the previous step (crawl_repo_page)")
            logger.info(f"Trying load required data from {self.load_dir}")
            reader = JsonlineReader(self.load_dir/'repo-page.jsonl')
            error_list = next(reader.run()).data.get('content')
            model_urls = []
            dataset_urls = []
            for err in error_list:
                if err['category'] == 'models':
                    model_urls.append(err['detail_link'])
                elif err['category'] == 'datasets':
                    dataset_urls.append(err['detail_link'])
            self._crawl_repo_page_res = [PipelineData({
                'category': 'models',
                'detail_urls': model_urls
            }, None, None), PipelineData({
                'category': 'datasets',
                'detail_urls': dataset_urls
            }, None, None)]
        inps = self._crawl_repo_page_res
        kargs = {k: v for k, v in kargs.items() if k in ['threads', 'max_retries', 'screenshot_path']}
        crawler = HFDetailPageCrawler(**kargs)
        count = sum(len(inp.data['detail_urls']) for inp in inps if inp.data is not None)
        pbar = tqdm(total=count, desc="Crawling detail infos from HuggingFace...")
        res = []
        if save:
            writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / "raw-models-info.jsonl"),
                str(self.save_dir / "raw-datasets-info.jsonl"),
                ['repo_org_mapper'], ['repo_org_mapper']
            )
        for inp in inps:
            crawler.parse_input(inp)
            for data in crawler.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    pbar.update(1)
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
                pbar.update(1)
        
        writer.close()
        pbar.close()
        self.error_writer.close()
        error_f.close()
        count = defaultdict(int)
        for data in res:
            if 'model_name' in data.data.keys():
                count['models'] += 1
            elif 'dataset_name' in data.data.keys():
                count['datasets'] += 1
        logger.info(f"Crawl detail page done. Total models: {count['models']}. Total datasets: {count['datasets']}")
        self._crawl_detail_page_res = res
        return self
    
    def _post_process(self, save, **kargs):
        error_f = self.error_f / 'post-process-error.jsonl'
        error_f = open(error_f, 'w')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Post Processing of HuggingFace data.")
        if not hasattr(self, "_crawl_detail_page_res"):
            logger.info("Missing the running result of the previous step (crawl_detail_page)")
            logger.info(f"Trying load required data from {self.save_dir}")
            org_links_reader = OrgLinksReader(sources=['HuggingFace'])
            org_links_reader.parse_input()
            models_reader = JsonlineReader(self.save_dir / 'raw-models-info.jsonl')
            datasets_reader = JsonlineReader(self.save_dir / 'raw-datasets-info.jsonl')
            repo_org_mapper = next(org_links_reader.run()).data['repo_org_mapper']
            self._crawl_detail_page_res = next(models_reader.run()).data.get('content')
            self._crawl_detail_page_res.extend(next(datasets_reader.run()).data.get('content'))
            for inp in self._crawl_detail_page_res:
                inp['repo_org_mapper'] = repo_org_mapper
            self._crawl_detail_page_res = [
                PipelineData(inp, None, None) for inp in self._crawl_detail_page_res
            ]
            
        inps = self._crawl_detail_page_res
        kargs = {k: v for k, v in kargs.items() if k in [
            'dataset_info_path', 'model_info_path', 'ai_gen', 'ai_check',
            'buffer_size', 'max_retries'
        ]}
        processor = HFInfoProcessor(**kargs)
        res = []
        if save:
            writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / 'processed-models-info.jsonl'),
                str(self.save_dir / 'processed-datasets-info.jsonl'),
            )
        for inp in inps:
            processor.parse_input(inp)
            for data in processor.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
        for data in processor.flush(update_infos=True):
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                continue
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)

        if kargs.get('ai_check', False):
            model_check = {
                f'{data['repo']}/{data['model_name']}': data['downloads_last_month']
                for data in processor.models_check_buffer
            }
            dataset_check = {
                f'{data['repo']}/{data['dataset_name']}': data['downloads_last_month']
                for data in processor.datasets_check_buffer
            }
            back_writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / "raw-models-info.jsonl"),
                str(self.save_dir / "raw-datasets-info.jsonl"),
                ['repo_org_mapper'], ['repo_org_mapper']
            )
            for inp in self._crawl_detail_page_res:
                if 'model_name' in inp.data.keys():
                    key = f'{inp.data['repo']}/{inp.data['model_name']}'
                    downloads_last_month = model_check.get(key, inp.data['downloads_last_month'])
                    inp.data['downloads_last_month'] = downloads_last_month
                elif 'dataset_name' in inp.data.keys():
                    key = f'{inp.data['repo']}/{inp.data['dataset_name']}'
                    downloads_last_month = dataset_check.get(key, inp.data['downloads_last_month'])
                    inp.data['downloads_last_month'] = downloads_last_month
                back_writer.parse_input(inp)
                next(back_writer.run())
            back_writer.close()
        
        writer.close()
        self.error_writer.close()
        error_f.close()
        count = defaultdict(int)
        for data in res:
            if 'model_name' in data.data.keys():
                count['models'] += 1
            elif 'dataset_name' in data.data.keys():
                count['datasets'] += 1
        logger.info(f"Post process done. Total models: {count['models']}. Total datasets: {count['datasets']}")
        self._post_process_res = res
        return self


class MSPipeline:
    
    def __init__(
        self,
        task_name: str,
        load_dir: str | None = None,
        save_dir: str | None = None,
        log_path: str | None = None,
    ):
        self.task_name = task_name
        self.crawl_date = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        if load_dir:
            self.load_dir = Path(load_dir)
        else:
            self.load_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/ModelScope'
        if save_dir:
            self.save_dir = Path(save_dir)
            if self.save_dir.name != 'ModelScope':
                self.save_dir = self.save_dir / 'ModelScope'
        else:
            self.save_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/ModelScope'
        if log_path is None:
            log_path = Path(__file__).parents[2] / f'logs/{task_name}-{self.crawl_date}'/'running.log'
        else:
            log_path = Path(log_path)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        self.error_f = log_path.parent
        
    def step_all(
        self,
        save: bool,
    ):
        return self._init_org_links(
            save
        )._crawl_repo_page(
            save        
        )._crawl_detail_page(
            save
        )._post_process(
            save
        )
        
    def step(
        self, 
        stage: Literal[
            'init_org_links',
            'crawl_repo_page',
            'crawl_detail_page',
            'post_process',
        ],
        save: bool,
        **kargs,
    ):
        match stage:
            case 'init_org_links':
                return self._init_org_links(save, **kargs)
            case 'crawl_repo_page':
                return self._crawl_repo_page(save, **kargs)
            case 'crawl_detail_page':
                return self._crawl_detail_page(save, **kargs)
            case 'post_process':
                return self._post_process(save, **kargs)
                
    def done(self):
        logger.success(f"MSPipeline {self.task_name} done.")
    
    def _init_org_links(self, save, **kargs):
        logger.info("Init org links of ModelScope")
        kargs = {k: v for k, v in kargs.items() if k in ['path', 'orgs']}
        kargs['sources'] = ['ModelScope']
        reader = OrgLinksReader(**kargs)
        reader.parse_input()
        res = next(reader.run())
        logger.info(f"Target orgs: {res.message['target_orgs']}")
        logger.info(f"Total links: {res.message['total_links']}")
        if save:
            save_path = self.save_dir / "org-links.jsonl"
            writer = JsonlineWriter(save_path)
            writer.parse_input(res)
            res = next(writer.run())
            writer.close()
        self._init_org_links_res = res
        return self
    
    def _crawl_repo_page(self, save, **kargs):
        error_f = self.error_f / 'org-links.jsonl'
        error_f = open(error_f, 'a')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Crawl repo page of ModelScope")
        if not hasattr(self, "_init_org_links_res"):
            logger.info("Missing the running result of the previous step (init_org_links)")
            logger.info(f"Trying load required data from {self.load_dir}")
            reader = JsonlineReader(self.load_dir/'org-links.jsonl')
            error_list = next(reader.run()).data.get('content')
            self._init_org_links_res = PipelineData({
                "ModelScope": [err['repo_link'] for err in error_list],
                "target_sources": ["ModelScope"],
            }, None, None)
        inp = self._init_org_links_res
        kargs = {k: v for k, v in kargs.items() if k in ['category', 'threads', 'max_retries']}
        crawler = MSRepoPageCrawler(**kargs)
        crawler.parse_input(inp)
        count = len(crawler.input['link-category'])
        pbar = tqdm(total=count, desc="Crawling repo infos from ModelScope...")
        res = []
        if save:
            save_path = self.save_dir / "repo-page.jsonl"
            writer = JsonlineWriter(save_path, drop_keys=['repo_org_mapper'])
        for data in crawler.run():
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                pbar.update(1)
                continue
            pbar.write(f"{data.message['repo']} modelscope has {data.message['total_links']} {data.message['category']}.")
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
            pbar.update(1)
        
        writer.close()
        pbar.close()
        self.error_writer.close()
        error_f.close()
        self._crawl_repo_page_res = res
        return self

    def _crawl_detail_page(self, save, **kargs):
        error_f = self.error_f / 'repo-page.jsonl'
        error_f = open(error_f, 'a')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Crawl detail page of ModelScope")
        if not hasattr(self, "_crawl_repo_page_res"):
            logger.info("Missing the running result of the previous step (crawl_repo_page)")
            logger.info(f"Trying load required data from {self.load_dir}")
            reader = JsonlineReader(self.load_dir/'repo-page.jsonl')
            error_list = next(reader.run()).data.get('content')
            model_urls = []
            dataset_urls = []
            for err in error_list:
                if err['category'] == 'models':
                    model_urls.append(err['detail_link'])
                elif err['category'] == 'datasets':
                    dataset_urls.append(err['detail_link'])
            self._crawl_repo_page_res = [PipelineData({
                'category': 'models',
                'detail_urls': model_urls
            }, None, None), PipelineData({
                'category': 'datasets',
                'detail_urls': dataset_urls
            }, None, None)]
        inps = self._crawl_repo_page_res
        kargs = {k: v for k, v in kargs.items() if k in ['threads', 'max_retries', 'screenshot_path']}
        crawler = MSDetailPageCrawler(**kargs)
        count = sum(len(inp.data['detail_urls']) for inp in inps if inp.data is not None)
        pbar = tqdm(total=count, desc="Crawling detail infos from ModelScope...")
        res = []
        if save:
            writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / "raw-models-info.jsonl"),
                str(self.save_dir / "raw-datasets-info.jsonl"),
                ['repo_org_mapper'], ['repo_org_mapper']
            )
        for inp in inps:
            crawler.parse_input(inp)
            for data in crawler.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    pbar.update(1)
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
                pbar.update(1)
        
        writer.close()
        pbar.close()
        self.error_writer.close()
        error_f.close()
        count = defaultdict(int)
        for data in res:
            if 'model_name' in data.data.keys():
                count['models'] += 1
            elif 'dataset_name' in data.data.keys():
                count['datasets'] += 1
        logger.info(f"Crawl detail page done. Total models: {count['models']}. Total datasets: {count['datasets']}")
        self._crawl_detail_page_res = res
        return self
    
    def _post_process(self, save, **kargs):
        error_f = self.error_f / 'post-process-error.jsonl'
        error_f = open(error_f, 'w')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Post Processing of ModelScope data.")
        if not hasattr(self, "_crawl_detail_page_res"):
            logger.info("Missing the running result of the previous step (crawl_detail_page)")
            logger.info(f"Trying load required data from {self.save_dir}")
            org_links_reader = OrgLinksReader(sources=['ModelScope'])
            org_links_reader.parse_input()
            models_reader = JsonlineReader(self.save_dir / 'raw-models-info.jsonl')
            datasets_reader = JsonlineReader(self.save_dir / 'raw-datasets-info.jsonl')
            repo_org_mapper = next(org_links_reader.run()).data['repo_org_mapper']
            self._crawl_detail_page_res = next(models_reader.run()).data.get('content')
            self._crawl_detail_page_res.extend(next(datasets_reader.run()).data.get('content'))
            for inp in self._crawl_detail_page_res:
                inp['repo_org_mapper'] = repo_org_mapper
            self._crawl_detail_page_res = [
                PipelineData(inp, None, None) for inp in self._crawl_detail_page_res
            ]
        inps = self._crawl_detail_page_res
        kargs = {k: v for k, v in kargs.items() if k in [
            'dataset_info_path', 'model_info_path', 'ai_gen', 'ai_check',
            'buffer_size', 'max_retries', 'history_data_path'
        ]}
        processor = MSInfoProcessor(**kargs)
        res = []
        if save:
            writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / 'processed-models-info.jsonl'),
                str(self.save_dir / 'processed-datasets-info.jsonl'),
            )
        for inp in inps:
            processor.parse_input(inp)
            for data in processor.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
        for data in processor.flush(update_infos=True):
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                continue
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
                
        if kargs.get('ai_check', False):
            model_check = {
                f'{data['repo']}/{data['model_name']}': data['total_downloads']
                for data in processor.models_check_buffer
            }
            dataset_check = {
                f'{data['repo']}/{data['dataset_name']}': data['total_downloads']
                for data in processor.datasets_check_buffer
            }
            back_writer = ModelDatasetJsonlineWriter(
                str(self.save_dir / "raw-models-info.jsonl"),
                str(self.save_dir / "raw-datasets-info.jsonl"),
                ['repo_org_mapper'], ['repo_org_mapper']
            )
            for inp in self._crawl_detail_page_res:
                if 'model_name' in inp.data.keys():
                    key = f'{inp.data['repo']}/{inp.data['model_name']}'
                    downloads = model_check.get(key, inp.data['total_downloads'])
                    inp.data['total_downloads'] = downloads
                elif 'dataset_name' in inp.data.keys():
                    key = f'{inp.data['repo']}/{inp.data['dataset_name']}'
                    downloads = dataset_check.get(key, inp.data['total_downloads'])
                    inp.data['total_downloads'] = downloads
                back_writer.parse_input(inp)
                next(back_writer.run())
            back_writer.close()
        
        writer.close()
        self.error_writer.close()
        error_f.close()
        count = defaultdict(int)
        for data in res:
            if 'model_name' in data.data.keys():
                count['models'] += 1
            elif 'dataset_name' in data.data.keys():
                count['datasets'] += 1
        logger.info(f"Post process done. Total models: {count['models']}. Total datasets: {count['datasets']}")
        self._post_process_res = res
        return self


class OpenDataLabPipeline:
    
    def __init__(
        self,
        task_name: str,
        load_dir: str | None = None,
        save_dir: str | None = None,
        log_path: str | None = None,
    ):
        self.task_name = task_name
        self.crawl_date = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        if load_dir:
            self.load_dir = Path(load_dir)
            if self.load_dir.name != 'OpenDataLab':
                self.load_dir = self.load_dir / 'OpenDataLab'
        else:
            self.load_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/OpenDataLab'
        if save_dir:
            self.save_dir = Path(save_dir)
            if self.save_dir.name != 'OpenDataLab':
                self.save_dir = self.save_dir / 'OpenDataLab'
        else:
            self.save_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/OpenDataLab'
        if log_path is None:
            log_path = Path(__file__).parents[2] / f"logs/{task_name}-{self.crawl_date}/running.log"
        else:
            log_path = Path(log_path)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        self.error_f = log_path.parent
        
    def step(
        self,
        stage: Literal[
            'init_org_links',
            'crawl_repo_page',
            'post_process',
        ],
        save: bool,
        **kargs,
    ):
        match stage:
            case 'init_org_links':
                return self._init_org_links(save, **kargs)
            case 'crawl_repo_page':
                return self._crawl_repo_page(save, **kargs)
            case 'post_process':
                return self._post_process(save, **kargs)

    def done(self):
        logger.success(f"OpenDataLabPipeline {self.task_name} done.")
        
    def _init_org_links(self, save, **kargs):
        logger.info("Init org links of OpenDataLab")
        kargs = {k: v for k, v in kargs.items() if k in ['path', 'orgs']}
        kargs['sources'] = ['OpenDataLab']
        reader = OrgLinksReader(**kargs)
        reader.parse_input()
        res = next(reader.run())
        logger.info(f"Target orgs: {res.message['target_orgs']}")
        logger.info(f"Total links: {res.message['total_links']}")
        if save:
            save_path = self.save_dir / "org-links.jsonl"
            writer = JsonlineWriter(save_path)
            writer.parse_input(res)
            res = next(writer.run())
        writer.close()
        self._init_org_links_res = res
        return self
    
    def _crawl_repo_page(self, save, **kargs):
        logger.info("Crawl OpenDataLab page")
        if not hasattr(self, "_init_org_links_res"):
            raise RuntimeError("Missing the running result of the previous step (init_org_links)")
        inp = self._init_org_links_res
        kargs = {k: v for k, v in kargs.items() if k in ['threads', 'max_retries']}
        crawler = OpenDataLabCrawler(**kargs)
        crawler.parse_input(inp)
        count = len(crawler.input['links'])
        pbar = tqdm(total=count, desc="Crawling OpenDataLab infos...")
        res = []
        if save:
            save_path = self.save_dir / "raw-datasets-info.jsonl"
            writer = JsonlineWriter(save_path)
        for data in crawler.run():
            if data.error is not None:
                raise RuntimeError("Error crawling OpenDataLab page.")
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
            pbar.update(1)
            
        writer.close()
        pbar.close()
        self._crawl_repo_page_res = res
        
    def _post_process(self, save, **kargs):
        error_f = self.error_f / 'post-process-error.jsonl'
        error_f = open(error_f, 'w')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Post Processing of OpenDataLab data.")
        if not hasattr(self, "_crawl_repo_page_res"):
            logger.info("Missing the running result of the previous step (crawl_repo_page)")
            logger.info(f"Trying load required data from {self.save_dir}")
            datasets_reader = JsonlineReader(self.save_dir / 'raw-datasets-info.jsonl')
            self._crawl_repo_page_res = next(datasets_reader.run()).data.get('content')
            self._crawl_repo_page_res = [
                PipelineData(inp, None, None) for inp in self._crawl_repo_page_res
            ]

        inps = self._crawl_repo_page_res
        kargs = {k: v for k, v in kargs.items() if k in [
            'dataset_info_path', 'history_data_path', 'ai_gen',
            'buffer_size', 'max_retries'
        ]}
        processor = OpenDataLabInfoProcessor(**kargs)
        res = []
        if save:
            writer = JsonlineWriter(str(self.save_dir / 'processed-datasets-info.jsonl'))
        for inp in inps:
            processor.parse_input(inp)
            for data in processor.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
        for data in processor.flush(update_infos=True):
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                continue
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
        
        writer.close()
        self.error_writer.close()
        error_f.close()
        count = len(res)
        logger.info(f"Post process done. Total datasets: {count}")
        self._post_process_res = res
        return self


class BAAIDataPipeline:
    
    def __init__(
        self,
        task_name: str,
        load_dir: str | None = None,
        save_dir: str | None = None,
        log_path: str | None = None,
    ):
        self.task_name = task_name
        self.crawl_date = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        if load_dir:
            self.load_dir = Path(load_dir)
            if self.load_dir.name != 'BAAIData':
                self.load_dir = self.load_dir / 'BAAIData'
        else:
            self.load_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/BAAIData'
        if save_dir:
            self.save_dir = Path(save_dir)
            if self.save_dir.name != 'BAAIData':
                self.save_dir = self.save_dir / 'BAAIData'
        else:
            self.save_dir = Path(__file__).parents[2] / f'data/{str(datetime.today().date())}/BAAIData'
        if log_path is None:
            log_path = Path(__file__).parents[2] / f"logs/{task_name}-{self.crawl_date}/running.log"
        else:
            log_path = Path(log_path)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        self.error_f = log_path.parent
        
    def step(
        self,
        stage: Literal[
            'init_org_links',
            'crawl_repo_page',
            'post_process',
        ],
        save: bool,
        **kargs,
    ):
        match stage:
            case 'init_org_links':
                return self._init_org_links(save, **kargs)
            case 'crawl_repo_page':
                return self._crawl_repo_page(save, **kargs)
            case 'post_process':
                return self._post_process(save, **kargs)

    def done(self):
        logger.success(f"BAAIDataPipeline {self.task_name} done.")
        
    def _init_org_links(self, save, **kargs):
        logger.info("Init org links of BAAIData")
        kargs = {k: v for k, v in kargs.items() if k in ['path']}
        kargs['sources'] = ['BAAIData']
        reader = OrgLinksReader(**kargs)
        reader.parse_input()
        res = next(reader.run())
        logger.info(f"Target orgs: {res.message['target_orgs']}")
        logger.info(f"Total links: {res.message['total_links']}")
        if save:
            save_path = self.save_dir / "org-links.jsonl"
            writer = JsonlineWriter(save_path)
            writer.parse_input(res)
            res = next(writer.run())
        writer.close()
        self._init_org_links_res = res
        return self
    
    def _crawl_repo_page(self, save, **kargs):
        logger.info("Crawl OpenDataLab page")
        if not hasattr(self, "_init_org_links_res"):
            raise RuntimeError("Missing the running result of the previous step (init_org_links)")
        inp = self._init_org_links_res
        kargs = {k: v for k, v in kargs.items() if k in ['max_retries']}
        crawler = BAAIDatasetsCrawler(**kargs)
        crawler.parse_input(inp)
        res = []
        if save:
            save_path = self.save_dir / "raw-datasets-info.jsonl"
            writer = JsonlineWriter(save_path)
        for data in crawler.run():
            if data.error is not None:
                raise RuntimeError("Error crawling BAAIData.")
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
            
        writer.close()
        self._crawl_repo_page_res = res
        
    def _post_process(self, save, **kargs):
        error_f = self.error_f / 'post-process-error.jsonl'
        error_f = open(error_f, 'w')
        self.error_writer = jsonlines.Writer(error_f)
        logger.info("Post Processing of BAAIData data.")
        if not hasattr(self, "_crawl_repo_page_res"):
            logger.info("Missing the running result of the previous step (crawl_repo_page)")
            logger.info(f"Trying load required data from {self.save_dir}")
            datasets_reader = JsonlineReader(self.save_dir / 'raw-datasets-info.jsonl')
            self._crawl_repo_page_res = next(datasets_reader.run()).data.get('content')
            self._crawl_repo_page_res = [
                PipelineData(inp, None, None) for inp in self._crawl_repo_page_res
            ]

        inps = self._crawl_repo_page_res
        kargs = {k: v for k, v in kargs.items() if k in [
            'dataset_info_path', 'history_data_path', 'ai_gen',
            'buffer_size', 'max_retries'
        ]}
        processor = BAAIDataInfoProcessor(**kargs)
        res = []
        if save:
            writer = JsonlineWriter(str(self.save_dir / 'processed-datasets-info.jsonl'))
        for inp in inps:
            processor.parse_input(inp)
            for data in processor.run():
                if data.error is not None:
                    self.error_writer.write(data.error)
                    error_f.flush()
                    continue
                if save:
                    writer.parse_input(data)
                    res.append(next(writer.run()))
                else:
                    res.append(data)
        for data in processor.flush(update_infos=True):
            if data.error is not None:
                self.error_writer.write(data.error)
                error_f.flush()
                continue
            if save:
                writer.parse_input(data)
                res.append(next(writer.run()))
            else:
                res.append(data)
        
        writer.close()
        self.error_writer.close()
        error_f.close()
        count = len(res)
        logger.info(f"Post process done. Total datasets: {count}")
        self._post_process_res = res
        return self


class MergeAndRankingPipeline:
    
    def __init__(
        self,
        data_dir: str | None = None,
        log_path: str | None = None,
    ):
        self.now = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        date = str(datetime.today().date())
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            self.data_dir = Path(__file__).parents[2] / f'data/{date}'
        assert re.match(r"\d+-\d+-\d+", self.data_dir.name) 
        if log_path is None:
            log_path = Path(__file__).parents[2] / f"logs/ranking-{self.now}/running.log"
        else:
            log_path = Path(log_path)
        self.data_dir_last_month = self._get_last_month_path(self.data_dir.name)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        
    def _get_last_month_path(self, date: str):
        date = datetime.strptime(date, r"%Y-%m-%d")
        last_month_date = date - timedelta(days=30)
        
        min_diff = None
        closest_date = None
        
        for d in (Path(__file__).parents[2]/'data').glob(r"????-??-??"):
            cur_date = datetime.strptime(d.name, r"%Y-%m-%d")
            diff = abs((cur_date - last_month_date).days)
            if min_diff is None or diff < min_diff:
                min_diff = diff
                closest_date = d
                
        if min_diff > 15:
            return None
        
        if (closest_date / 'overall-rank.csv').exists():
            return closest_date
        return None
        
    def step(
        self,
        stage: Literal[
            "merge_models",
            "merge_datasets",
            "ranking",
        ],
        save: bool = True,
        **kargs,
    ):
        match stage:
            case 'merge_models':
                return self._merge_models(save, **kargs)
            case 'merge_datasets':
                return self._merge_dataset(save, **kargs)
            case 'ranking':
                return self._ranking(save, **kargs)
    
    def done(self):
        logger.success("Merge and ranking done.")
        
    def _merge_models(self, save, **kargs):
        logger.info("Merge models")
        buffer = defaultdict(list)
        for p in self.data_dir.iterdir():
            data_path = p / 'processed-models-info.jsonl'
            if not data_path.exists():
                continue
            with jsonlines.open(data_path, 'r') as reader:
                for data in reader:
                    key = f"{data['org']}/{data['model_name']}"
                    buffer[key].append(data)
        if len(buffer) == 0:
            raise RuntimeError("No processed data found.")
        save_path = self.data_dir / 'merged-models-info.jsonl'
        res = []
        with jsonlines.open(save_path, 'w') as writer:
            for _, models in buffer.items():
                data = {
                    "org": models[0]['org'],
                    "repo": models[0]['repo'],
                    "model_name": models[0]['model_name'],
                    "modality": models[0]['modality'],
                    "downloads_last_month": sum(model['downloads_last_month']
                                                for model in models
                                                if model['downloads_last_month'] > 0),
                    "likes": sum(model['likes'] for model in models),
                    "community": sum(model['community'] for model in models
                                        if 'community' in model),
                    "descendants": sum(model['descendants'] for model in models
                                        if 'descendants' in model),
                    "date_crawl": models[0]['date_crawl'],
                } # TODO Currently missing the date_last_crawl and date_enter_db fields
                writer.write(data)
                res.append(data)
        self._merge_models_res = res
        logger.info(f"Total model records: {len(buffer)}")
        return self
        
    def _merge_dataset(self, save, **kargs):
        logger.info("Merge datasets")
        buffer = defaultdict(list)
        for p in self.data_dir.iterdir():
            data_path = p / 'processed-datasets-info.jsonl'
            if not data_path.exists():
                continue
            with jsonlines.open(data_path, 'r') as reader:
                for data in reader:
                    key = f"{data['org']}/{data['dataset_name']}"
                    buffer[key].append(data)
        if len(buffer) == 0:
            raise RuntimeError("No processed data found.")
        save_path = self.data_dir / 'merged-datasets-info.jsonl'
        res = []
        with jsonlines.open(save_path, 'w') as writer:
            for _, datasets in buffer.items():
                data = {
                    "org": datasets[0]['org'],
                    "repo": datasets[0]['repo'],
                    "dataset_name": datasets[0]['dataset_name'],
                    "modality": datasets[0]['modality'],
                    "lifecycle": datasets[0]['lifecycle'],
                    "downloads_last_month": sum(dataset['downloads_last_month']
                                                for dataset in datasets
                                                if dataset['downloads_last_month'] > 0),
                    "likes": sum(dataset['likes'] for dataset in datasets),
                    "community": sum(dataset['community'] for dataset in datasets
                                        if 'community' in dataset),
                    "dataset_usage": sum(dataset['dataset_usage'] for dataset in datasets
                                            if 'dataset_usage' in dataset),
                    "date_crawl": datasets[0]['date_crawl'],
                } # TODO Currently missing the date_last_crawl and date_enter_db fields
                writer.write(data)
                res.append(data)
        self._merge_datasets_res = res
        logger.info(f"Total datasets records: {len(buffer)}")
        return self

    def _summary_data(
        self, 
        df: pd.DataFrame, 
        config: dict,
        target_orgs: list,
    ) -> pd.DataFrame:
        weights: dict[str, float | int] = config[1]
        if target_orgs[0] == 'all':
            target_orgs = df['org'].unique()
        res = pd.DataFrame(index=target_orgs)
        lifecycle_mapper = {
            'pretraining': 'Pre-training',
            'finetuning': 'Fine-tuning',
            'preference': 'Preference'
        }
        
        # TODO temp handle other source dataset
        data_path = Path(__file__).parents[2] / 'data/other-source-datasets.jsonl'
        if data_path.exists():
            other_data = pd.read_json(data_path, lines=True)
        
        for key in weights.keys():
            if key.startswith('num'):
                if key.split("_")[-1] in ['pretraining', 'finetuning', 'preference']:
                    lifecycle = lifecycle_mapper.get(key.split("_")[-1])
                    res[key] = df[df["lifecycle"] == lifecycle].groupby(
                        'org').size().reindex(target_orgs, fill_value=0)
                    
                    # TODO temp handle other source dataset
                    if data_path.exists():
                        res[key] += other_data[other_data['lifecycle'] == lifecycle].groupby(
                            'org').size().reindex(target_orgs, fill_value=0)
                else:
                    modality = key.split("_")[-1].title()
                    res[key] = df[df['modality'] == modality].groupby(
                        'org').size().reindex(target_orgs, fill_value=0)
                    
                    # TODO temp handle other source dataset
                    if data_path.exists():
                        res[key] += other_data[other_data['modality'] == modality].groupby(
                            'org').size().reindex(target_orgs, fill_value=0)
            elif key.startswith("downloads"):
                if key.split("_")[-1] in ['pretraining', 'finetuning', 'preference']:
                    lifecycle = lifecycle_mapper.get(key.split("_")[-1])
                    res[key] = df[df["lifecycle"] == lifecycle].groupby(
                        'org')['downloads_last_month'].sum().reindex(target_orgs, fill_value=0)
                else: 
                    modality = key.split("_")[-1].title()
                    res[key] = df[df['modality'] == modality].groupby(
                        'org')['downloads_last_month'].sum().reindex(target_orgs, fill_value=0)
            elif key == 'dataset_usage':
                res[key] = df.groupby('org')['dataset_usage'].sum().reindex(
                    target_orgs, fill_value=0)
            elif key == 'operators':
                # TODO data process tool operators
                res[key] = pd.Series({
                    "BAAI": 24,
                    "Ali": 105,
                }).reindex(target_orgs, fill_value=0)
            else:
                raise RuntimeError(f"Unrecognized field {key}")
            
        res.index.name = 'org'
        return res

    def _summary_model(
        self, 
        df: pd.DataFrame, 
        config: dict,
        target_orgs: list,
    ) -> pd.DataFrame:
        weights: dict[str, float | int] = config[1]
        if target_orgs[0] == 'all':
            target_orgs = df['org'].unique()
        res = pd.DataFrame(index=target_orgs)
        for key in weights.keys():
            if key.startswith('num') and key != 'num_adapted_chips':
                modality = key.split("_")[-1].title()
                res[key] = df[df['modality'] == modality].groupby(
                    'org').size().reindex(target_orgs, fill_value=0)
            elif key.startswith('downloads'):
                modality = key.split("_")[-1].title()
                res[key] = df[df['modality'] == modality].groupby(
                    'org')['downloads_last_month'].sum().reindex(
                        target_orgs, fill_value=0)
            elif key == 'descendants':
                res[key] = df.groupby('org')['descendants'].sum().reindex(
                    target_orgs, fill_value=0)
            elif key == 'likes':
                res[key] = df.groupby('org')[key].sum().reindex(
                    target_orgs, fill_value=0)
            elif key == 'issue':
                res[key] = df.groupby('org')['community'].sum().reindex(
                    target_orgs, fill_value=0)
            elif key == 'num_adapted_chips':
                # TODO chips model
                res[key] = pd.Series({
                    "BAAI": 4,
                    "Baidu": 2,
                    "Huawei": 2,
                    "Meta": 2,
                    "Google": 2,
                    "ByteDance": 2
                }).reindex(target_orgs, fill_value=1)
            else:
                raise RuntimeError(f"Unrecognized field {key}")

        res.index.name = 'org'
        return res

    def _summary_infra(self, config: dict, target_orgs: list) -> pd.DataFrame:
        infra_path = self.data_dir / 'infra-summary.csv'
        weights: dict[str, float | int] = config[1]
        df = pd.read_csv(infra_path, index_col='org')
        if target_orgs[0] == 'all':
            target_orgs = df.index.unique()
        df = df[df.columns.intersection(weights.keys())]
        df = df[df.index.isin(target_orgs)]
        return df
    
    def _summary_eval(self, config: dict, target_orgs: list) -> pd.DataFrame:
        eval_path = self.data_dir / 'eval-summary.csv'
        weights: dict[str, float | int] = config[1]
        df = pd.read_csv(eval_path, index_col='org')
        if target_orgs[0] == 'all':
            target_orgs = df.index.unique()
        df = df[df.columns.intersection(weights.keys())]
        df = df[df.index.isin(target_orgs)]
        return df

    def _normalize_summary(self, summary: pd.DataFrame, config: dict) -> pd.DataFrame:
        df = summary.div(summary.max())
        weights = config[1]
        weights = {
            k: v if isinstance(v, (int, float)) else eval(v)
            for k, v in weights.items() if k in summary.columns
        }
        if config[0] == 'average':
            df['score'] = df.mean(axis=1)
            df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        elif config[0] == 'weight':
            df['score'] = df.mul(weights).sum(axis=1)
            df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        else:
            raise RuntimeError(f'Unrecognized method: {config[0]}, accept `average` or `weight`')
        return df

    def _ranking(self, save, **kargs):
        logger.info("Calculate ranking.")

        if not hasattr(self, "_merge_models_res"):
            logger.info(f"Trying load merged models from {self.data_dir}")
            merged_models = pd.read_json(self.data_dir/'merged-models-info.jsonl', 
                                         lines=True)
        else:
            merged_models = pd.DataFrame(self._merge_models_res)
        if not hasattr(self, "_merge_datasets_res"):
            logger.info(f"Trying load merged datasets from {self.data_dir}")
            merged_datasets = pd.read_json(self.data_dir/'merged-datasets-info.jsonl', 
                                           lines=True)
        else:
            merged_datasets = pd.DataFrame(self._merge_datasets_res)

        kargs = {k: v for k, v in kargs.items() if k in [
            'data_config', 'model_config', 'infra_config', 'eval_config',
            'target_orgs', 'ranking_weights'
        ]}
        target_orgs = kargs.get('target_orgs', ['all'])
        
        # TODO Add embodied model? If not adding embodied model, then reset to multimodal.
        if kargs['model_config'][1].get('num_embodied') is None and kargs['model_config'][1].get('downloads_embodied') is None:
            merged_models['modality'].replace('Embodied', 'Multimodal')

        logger.info("Summary table of data from four dimensions.")
        infra_summary = self._summary_infra(kargs['infra_config'], target_orgs)
        eval_summary = self._summary_eval(kargs['eval_config'], target_orgs)
        if target_orgs[0] == 'all':
            target_orgs = infra_summary.index.tolist()
        data_summary = self._summary_data(merged_datasets, kargs['data_config'], target_orgs)
        model_summary = self._summary_model(merged_models, kargs['model_config'], target_orgs)
        
        if self.data_dir_last_month:
            data_summary_last_month = pd.read_csv(self.data_dir_last_month/'data-summary.csv',
                                                  index_col='org')
            model_summary_last_month = pd.read_csv(self.data_dir_last_month/'model-summary.csv',
                                                   index_col='org')
            data_summary_delta = data_summary - data_summary_last_month
            model_summary_delta = model_summary - model_summary_last_month
            
            data_summary_delta.to_csv(self.data_dir / "data-summary-delta.csv")
            model_summary_delta.to_csv(self.data_dir / "model-summary-delta.csv")
        
        data_summary.to_csv(self.data_dir / "data-summary.csv")
        model_summary.to_csv(self.data_dir / "model-summary.csv")

        logger.info("Normalize the summary table and calculate the rankings for each dimension.")
        data_normalization = self._normalize_summary(data_summary, kargs['data_config'])
        model_normalization = self._normalize_summary(model_summary, kargs['model_config'])
        infra_normalization = self._normalize_summary(infra_summary, kargs['infra_config'])
        eval_normalization = self._normalize_summary(eval_summary, kargs['eval_config'])
        
        if self.data_dir_last_month:
            data_rank_last_month = pd.read_csv(self.data_dir_last_month/'data-rank.csv',
                                               index_col='org')
            model_rank_last_month = pd.read_csv(self.data_dir_last_month/'model-rank.csv',
                                                index_col='org')
            infra_rank_last_month = pd.read_csv(self.data_dir_last_month/'infra-rank.csv',
                                                index_col='org')
            eval_rank_last_month = pd.read_csv(self.data_dir_last_month/'eval-rank.csv',
                                               index_col='org')
            data_normalization['delta rank'] = data_rank_last_month['rank'] - data_normalization['rank']
            model_normalization['delta rank'] = model_rank_last_month['rank'] - model_normalization['rank']
            infra_normalization['delta rank'] = infra_rank_last_month['rank'] - infra_normalization['rank']
            eval_normalization['delta rank'] = eval_rank_last_month['rank'] - eval_normalization['rank']
        
        data_normalization.to_csv(self.data_dir / 'data-rank.csv')
        model_normalization.to_csv(self.data_dir / 'model-rank.csv')
        infra_normalization.to_csv(self.data_dir / 'infra-rank.csv')
        eval_normalization.to_csv(self.data_dir / 'eval-rank.csv')

        logger.info("Calculate overall ranking based on sub-dimension rankings.")
        orgs = data_normalization.index.intersection(
            model_normalization.index
        ).intersection(
            infra_normalization.index
        ).intersection(
            eval_normalization.index
        )
        overall_ranking = pd.DataFrame(index=orgs)
        overall_ranking.index.name = 'org'
        overall_ranking['data'] = 1 / np.log2(data_normalization['rank'] + 1)
        overall_ranking['model'] = 1 / np.log2(model_normalization['rank'] + 1)
        overall_ranking['infra'] = 1 / np.log2(infra_normalization['rank'] + 1)
        overall_ranking['eval'] = 1 / np.log2(eval_normalization['rank'] + 1)
        overall_weights = kargs['ranking_weights']
        overall_weights = {
            k: v if isinstance(v, (int, float)) else eval(v)
            for k, v in overall_weights.items()
        }
        overall_ranking['score'] = overall_ranking.mul(overall_weights).sum(axis=1)
        overall_ranking['rank'] = overall_ranking['score'].rank(ascending=False, method='dense').astype(int)
        
        if self.data_dir_last_month:
            overall_ranking_last_month = pd.read_csv(self.data_dir_last_month/'overall-rank.csv',
                                                     index_col='org')
            overall_ranking['delta rank'] = overall_ranking_last_month['rank'] - overall_ranking['rank']
        
        overall_ranking.to_csv(self.data_dir / "overall-rank.csv")
        return self


class AccumulateAndRankingPipeline:
    
    def __init__(
        self,
        data_dir: str | None,
        log_path: str | None,
    ):
        self.now = datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")
        self.date = str(datetime.today().date())
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            self.data_dir = Path(__file__).parents[2] / f'data/{self.date}'
        assert re.match(r"\d+-\d+-\d+", self.data_dir.name) 
        if log_path is None:
            log_path = Path(__file__).parents[2] / f"logs/accumulating-{self.now}/running.log"
        else:
            log_path = Path(log_path)
        self.data_dir_last_month = self._get_last_month_path(self.data_dir.name)
        self.log_path = log_path
        self.log_path.parent.mkdir(exist_ok=True, parents=True)
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
        logger.add(log_path, level="DEBUG")
        
    def _get_last_month_path(self, date: str):
        date = datetime.strptime(date, r"%Y-%m-%d")
        last_month_date = date - timedelta(days=30)
        
        min_diff = None
        closest_date = None
        
        for d in sorted((Path(__file__).parents[2]/'data').glob(r"????-??-??"))[1:]:
            cur_date = datetime.strptime(d.name, r"%Y-%m-%d")
            diff = abs((cur_date - last_month_date).days)
            if min_diff is None or diff < min_diff:
                min_diff = diff
                closest_date = d
                
        if min_diff > 15:
            return None
        
        if (closest_date / 'overall-accumulated-rank.csv').exists():
            return closest_date
        return closest_date
        
    def step(
        self,
        stage: Literal[
            'accumulate',
            'ranking'
        ],
        save: bool = True,
        **kargs,
    ):
        match stage:
            case 'accumulate':
                return self._accumulate(save, **kargs)
            case 'ranking':
                return self._ranking(save, **kargs)
    
    def done(self):
        logger.success("AccumulateAndRankingPipeline done.")
    
    def _accumulate(self, save, **kargs):
        date = datetime.strptime(self.date, r"%Y-%m-%d")
        base_path = self.data_dir.parent
        models_buffer = defaultdict(list)
        datasets_buffer = defaultdict(list)
        for path in sorted(base_path.glob("????-??-??"))[1:]:
            if datetime.strptime(path.name, r"%Y-%m-%d") > date:
                continue
            with jsonlines.open(path/'merged-models-info.jsonl', 'r') as f:
                for item in f:
                    key = f"{item['repo']}/{item['model_name']}"
                    models_buffer[key].append(item)
            with jsonlines.open(path/'merged-datasets-info.jsonl', 'r') as f:
                for item in f:
                    key = f"{item['repo']}/{item['dataset_name']}"
                    datasets_buffer[key].append(item)
        models_buffer = [
            {
                'org': lst[0]['org'],
                'repo': lst[0]['repo'],
                'model_name': lst[0]['model_name'],
                'modality': lst[0]['modality'],
                'accumulated_downloads': sum(item['downloads_last_month'] for item in lst),
                'likes': max([item['likes'] for item in lst]),
                'community': max([item['community'] for item in lst]),
                'descendants': max([item['descendants'] for item in lst]), 
            }
            for lst in models_buffer.values()
        ]
        datasets_buffer = [
            {
                'org': lst[0]['org'],
                'repo': lst[0]['repo'],
                'dataset_name': lst[0]['dataset_name'],
                'modality': lst[0]['modality'],
                'lifecycle': lst[0]['lifecycle'],
                'accumulated_downloads': sum(item['downloads_last_month'] for item in lst),
                'likes': max([item['likes'] for item in lst]),
                'community': max([item['community'] for item in lst]),
                'dataset_usage': max([item['dataset_usage'] for item in lst])
            }
            for lst in datasets_buffer.values()
        ]
        with jsonlines.open(self.data_dir/'accumulated-models-info.jsonl', 'w') as f:
            f.write_all(models_buffer)
        with jsonlines.open(self.data_dir/'accumulated-datasets-info.jsonl', 'w') as f:
            f.write_all(datasets_buffer)
            
        self._accumulated_models = models_buffer
        self._accumulated_datasets = datasets_buffer
        return self
        
    def _summary_data(
        self, 
        df: pd.DataFrame, 
        config: dict,
        target_orgs: list,
    ) -> pd.DataFrame:
        weights: dict[str, float | int] = config[1]
        if target_orgs[0] == 'all':
            target_orgs = df['org'].unique()
        res = pd.DataFrame(index=target_orgs)
        lifecycle_mapper = {
            'pretraining': 'Pre-training',
            'finetuning': 'Fine-tuning',
            'preference': 'Preference'
        }
        
        # TODO temp handle other source dataset
        data_path = Path(__file__).parents[2] / 'data/other-source-datasets.jsonl'
        if data_path.exists():
            other_data = pd.read_json(data_path, lines=True)
            
        for key in weights.keys():
            if key.startswith('num'):
                if key.split("_")[-1] in ['pretraining', 'finetuning', 'preference']:
                    lifecycle = lifecycle_mapper.get(key.split("_")[-1])
                    res[key] = df[df["lifecycle"] == lifecycle].groupby(
                        'org').size().reindex(target_orgs, fill_value=0)
                    
                    # TODO temp handle other source dataset
                    if data_path.exists():
                        res[key] += other_data[other_data['lifecycle'] == lifecycle].groupby(
                            'org').size().reindex(target_orgs, fill_value=0)
                else:
                    modality = key.split("_")[-1].title()
                    res[key] = df[df['modality'] == modality].groupby(
                        'org').size().reindex(target_orgs, fill_value=0)
                    
                    # TODO temp handle other source dataset
                    if data_path.exists():
                        res[key] += other_data[other_data['modality'] == modality].groupby(
                            'org').size().reindex(target_orgs, fill_value=0)
            elif key.startswith("downloads"):
                if key.split("_")[-1] in ['pretraining', 'finetuning', 'preference']:
                    lifecycle = lifecycle_mapper.get(key.split("_")[-1])
                    res[key] = df[df["lifecycle"] == lifecycle].groupby(
                        'org')['accumulated_downloads'].sum().reindex(target_orgs, fill_value=0)
                else: 
                    modality = key.split("_")[-1].title()
                    res[key] = df[df['modality'] == modality].groupby(
                        'org')['accumulated_downloads'].sum().reindex(target_orgs, fill_value=0)
            elif key == 'dataset_usage':
                res[key] = df.groupby('org')['dataset_usage'].sum().reindex(
                    target_orgs, fill_value=0)
            elif key == 'operators':
                # TODO data process tool operators
                res[key] = pd.Series({
                    "BAAI": 24,
                    "Ali": 105,
                }).reindex(target_orgs, fill_value=0)
            else:
                raise RuntimeError(f"Unrecognized field {key}")
            
        res.index.name = 'org'
        return res

    def _summary_model(
        self, 
        df: pd.DataFrame, 
        config: dict,
        target_orgs: list,
    ) -> pd.DataFrame:
        weights: dict[str, float | int] = config[1]
        if target_orgs[0] == 'all':
            target_orgs = df['org'].unique()
        res = pd.DataFrame(index=target_orgs)
        for key in weights.keys():
            if key.startswith('num') and key != 'num_adapted_chips':
                modality = key.split("_")[-1].title()
                res[key] = df[df['modality'] == modality].groupby(
                    'org').size().reindex(target_orgs, fill_value=0)
            elif key.startswith('downloads'):
                modality = key.split("_")[-1].title()
                res[key] = df[df['modality'] == modality].groupby(
                    'org')['accumulated_downloads'].sum().reindex(
                        target_orgs, fill_value=0)
            elif key == 'descendants':
                res[key] = df.groupby('org')['descendants'].sum().reindex(
                    target_orgs, fill_value=0)
            elif key == 'likes':
                res[key] = df.groupby('org')[key].sum().reindex(
                    target_orgs, fill_value=0)
            elif key == 'issue':
                res[key] = df.groupby('org')['community'].sum().reindex(
                    target_orgs, fill_value=0)
            elif key == 'num_adapted_chips':
                # TODO chips model
                res[key] = pd.Series({
                    "BAAI": 4,
                    "Baidu": 2,
                    "Huawei": 2,
                    "Meta": 2,
                    "Google": 2,
                    "ByteDance": 2
                }).reindex(target_orgs, fill_value=1)
            else:
                raise RuntimeError(f"Unrecognized field {key}")

        res.index.name = 'org'
        return res
    
    def _summary_infra(self, config: dict, target_orgs: list) -> pd.DataFrame:
        infra_path = self.data_dir / 'infra-summary.csv'
        weights: dict[str, float | int] = config[1]
        df = pd.read_csv(infra_path, index_col='org')
        if target_orgs[0] == 'all':
            target_orgs = df.index.unique()
        df = df[df.columns.intersection(weights.keys())]
        df = df[df.index.isin(target_orgs)]
        return df
    
    def _summary_eval(self, config: dict, target_orgs: list) -> pd.DataFrame:
        eval_path = self.data_dir / 'eval-summary.csv'
        weights: dict[str, float | int] = config[1]
        df = pd.read_csv(eval_path, index_col='org')
        if target_orgs[0] == 'all':
            target_orgs = df.index.unique()
        df = df[df.columns.intersection(weights.keys())]
        df = df[df.index.isin(target_orgs)]
        return df
    
    def _normalize_summary(self, summary: pd.DataFrame, config: dict) -> pd.DataFrame:
        df = summary.div(summary.max())
        weights = config[1]
        weights = {
            k: v if isinstance(v, (int, float)) else eval(v)
            for k, v in weights.items() if k in summary.columns
        }
        if config[0] == 'average':
            df['score'] = df.mean(axis=1)
            df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        elif config[0] == 'weight':
            df['score'] = df.mul(weights).sum(axis=1)
            df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        else:
            raise RuntimeError(f'Unrecognized method: {config[0]}, accept `average` or `weight`')
        return df

    def _ranking(self, save, **kargs):
        logger.info("Calculate accumulated ranking.")
        
        if not hasattr(self, '_accumulated_models'):
            logger.info(f"Trying load accumulated models from {self.data_dir}")
            accumulated_models = pd.read_json(self.data_dir/'accumulated-models-info.jsonl',
                                              lines=True)
        else:
            accumulated_models = pd.DataFrame(self._accumulated_models)
        if not hasattr(self, '_accumulated_datasets'):
            logger.info(f"Trying load accumulated datasets from {self.data_dir}")
            accumulated_datasets = pd.read_json(self.data_dir/'accumulated-datasets-info.jsonl',
                                                lines=True)
        else:
            accumulated_datasets = pd.DataFrame(self._accumulated_datasets)

        kargs = {k: v for k, v in kargs.items() if k in [
            'data_config', 'model_config', 'infra_config', 'eval_config',
            'target_orgs', 'ranking_weights'
        ]}
        target_orgs = kargs.get('target_orgs', ['all'])
        
        # TODO Add embodied model? If not adding embodied model, then reset to multimodal.
        if kargs['model_config'][1].get('num_embodied') is None and kargs['model_config'][1].get('downloads_embodied') is None:
            accumulated_models['modality'].replace('Embodied', 'Multimodal')
        
        logger.info("Summary table of data from four dimensions.")
        infra_summary = self._summary_infra(kargs['infra_config'], target_orgs)
        eval_summary = self._summary_eval(kargs['eval_config'], target_orgs)
        if target_orgs[0] == 'all':
            target_orgs = infra_summary.index.tolist()
        data_summary = self._summary_data(accumulated_datasets, kargs['data_config'], target_orgs)
        model_summary = self._summary_model(accumulated_models, kargs['model_config'], target_orgs)
        
        data_summary.to_csv(self.data_dir/"data-accumulated-summary.csv")
        model_summary.to_csv(self.data_dir/"model-accumulated-summary.csv")
        
        logger.info("Normalize the summary table and calculate the rankings for datasets and models.")
        data_normalization = self._normalize_summary(data_summary, kargs['data_config'])
        model_normalization = self._normalize_summary(model_summary, kargs['model_config'])
        infra_normalization = self._normalize_summary(infra_summary, kargs['infra_config'])
        eval_normalization = self._normalize_summary(eval_summary, kargs['eval_config'])
        
        if self.data_dir_last_month:
            data_rank_last_month = pd.read_csv(
                self.data_dir_last_month/'data-accumulated-rank.csv',
                index_col='org')
            model_rank_last_month = pd.read_csv(
                self.data_dir_last_month/'model-accumulated-rank.csv',
                index_col='org')
            data_normalization['delta rank'] = data_rank_last_month['rank'] - data_normalization['rank']
            model_normalization['delta rank'] = model_rank_last_month['rank'] - model_normalization['rank']
            
        data_normalization.to_csv(self.data_dir/'data-accumulated-rank.csv')
        model_normalization.to_csv(self.data_dir/'model-accumulated-rank.csv')

        logger.info("Calculate overall ranking based on sub-dimension rankings.")
        orgs = data_normalization.index.intersection(
            model_normalization.index
        ).intersection(
            infra_normalization.index
        ).intersection(
            eval_normalization.index
        )
        overall_ranking = pd.DataFrame(index=orgs)
        overall_ranking['data'] = 1 / np.log2(data_normalization['rank'] + 1)
        overall_ranking['model'] = 1 / np.log2(model_normalization['rank'] + 1)
        overall_ranking['infra'] = 1 / np.log2(infra_normalization['rank'] + 1)
        overall_ranking['eval'] = 1 / np.log2(eval_normalization['rank'] + 1)
        overall_weights = kargs['ranking_weights']
        overall_weights = {
            k: v if isinstance(v, (int, float)) else eval(v)
            for k, v in overall_weights.items()
        }
        overall_ranking['score'] = overall_ranking.mul(overall_weights).sum(axis=1)
        overall_ranking['rank'] = overall_ranking['score'].rank(ascending=False, method='dense').astype(int)
        
        if self.data_dir_last_month:
            overall_ranking_last_month = pd.read_csv(
                self.data_dir_last_month/'overall-accumulated-rank.csv',
                index_col='org')
            overall_ranking['delta rank'] = overall_ranking_last_month['rank'] - overall_ranking['rank']
        
        overall_ranking.to_csv(self.data_dir/"overall-accumulated-rank.csv")
        return self
