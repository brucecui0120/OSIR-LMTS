import json
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Optional
from collections import defaultdict
from typing_extensions import deprecated
import jsonlines
from loguru import logger
from .base import PipelineStep, PipelineResult, PipelineData
from ..ai.model_info_generator import ModelInfo, gen_model_info_huggingface, gen_model_info_modelscope
from ..ai.dataset_info_generator import DatasetInfo, gen_dataset_info_huggingface, gen_dataset_info_modelscope
from ..ai.screenshot_checker import check_image_info, CheckRequest


class HFInfoProcessor(PipelineStep):
    
    ptype = "🚗 PROCESSOR"
    required_keys = ["repo_org_mapper"]
    
    def __init__(
        self,
        dataset_info_path: str | None = None,
        model_info_path: str | None = None,
        ai_gen: bool = True,
        ai_check: bool = False,
        buffer_size: int = 8,
        max_retries: int = 3,
    ):
        self.ai_gen = ai_gen
        self.ai_check = ai_check
        self.buffer_size = buffer_size
        self.max_retries = max_retries
        
        if ai_check:
            self.models_check_buffer = []
            self.datasets_check_buffer = []

        curr_path = Path(__file__)
        if dataset_info_path:
            dataset_info_path = Path(dataset_info_path)
        else:
            dataset_info_path = curr_path.parents[3] / 'config/dataset-info.json'
        if model_info_path:
            model_info_path = Path(model_info_path)
        else:
            model_info_path = curr_path.parents[3] / 'config/model-info.json'
        self.model_info_path = model_info_path
        self.dataset_info_path = dataset_info_path
        self.model_infos = self._init_info(model_info_path)
        self.dataset_infos = self._init_info(dataset_info_path)
        self.models_buffer = []
        self.models_buffer_counter = defaultdict(int)
        self.datasets_buffer = []
        self.datasets_buffer_counter = defaultdict(int)
        
    def _init_info(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            info = json.load(f)
        return info
        
    def parse_input(self, input_data: PipelineData | None = None):
        self.required_keys = [
            'repo', 'downloads_last_month', 'likes', 'community', 'date_crawl',
            'link', 'img_path', 'error_msg', 'metadata'
        ]
        if 'model_name' in input_data.data.keys():
            self.category = 'models'
            self.required_keys += [
                'repo_org_mapper', 'model_name', 'descendants'
            ]
        elif 'dataset_name' in input_data.data.keys():
            self.category = 'datasets'
            self.required_keys += [
                'repo_org_mapper', 'dataset_name', 'dataset_usage'
            ]
        else:
            raise KeyError('input_data.data must contains model_name or dataset_name.')
        self.data = input_data.data.copy()
        self.input = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            self.input[k] = self.data.pop(k)
        
    def _process_model(self, inp: dict) -> Optional[PipelineData]:
        try:
            model_name = inp['model_name']
            repo = inp['repo']
            org = inp['repo_org_mapper'][repo]
            org = inp['repo_org_mapper'].get(repo, None)
            if org is None:
                return None
            model_key = f'{repo}/{model_name}'
            model_info = self.model_infos.get(model_key, None)
            if model_info:
                modality = model_info['modality']
                is_large_model = model_info['is_large_model']
            else:
                if self.models_buffer_counter[model_key] <= self.max_retries:
                    self.models_buffer.append(inp.copy())
                    self.models_buffer_counter[model_key] += 1
                is_large_model = False
            if is_large_model:
                downloads_last_month = inp['downloads_last_month']
                img_path = inp['img_path']
                if downloads_last_month == 0 and self.ai_check and img_path and Path(img_path).exists():
                    request = CheckRequest(img_path, inp['link'], 'HuggingFace')
                    response = check_image_info([request])[0]
                    if response.downloads_last_month is not None and response.downloads_last_month > 0:
                        downloads_last_month = response.downloads_last_month
                        logger.warning(f"Data error: {inp}, downloads_last_month corrected from {inp['downloads_last_month']} to {downloads_last_month}.")
                        inp['downloads_last_month'] = response.downloads_last_month
                        self.models_check_buffer.append(inp)
                if downloads_last_month < 50:
                    return None
                return PipelineData({
                    "org": org,
                    "repo": repo,
                    "model_name": model_name,
                    "modality": modality,
                    "downloads_last_month": downloads_last_month,
                    "likes": inp['likes'],
                    "community": inp['community'],
                    "descendants": inp['descendants'],
                    "date_crawl": inp['date_crawl'],
                    "link": inp['link'],
                    "source": "HuggingFace",
                    "img_path": img_path
                }, {
                    "org": org,
                    "model_name": model_name,
                    "modality": modality,
                }, None)
            else: 
                return None
        except Exception:
            raise
        
    def _process_dataset(self, inp: dict) -> Optional[PipelineData]:
        try:
            dataset_name = inp['dataset_name']
            repo = inp['repo']
            org = inp['repo_org_mapper'].get(repo, None)
            if org is None:
                return None
            dataset_key = f'{repo}/{dataset_name}'
            dataset_info = self.dataset_infos.get(dataset_key, None)
            if dataset_info:
                modality = dataset_info['modality']
                lifecycle = dataset_info['lifecycle']
                is_valid = dataset_info['is_valid']
            else:
                if self.datasets_buffer_counter[dataset_key] <= self.max_retries:
                    self.datasets_buffer.append(inp.copy())
                    self.datasets_buffer_counter[dataset_key] += 1
                is_valid = False
            if is_valid:
                downloads_last_month = inp['downloads_last_month']
                img_path = inp['img_path']
                if downloads_last_month == 0 and self.ai_check and img_path and Path(img_path).exists():
                    request = CheckRequest(img_path, inp['link'], 'HuggingFace')
                    response = check_image_info([request])[0]
                    if response.downloads_last_month is not None and response.downloads_last_month > 0:
                        downloads_last_month = response.downloads_last_month
                        logger.warning(f"Data error: {inp}, downloads_last_month corrected from {inp['downloads_last_month']} to {downloads_last_month}.")
                        inp['downloads_last_month'] = response.downloads_last_month
                        self.datasets_check_buffer.append(inp)
                return PipelineData({
                    "org": org,
                    "repo": repo,
                    "dataset_name": dataset_name,
                    "modality": modality,
                    "lifecycle": lifecycle,
                    "downloads_last_month": downloads_last_month,
                    "likes": inp['likes'],
                    "community": inp['community'],
                    "dataset_usage": inp['dataset_usage'],
                    "date_crawl": inp['date_crawl'],
                    "link": inp['link'],
                    "source": "HuggingFace",
                    "img_path": img_path
                }, {
                    "org": org,
                    "dataset_name": dataset_name,
                    "modality": modality,
                    "lifecycle": lifecycle
                }, None)
            else:
                return None
        except Exception:
            raise
        
    def _gen_new_info(self, infos: list[ModelInfo | DatasetInfo]):
        res = {}
        for info in infos:
            link = info.link
            name = link.rstrip('/').split('/')[-1]
            repo = link.rstrip('/').split('/')[-2]
            key = f"{repo}/{name}"
            if isinstance(info, ModelInfo):
                res[key] = {
                    'modality': info.modality,
                    'is_large_model': info.is_large_model
                }
            elif isinstance(info, DatasetInfo):
                res[key] = {
                    'modality': info.modality,
                    'lifecycle': info.lifecycle,
                    'is_valid': info.is_valid
                }
        return res
    
    def update_model_info(self):
        with open(self.model_info_path, 'w') as f:
            json.dump(self.model_infos, f, indent=4, ensure_ascii=False)
            
    def update_dataset_info(self):
        with open(self.dataset_info_path, 'w') as f:
            json.dump(self.dataset_infos, f, indent=4, ensure_ascii=False)
            
    def run(self) -> PipelineResult:
        try:
            if self.category == 'models':
                data = self._process_model(self.input)
            elif self.category == 'datasets':
                data = self._process_dataset(self.input)
            else:
                raise ValueError('category must be models or datasets.')
            if data:
                data.data.update(self.data)
                yield data
                    
        except Exception as e:
            logger.opt(exception=e).error(f"HFInfoProcessor Error with input: {self.input}")
            error_msg = traceback.format_exc()
            yield PipelineData(None, None, {
                "type": type(e),
                "error_msg": error_msg
            })

        if len(self.models_buffer) >= self.buffer_size:
            urls = [inp['link'] for inp in self.models_buffer]
            model_infos = gen_model_info_huggingface(urls)
            model_infos = self._gen_new_info(model_infos)
            logger.info(f"Generate model informations:\n{json.dumps(model_infos, indent=2, ensure_ascii=False)}")
            self.model_infos.update(model_infos)
            self.update_model_info()
            inps = self.models_buffer.copy()
            self.models_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_model(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"HFInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })
        
        if len(self.datasets_buffer) >= self.buffer_size:
            urls = [inp['link'] for inp in self.datasets_buffer]
            dataset_infos = gen_dataset_info_huggingface(urls)
            dataset_infos = self._gen_new_info(dataset_infos)
            logger.info(f"Generate dataset informations:\n{json.dumps(dataset_infos, indent=2, ensure_ascii=False)}")
            self.dataset_infos.update(dataset_infos)
            self.update_dataset_info()
            inps = self.datasets_buffer.copy()
            self.datasets_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_dataset(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"HFInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })
            
    def flush(self, update_infos: bool = True) -> Optional[PipelineResult]:
        if len(self.models_buffer) > 0:
            urls = [inp['link'] for inp in self.models_buffer]
            model_infos = gen_model_info_huggingface(urls)
            model_infos = self._gen_new_info(model_infos)
            logger.info(f"Generate model informations:\n{json.dumps(model_infos, indent=2, ensure_ascii=False)}")
            self.model_infos.update(model_infos)
            self.update_model_info()
            inps = self.models_buffer.copy()
            self.models_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_model(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"HFInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })
        
        if len(self.datasets_buffer) > 0:
            urls = [inp['link'] for inp in self.datasets_buffer]
            dataset_infos = gen_dataset_info_huggingface(urls)
            dataset_infos = self._gen_new_info(dataset_infos)
            logger.info(f"Generate dataset informations:\n{json.dumps(dataset_infos, indent=2, ensure_ascii=False)}")
            self.dataset_infos.update(dataset_infos)
            self.update_dataset_info()
            inps = self.datasets_buffer.copy()
            self.datasets_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_dataset(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"HFInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })


class MSInfoProcessor(PipelineStep):
    
    ptype = "🚗 PROCESSOR"
    required_keys = ["repo_org_mapper"]
    
    def __init__(
        self,
        history_data_path: str | None = None,
        dataset_info_path: str | None = None,
        model_info_path: str | None = None,
        ai_gen: bool = True,
        ai_check: bool = False,
        buffer_size: int = 8,
        max_retries: int = 3,
    ):
        self.ai_gen = ai_gen
        self.ai_check = ai_check
        self.buffer_size = buffer_size
        self.max_retries = max_retries
        
        if ai_check:
            self.models_check_buffer = []
            self.datasets_check_buffer = []

        curr_path = Path(__file__)
        if history_data_path:
            history_data_path = Path(history_data_path)
        else:
            history_data_path = curr_path.parents[3] / 'data'
        if dataset_info_path:
            dataset_info_path = Path(dataset_info_path)
        else:
            dataset_info_path = curr_path.parents[3] / 'config/dataset-info.json'
        if model_info_path:
            model_info_path = Path(model_info_path)
        else:
            model_info_path = curr_path.parents[3] / 'config/model-info.json'
        self.history_data_path = {}
        for p in history_data_path.glob("????-??-??"):
            self.history_data_path[p.name] = p
        self.model_info_path = model_info_path
        self.dataset_info_path = dataset_info_path
        self.model_infos = self._init_info(model_info_path)
        self.dataset_infos = self._init_info(dataset_info_path)
        self.models_buffer = []
        self.models_buffer_counter = defaultdict(int)
        self.datasets_buffer = []
        self.datasets_buffer_counter = defaultdict(int)
        self.last_month_downloads_of = {}
        
    def _init_info(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            info = json.load(f)
        return info
    
    def _get_last_month_downloads_of(self, date_crawl: str) -> dict[str, int]:
        date_crawl = datetime.strptime(date_crawl, r"%Y-%m-%d")
        last_month_date = date_crawl - timedelta(days=30)
        
        min_diff = None
        closest_date = None
        
        for date in self.history_data_path.keys():
            cur_date = datetime.strptime(date, r"%Y-%m-%d")
            diff = abs((cur_date - last_month_date).days)
            if min_diff is None or diff < min_diff:
                min_diff = diff
                closest_date = date
        
        last_month_downloads = {}
        if min_diff > 15:
            return last_month_downloads
        
        p = self.history_data_path[closest_date] / 'ModelScope/raw-models-info.jsonl'
        if p.exists():
            with jsonlines.open(p, 'r') as f:
                for item in f:
                    key = f"{item['repo']}/{item['model_name']}"
                    last_month_downloads[key] = item['total_downloads']
        p = self.history_data_path[closest_date] / 'ModelScope/raw-datasets-info.jsonl'
        if p.exists():
            with jsonlines.open(p, 'r') as f:
                for item in f:
                    key = f"{item['repo']}/{item['dataset_name']}"
                    last_month_downloads[key] = item['total_downloads']

        return last_month_downloads
    
    def parse_input(self, input_data: PipelineData | None = None):
        self.required_keys = [
            'repo', 'total_downloads', 'likes', 'community', 'date_crawl',
            'link', 'img_path', 'error_msg', 'metadata'
        ]
        if 'model_name' in input_data.data.keys():
            self.category = 'models'
            self.required_keys.extend(['model_name', 'repo_org_mapper'])
        elif 'dataset_name' in input_data.data.keys():
            self.category = 'datasets'
            self.required_keys.extend(['repo_org_mapper', 'dataset_name'])
        else:
            raise KeyError('input_data.data must contains model_name or dataset_name.')
        self.data = input_data.data.copy()
        self.input = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            self.input[k] = self.data.pop(k)
        
        date_crawl = self.input['date_crawl']
        if date_crawl not in self.last_month_downloads_of:
            self.last_month_downloads_of[date_crawl] = self._get_last_month_downloads_of(date_crawl)
            
    def _process_model(self, inp: dict) -> Optional[PipelineData]:
        try:
            model_name = inp['model_name']
            repo = inp['repo']
            org = inp['repo_org_mapper'].get(repo, None)
            if org is None:
                return None
            model_key = f'{repo}/{model_name}'
            date_crawl = inp['date_crawl']
            last_month_downloads = self.last_month_downloads_of[date_crawl].get(model_key, None)
            if last_month_downloads is None:
                is_large_model = False
            else:
                model_info = self.model_infos.get(model_key, None)
                if model_info:
                    modality = model_info['modality']
                    is_large_model = model_info['is_large_model']
                else: 
                    if self.models_buffer_counter[model_key] <= self.max_retries:
                        self.models_buffer.append(inp.copy())
                        self.models_buffer_counter[model_key] += 1
                    is_large_model = False

            if is_large_model:
                downloads = inp['total_downloads']
                img_path = inp['img_path']
                if downloads == 0 and self.ai_check and img_path and Path(img_path).exists():
                    request = CheckRequest(img_path, inp['link'], 'ModelScope')
                    response = check_image_info([request])[0]
                    if response.downloads is not None and response.downloads > 0:
                        downloads = response.downloads
                        logger.warning(f"Data error: {inp}, downloads corrected from {inp['total_downloads']} to {downloads}.")
                        inp['total_downloads'] = response.downloads
                        self.models_check_buffer.append(inp)
                downloads_last_month = downloads - last_month_downloads
                if downloads_last_month < 50:
                    return None
                return PipelineData({
                    "org": org,
                    "repo": repo,
                    "model_name": model_name,
                    "modality": modality,
                    "downloads_last_month": downloads_last_month,
                    "total_downloads": downloads,
                    "likes": inp['likes'],
                    "community": inp['community'],
                    "date_crawl": date_crawl,
                    "link": inp['link'],
                    "source": "ModelScope",
                    "img_path": img_path
                }, {
                    "org": org,
                    "model_name": model_name,
                    "modality": modality
                }, None)
            else: 
                return None
        except Exception:
            raise
        
    def _process_dataset(self, inp: dict) -> Optional[PipelineData]:
        try:
            dataset_name = inp['dataset_name']
            repo = inp['repo']
            org = inp['repo_org_mapper'].get(repo, None)
            if org is None:
                return None
            dataset_key = f"{repo}/{dataset_name}"
            date_crawl = inp['date_crawl']
            last_month_downloads = self.last_month_downloads_of[date_crawl].get(dataset_key, None)
            
            dataset_info = self.dataset_infos.get(dataset_key, None)
            if dataset_info:
                modality = dataset_info['modality']
                lifecycle = dataset_info['lifecycle']
                is_valid = dataset_info['is_valid']
            else:
                if self.datasets_buffer_counter[dataset_key] <= self.max_retries:
                    self.datasets_buffer.append(inp.copy())
                    self.datasets_buffer_counter[dataset_key] += 1
                is_valid = False
            
            if is_valid:
                downloads = inp['total_downloads']
                img_path = inp['img_path']
                if downloads == 0 and self.ai_check and img_path and Path(img_path).exists():
                    request = CheckRequest(img_path, inp['link'], 'ModelScope')
                    response = check_image_info([request])[0]
                    if response.downloads is not None and response.downloads > 0:
                        downloads = response.downloads
                        logger.warning(f"Data error: {inp}, downloads corrected from {inp['total_downloads']} to {downloads}.")
                        inp['total_downloads'] = response.downloads
                        self.datasets_check_buffer.append(inp)
                if last_month_downloads:
                    downloads_last_month = downloads - last_month_downloads
                else:
                    downloads_last_month = 0
                return PipelineData({
                    "org": org,
                    "repo": repo,
                    "dataset_name": dataset_name,
                    "modality": modality,
                    "lifecycle": lifecycle,
                    "downloads_last_month": downloads_last_month,
                    "total_downloads": inp['total_downloads'],
                    "likes": inp['likes'], 
                    "community": inp['community'],
                    "date_crawl": date_crawl,
                    "link": inp['link'],
                    "source": "ModelScope",
                    "img_path": inp['img_path']
                }, {
                    "org": org,
                    "dataset_name": dataset_name,
                    "modality": modality,
                    "lifecycle": lifecycle
                }, None)
            else:
                return None
        except Exception:
            raise
        
    def _gen_new_info(self, infos: list[ModelInfo | DatasetInfo]):
        res = {}
        for info in infos:
            link = info.link
            name = link.rstrip('/').split('/')[-1]
            repo = link.rstrip('/').split('/')[-2]
            key = f"{repo}/{name}"
            if isinstance(info, ModelInfo):
                res[key] = {
                    'modality': info.modality,
                    'is_large_model': info.is_large_model
                }
            elif isinstance(info, DatasetInfo):
                res[key] = {
                    'modality': info.modality,
                    'lifecycle': info.lifecycle,
                    'is_valid': info.is_valid
                }
        return res
    
    def update_model_info(self):
        with open(self.model_info_path, 'w') as f:
            json.dump(self.model_infos, f, indent=4, ensure_ascii=False)
    
    def update_dataset_info(self):
        with open(self.dataset_info_path, 'w') as f:
            json.dump(self.dataset_infos, f, indent=4, ensure_ascii=False)
    
    def run(self) -> PipelineResult:
        try:
            if self.category == 'models':
                data = self._process_model(self.input)
            elif self.category == 'datasets':
                data = self._process_dataset(self.input)
            else:
                raise ValueError('category must be models or datasets.')
            if data:
                data.data.update(self.data)
                yield data
        
        except Exception as e:
            logger.opt(exception=e).error(f"MSInfoProcessor Error with input: {self.input}")
            error_msg = traceback.format_exc()
            yield PipelineData(None, None, {
                "type": type(e),
                "error_msg": error_msg
            })
            
        if len(self.models_buffer) >= self.buffer_size:
            urls = [inp['link'] for inp in self.models_buffer]
            model_infos = gen_model_info_modelscope(urls)
            model_infos = self._gen_new_info(model_infos)
            logger.info(f"Generate model informations:\n{json.dumps(model_infos, indent=2, ensure_ascii=False)}")
            self.model_infos.update(model_infos)
            self.update_model_info()
            inps = self.models_buffer.copy()
            self.models_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_model(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"MSInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })
        
        if len(self.datasets_buffer) >= self.buffer_size:
            urls = [inp['link'] for inp in self.datasets_buffer]
            dataset_infos = gen_dataset_info_modelscope(urls)
            dataset_infos = self._gen_new_info(dataset_infos)
            logger.info(f"Generate dataset informations:\n{json.dumps(dataset_infos, indent=2, ensure_ascii=False)}")
            self.dataset_infos.update(dataset_infos)
            self.update_dataset_info()
            inps = self.datasets_buffer.copy()
            self.datasets_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_dataset(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"MSInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })
            
    def flush(self, update_infos: bool = True) -> Optional[PipelineResult]:
        if len(self.models_buffer) > 0:
            urls = [inp['link'] for inp in self.models_buffer]
            model_infos = gen_model_info_modelscope(urls)
            model_infos = self._gen_new_info(model_infos)
            logger.info(f"Generate model informations:\n{json.dumps(model_infos, indent=2, ensure_ascii=False)}")
            self.model_infos.update(model_infos)
            self.update_model_info()
            inps = self.models_buffer.copy()
            self.models_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_model(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"MSInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })
        
        if len(self.datasets_buffer) > 0:
            urls = [inp['link'] for inp in self.datasets_buffer]
            dataset_infos = gen_dataset_info_modelscope(urls)
            dataset_infos = self._gen_new_info(dataset_infos)
            logger.info(f"Generate dataset informations:\n{json.dumps(dataset_infos, indent=2, ensure_ascii=False)}")
            self.dataset_infos.update(dataset_infos)
            self.update_dataset_info()
            inps = self.datasets_buffer.copy()
            self.datasets_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_dataset(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"MSInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })

    
class OpenDataLabInfoProcessor(PipelineStep):
    
    ptype = "🚗 PROCESSOR"
    required_keys = [
        'org', 'repo', 'dataset_name', 'total_downloads', 'likes', 'date_crawl',
        'link', 'metadata'
    ]
    
    def __init__(
        self,
        history_data_path: str | None = None,
        dataset_info_path: str | None = None,
        ai_gen: bool = True,
        buffer_size: int = 8,
        max_retries: int = 3,
    ):
        self.ai_gen = ai_gen
        self.buffer_size = buffer_size
        self.max_retries = max_retries

        curr_path = Path(__file__)
        if history_data_path:
            history_data_path = Path(history_data_path)
        else:
            history_data_path = curr_path.parents[3] / 'data'
        if dataset_info_path:
            dataset_info_path = Path(dataset_info_path)
        else:
            dataset_info_path = curr_path.parents[3] / 'config/dataset-info.json'
        self.history_data_path = {}
        for p in history_data_path.glob("????-??-??"):
            self.history_data_path[p.name] = p
        self.dataset_info_path = dataset_info_path
        self.dataset_infos = self._init_info(dataset_info_path)
        self.datasets_buffer = []
        self.datasets_buffer_counter = defaultdict(int)
        self.last_month_downloads_of = {}
        
    def _init_info(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            info = json.load(f)
        return info
    
    def _get_last_month_downloads_of(self, date_crawl: str) -> dict[str, int]:
        date_crawl = datetime.strptime(date_crawl, r"%Y-%m-%d")
        last_month_date = date_crawl - timedelta(days=30)
        
        min_diff = None
        closest_date = None
        
        for date in self.history_data_path.keys():
            cur_date = datetime.strptime(date, r"%Y-%m-%d")
            diff = abs((cur_date - last_month_date).days)
            if min_diff is None or diff < min_diff:
                min_diff = diff
                closest_date = date
        
        last_month_downloads = {}
        if min_diff > 15:
            return last_month_downloads
        
        p = self.history_data_path[closest_date] / 'OpenDataLab/raw-datasets-info.jsonl'
        if p.exists():
            with jsonlines.open(p, 'r') as f:
                for item in f:
                    key = f"{item['repo']}/{item['dataset_name']}"
                    last_month_downloads[key] = item['total_downloads']
        
        return last_month_downloads
    
    def parse_input(self, input_data: PipelineData | None = None):
        self.data = input_data.data.copy()
        self.input = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            self.input[k] = self.data.pop(k)
        
        date_crawl = self.input['date_crawl']
        if date_crawl not in self.last_month_downloads_of:
            self.last_month_downloads_of[date_crawl] = self._get_last_month_downloads_of(date_crawl)
            
    def _process_dataset(self, inp: dict) -> Optional[PipelineData]:
        try:
            dataset_name = inp['dataset_name']
            repo = inp['repo']
            org = inp['org']
            dataset_key = f"{repo}/{dataset_name}"
            date_crawl = inp['date_crawl']
            last_month_downloads = self.last_month_downloads_of[date_crawl].get(dataset_key, None)
            if last_month_downloads is None:
                is_valid = False
            else:
                dataset_info = self.dataset_infos.get(dataset_key, None)
                if dataset_info:
                    modality = dataset_info['modality']
                    lifecycle = dataset_info['lifecycle']
                    is_valid = dataset_info['is_valid']
                else:
                    if self.datasets_buffer_counter[dataset_key] <= self.max_retries:
                        self.datasets_buffer.append(inp.copy())
                        self.datasets_buffer_counter[dataset_key] += 1
                    is_valid = False
            
            if is_valid:
                downloads_last_month = inp['total_downloads'] - last_month_downloads
                if downloads_last_month < 0:
                    return None
                return PipelineData({
                    "org": org,
                    "repo": repo,
                    "dataset_name": dataset_name,
                    "modality": modality,
                    "lifecycle": lifecycle,
                    "downloads_last_month": downloads_last_month,
                    "total_downloads": inp['total_downloads'],
                    "likes": inp['likes'], 
                    "date_crawl": date_crawl,
                    "link": inp['link'],
                    "source": "OpenDataLab",
                }, {
                    "org": org,
                    "dataset_name": dataset_name,
                    "modality": modality,
                    "lifecycle": lifecycle
                }, None)
            else:
                return None
        except Exception:
            raise
        
    def _gen_new_info(self, infos: list[DatasetInfo]):
        res = {}
        for info in infos:
            link = info.link
            name = link.rstrip('/').split('/')[-1]
            repo = link.rstrip('/').split('/')[-2]
            key = f"{repo}/{name}"
            if isinstance(info, DatasetInfo):
                res[key] = {
                    'modality': info.modality,
                    'lifecycle': info.lifecycle,
                    'is_valid': info.is_valid
                }
        return res
    
    def update_dataset_info(self):
        with open(self.dataset_info_path, 'w') as f:
            json.dump(self.dataset_infos, f, indent=4, ensure_ascii=False)
    
    def run(self) -> PipelineResult:
        try:
            data = self._process_dataset(self.input)
            if data:
                data.data.update(self.data)
                yield data
        
        except Exception as e:
            logger.opt(exception=e).error(f"OpenDataLabInfoProcessor Error with input: {self.input}")
            error_msg = traceback.format_exc()
            yield PipelineData(None, None, {
                "type": type(e),
                "error_msg": error_msg
            })
            
        if len(self.datasets_buffer) >= self.buffer_size:
            urls = [inp['link'] for inp in self.datasets_buffer]
            dataset_infos = gen_dataset_info_huggingface(urls)
            dataset_infos = self._gen_new_info(dataset_infos)
            logger.info(f"Generate dataset informations:\n{json.dumps(dataset_infos, indent=2, ensure_ascii=False)}")
            self.dataset_infos.update(dataset_infos)
            self.update_dataset_info()
            inps = self.datasets_buffer.copy()
            self.datasets_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_dataset(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"OpenDataLabInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })
            
    def flush(self, update_infos: bool = True) -> Optional[PipelineResult]:
        if len(self.datasets_buffer) > 0:
            urls = [inp['link'] for inp in self.datasets_buffer]
            dataset_infos = gen_dataset_info_huggingface(urls)
            dataset_infos = self._gen_new_info(dataset_infos)
            logger.info(f"Generate dataset informations:\n{json.dumps(dataset_infos, indent=2, ensure_ascii=False)}")
            self.dataset_infos.update(dataset_infos)
            self.update_dataset_info()
            inps = self.datasets_buffer.copy()
            self.datasets_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_dataset(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"OpenDataLabInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })


class BAAIDataInfoProcessor(PipelineStep):
    
    ptype = "🚗 PROCESSOR"
    required_keys = [
        'org', 'repo', 'dataset_name', 'total_downloads', 'likes', 'date_crawl',
        'link'
    ]
    
    def __init__(
        self,
        history_data_path: str | None = None,
        dataset_info_path: str | None = None,
        ai_gen: bool = True,
        buffer_size: int = 8,
        max_retries: int = 3,
    ):
        self.ai_gen = ai_gen
        self.buffer_size = buffer_size
        self.max_retries = max_retries

        curr_path = Path(__file__)
        if history_data_path:
            history_data_path = Path(history_data_path)
        else:
            history_data_path = curr_path.parents[3] / 'data'
        if dataset_info_path:
            dataset_info_path = Path(dataset_info_path)
        else:
            dataset_info_path = curr_path.parents[3] / 'config/dataset-info.json'
        self.history_data_path = {}
        for p in history_data_path.glob("????-??-??"):
            self.history_data_path[p.name] = p
        self.dataset_info_path = dataset_info_path
        self.dataset_infos = self._init_info(dataset_info_path)
        self.datasets_buffer = []
        self.datasets_buffer_counter = defaultdict(int)
        self.last_month_downloads_of = {}
        
    def _init_info(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            info = json.load(f)
        return info
    
    def _get_last_month_downloads_of(self, date_crawl: str) -> dict[str, int]:
        date_crawl = datetime.strptime(date_crawl, r"%Y-%m-%d")
        last_month_date = date_crawl - timedelta(days=30)
        
        min_diff = None
        closest_date = None
        
        for date in self.history_data_path.keys():
            cur_date = datetime.strptime(date, r"%Y-%m-%d")
            diff = abs((cur_date - last_month_date).days)
            if min_diff is None or diff < min_diff:
                min_diff = diff
                closest_date = date
        
        last_month_downloads = {}
        if min_diff > 15:
            return last_month_downloads
        
        p = self.history_data_path[closest_date] / 'BAAIData/raw-datasets-info.jsonl'
        if p.exists():
            with jsonlines.open(p, 'r') as f:
                for item in f:
                    key = f"{item['repo']}/{item['dataset_name']}"
                    last_month_downloads[key] = item['total_downloads']
        
        return last_month_downloads
    
    def parse_input(self, input_data: PipelineData | None = None):
        self.data = input_data.data.copy()
        self.input = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            self.input[k] = self.data.pop(k)
        
        date_crawl = self.input['date_crawl']
        if date_crawl not in self.last_month_downloads_of:
            self.last_month_downloads_of[date_crawl] = self._get_last_month_downloads_of(date_crawl)
            
    def _process_dataset(self, inp: dict) -> Optional[PipelineData]:
        try:
            dataset_name = inp['dataset_name']
            repo = inp['repo']
            org = inp['org']
            dataset_key = f"{repo}/{dataset_name}"
            date_crawl = inp['date_crawl']
            last_month_downloads = self.last_month_downloads_of[date_crawl].get(dataset_key, None)
            if last_month_downloads is None:
                is_valid = False
            else:
                dataset_info = self.dataset_infos.get(dataset_key, None)
                if dataset_info:
                    modality = dataset_info['modality']
                    lifecycle = dataset_info['lifecycle']
                    is_valid = dataset_info['is_valid']
                else:
                    if self.datasets_buffer_counter[dataset_key] <= self.max_retries:
                        self.datasets_buffer.append(inp.copy())
                        self.datasets_buffer_counter[dataset_key] += 1
                    is_valid = False
            
            if is_valid:
                downloads_last_month = inp['total_downloads'] - last_month_downloads
                if downloads_last_month < 0:
                    return None
                return PipelineData({
                    "org": org,
                    "repo": repo,
                    "dataset_name": dataset_name,
                    "modality": modality,
                    "lifecycle": lifecycle,
                    "downloads_last_month": downloads_last_month,
                    "total_downloads": inp['total_downloads'],
                    "likes": inp['likes'], 
                    "date_crawl": date_crawl,
                    "link": inp['link'],
                    "source": "BAAIData",
                }, {
                    "org": org,
                    "dataset_name": dataset_name,
                    "modality": modality,
                    "lifecycle": lifecycle
                }, None)
            else:
                return None
        except Exception:
            raise
        
    def _gen_new_info(self, infos: list[DatasetInfo]):
        res = {}
        for info in infos:
            link = info.link
            name = link.rstrip('/').split('/')[-1]
            repo = link.rstrip('/').split('/')[-2]
            key = f"{repo}/{name}"
            if isinstance(info, DatasetInfo):
                res[key] = {
                    'modality': info.modality,
                    'lifecycle': info.lifecycle,
                    'is_valid': info.is_valid
                }
        return res
    
    def run(self) -> PipelineResult:
        try:
            data = self._process_dataset(self.input)
            if data:
                data.data.update(self.data)
                yield data
        
        except Exception as e:
            logger.opt(exception=e).error(f"BAAIDataInfoProcessor Error with input: {self.input}")
            error_msg = traceback.format_exc()
            yield PipelineData(None, None, {
                "type": type(e),
                "error_msg": error_msg
            })
            
        if len(self.datasets_buffer) >= self.buffer_size:
            urls = [inp['link'] for inp in self.datasets_buffer]
            dataset_infos = gen_dataset_info_huggingface(urls)
            dataset_infos = self._gen_new_info(dataset_infos)
            logger.info(f"Generate dataset informations:\n{json.dumps(dataset_infos, indent=2, ensure_ascii=False)}")
            self.dataset_infos.update(dataset_infos)
            inps = self.datasets_buffer.copy()
            self.datasets_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_dataset(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"BAAIDataInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })
            
    def flush(self, update_infos: bool = True) -> Optional[PipelineResult]:
        if len(self.datasets_buffer) > 0:
            urls = [inp['link'] for inp in self.datasets_buffer]
            dataset_infos = gen_dataset_info_huggingface(urls)
            dataset_infos = self._gen_new_info(dataset_infos)
            logger.info(f"Generate dataset informations:\n{json.dumps(dataset_infos, indent=2, ensure_ascii=False)}")
            self.dataset_infos.update(dataset_infos)
            inps = self.datasets_buffer.copy()
            self.datasets_buffer.clear()
            for inp in inps:
                try:
                    data = self._process_dataset(inp)
                    if data:
                        data.data.update(self.data)
                        yield data
                except Exception as e:
                    logger.opt(exception=e).error(f"BAAIDataInfoProcessor Error with input: {inp}")
                    error_msg = traceback.format_exc()
                    yield PipelineData(None, None, {
                        "type": type(e),
                        "error_msg": error_msg
                    })

        if update_infos:
            with open(self.dataset_info_path, 'w') as f:
                json.dump(self.dataset_infos, f, indent=4, ensure_ascii=False)
    
@deprecated("MultiSourceInfoMerge PipelineStep is deprecated. Use MultiSourceInfoMergeExecutor instead.")
class MultiSourceInfoMerge(PipelineStep):
    
    ptype = "🚗 PROCESSOR"
    required_keys = ['org', 'repo', 'modality', 'downloads_last_month', 
                     'likes', 'date_crawl', 'source']
    
    def __init__(
        self,
        category: Literal['datasets', 'models'] | None = None
    ):
        if category == 'datasets' or category is None:
            self.datasets_buffer = defaultdict(list)
        if category == 'models' or category is None:
            self.models_buffer = defaultdict(list) 
        
    def parse_input(self, input_data: PipelineData | None = None):
        self.required_keys = [
            'org', 'repo', 'modality', 'downloads_last_month', 
            'likes', 'date_crawl', 'source'
        ]
        assert 'model_name' in input_data.data.keys() or 'dataset_name' in input_data.data.keys()
        assert 'source' in input_data.data.keys()
        if 'model_name' in input_data.data.keys():
            self.category = 'models'
            assert hasattr(self, "models_buffer")
        else:
            self.category = 'datasets'
            assert hasattr(self, "datasets_buffer")
        match (input_data.data['source'], self.category):
            case ('HuggingFace', 'models'):
                self.required_keys.extend([
                    'model_name', 'community', 'descendants'
                ])
            case ('HuggingFace', 'datasets'):
                self.required_keys.extend([
                    'dataset_name', 'lifecycle', 'community', 'dataset_usage'
                ])
            case ('ModelScope', 'models'):
                self.required_keys.extend([
                    'model_name', 'community', 'community'
                ])
            case ('ModelScope', 'datasets'):
                self.required_keys.extend([
                    'dataset_name', 'lifecycle', 'community'
                ])
            case ('OpenDataLab', 'datasets'):
                self.required_keys.extend([
                    'dataset_name', 'lifecycle'
                ])
            case ('BAAIData', 'datasets'):
                self.required_keys.extend([
                    'dataset_name', 'lifecycle'
                ])
            case _:
                logger.error(f"Unknown source-type pair {input_data.data['source']}-{self.category}")
        
        self.data = input_data.data.copy()
        self.input = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            self.input[k] = self.data.pop(k)
    
    def run(self) -> PipelineResult:
        try:
            if self.category == 'models':
                name = self.input['model_name']
                key = f"{self.input['repo']}/{name}"
                self.models_buffer[key].append(self.input)
                data = self.input.copy()
                data.update(self.data)
                yield PipelineData({data, None, None})
            else:
                name = self.input['dataset_name']
                key = f"{self.input['repo']}/{name}"
                self.datasets_buffer[key].append(self.input)
                data = self.input.copy()
                data.update(self.data)
                yield PipelineData({data, None, None})
            
        except Exception as e:
            yield PipelineData(None, None, {
                "type": type(e),
                "error_msg": traceback.format_exc()
            })
            
    def flush(self) -> PipelineResult:
        try:
            model_records = []
            dataset_records = []
            if hasattr(self, "models_buffer"):
                for _, models in self.models_buffer.items():
                    item = {
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
                    }
                    model_records.append(item)
            if hasattr(self, "datasets_buffer"):
                for _, datasets in self.datasets_buffer.items():
                    item = {
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
                    }
                    dataset_records.append(item)
            yield PipelineData({
                'model_records': model_records,
                'dataset_records': dataset_records,
            }, {
                'total_model_records': len(model_records),
                'total_dataset_records': len(dataset_records),
            }, None)
        except Exception as e:
            yield PipelineData(None, None, {
                "type": type(e),
                "error_msg": traceback.format_exc()
            })
