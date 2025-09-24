import sys
import yaml
import argparse
from datetime import datetime
from pathlib import Path
from typing_extensions import deprecated
from .core import AccumulateAndRankingPipeline, BAAIDataPipeline, HFPipeline, MSPipeline, MergeAndRankingPipeline, OpenDataLabPipeline


def main() -> None:
    print("Hello from oslm-crawler!")
    parser = get_parser()
    args = parser.parse_args()
    config = init_config(args)
    
    if hasattr(args, 'func'):
        args.func(config)
    else:
        parser.print_help()


def init_config(args):
    if not hasattr(args, 'config'):
        args.config = Path(__file__).parents[2] / 'config/default_task.yaml'
    with open(args.config, 'r') as f:
        config: dict = yaml.safe_load(f)
    
    if args.command == 'crawl':
        match args.pipeline:
            case 'huggingface':
                pipelines = ["HuggingFacePipeline"]
            case 'modelscope':
                pipelines = ["ModelScopePipeline"]
            case 'opendatalab':
                pipelines = ["OpenDataLabPipeline"]
            case 'baaidata':
                pipelines = ["BAAIDataPipeline"]
            case 'all':
                pipelines = ["BAAIDataPipeline", "OpenDataLabPipeline", 
                             "ModelScopePipeline", "HuggingFacePipeline"]
        for k, v in config.items():
            if k in pipelines:
                if args.load_dir:
                    v['load_dir'] = args.load_dir
                if args.save_dir:
                    v['save_dir'] = args.save_dir
                if args.log_path:
                    v['log_path'] = args.log_path
                config[k] = v
    elif args.command == 'gen-rank':
        if args.data_dir:
            config['RankingPipeline']['data_dir'] = args.data_dir
        elif args.all:
            config['RankingPipeline']['data_dir'] = 'all'
    elif args.command == 'accumulate':
        if args.data_dir:
            config['RankingPipeline']['data_dir'] = args.data_dir
        elif args.all:
            config['RankingPipeline']['data_dir'] = 'all'
        
    return config
    

def get_parser():
    parser = argparse.ArgumentParser(prog="oslm-crawler", description="oslm-crawler")
    sub_parsers = parser.add_subparsers(dest='command', help='Available commands')
    
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--config", type=str, default=argparse.SUPPRESS, help="Path of the YAML configuration file.")

    crawl_parser = sub_parsers.add_parser("crawl", parents=[parent_parser], help="Crawl data from huggingface, modelscope, opendatalab, or baai")
    crawl_parser.add_argument("pipeline", choices=["huggingface", "modelscope", "opendatalab", "baaidata", "all"], help="Pipeline")
    crawl_parser.add_argument("--load-dir", help="When skipping the prerequisite steps, the data required for subsequent steps is loaded from this path. The default value is data/{today-date}/HuggingFace. When an error occurs and you need to rerun, you should manually specify to the error output directory.")
    crawl_parser.add_argument("--save-dir", help="Save directory for crawler results, default value is data/{today-date}/HuggingFace. The load_dir of post_process is different from other steps, it loads from save_dir by default.")
    crawl_parser.add_argument("--log-path", help=r"Log output directory, default value is logs/{task_name}-{datetime}")
    crawl_parser.set_defaults(func=crawl)

    gen_rank_parser = sub_parsers.add_parser("gen-rank", parents=[parent_parser], help="Merge data from different source and generate rank table.")
    group = gen_rank_parser.add_mutually_exclusive_group()
    group.add_argument("--all", action="store_true", help="Generate rankings for all data in the default path.")
    group.add_argument("--data-dir", help="Data directory, default value is the current day")
    gen_rank_parser.set_defaults(func=gen_rank)
    
    accumulate_parser = sub_parsers.add_parser("accumulate", parents=[parent_parser], help="Accumulate historical download data and generate rankings.")
    group = accumulate_parser.add_mutually_exclusive_group()
    group.add_argument("--all", action="store_true", help="Generate accumulated rankings for all data in the default path.")
    group.add_argument("--data-dir", help="Data directory, default value is the current day")
    accumulate_parser.set_defaults(func=accumulate)
    
    return parser


def crawl(config):
    if 'HuggingFacePipeline' in config:
        conf = config['HuggingFacePipeline']
        proc = HFPipeline(
            conf['task_name'],
            conf['load_dir'],
            conf['save_dir'],
            conf['log_path'],
        )
        if 'init_org_links' in conf:
            proc = proc.step('init_org_links', **conf['init_org_links'])
        if 'crawl_repo_page' in conf:
            proc = proc.step('crawl_repo_page' **conf['crawl_repo_page'])
        if 'crawl_detail_page' in conf:
            proc = proc.step('crawl_detail_page', **conf['crawl_detail_page'])
        if 'post_process' in conf:
            proc = proc.step('post_process', **conf['post_process'])
        proc.done()
    if 'ModelScopePipeline' in config:
        conf = config['ModelScopePipeline']
        proc = MSPipeline(
            conf['task_name'],
            conf['load_dir'],
            conf['save_dir'],
            conf['log_path'],
        )
        if 'init_org_links' in conf:
            proc = proc.step('init_org_links', **conf['init_org_links'])
        if 'crawl_repo_page' in conf:
            proc = proc.step('crawl_repo_page', **conf['crawl_repo_page'])
        if 'crawl_detail_page' in conf:
            proc = proc.step('crawl_detail_page', **conf['crawl_detail_page'])
        if 'post_process' in conf:
            proc = proc.step('post_process', **conf['post_process'])
        proc.done()
    if 'OpenDataLabPipeline' in config:
        conf = config['OpenDataLabPipeline']
        proc = OpenDataLabPipeline(
            conf['task_name'],
            conf['load_dir'],
            conf['save_dir'],
            conf['log_path'],
        )
        if 'init_org_links' in conf:
            proc = proc.step('init_org_links', **conf['init_org_links'])
        if 'crawl_repo_page' in conf:
            proc = proc.step('crawl_repo_page' **conf['crawl_repo_page'])
        if 'post_process' in conf:
            proc = proc.step('post_process', **conf['post_process'])
        proc.done()
    if 'BAAIDataPipeline' in config:
        conf = config['BAAIDataPipeline']
        proc = BAAIDataPipeline(
            conf['task_name'],
            conf['load_dir'],
            conf['save_dir'],
            conf['log_path'],
        )
        if 'init_org_links' in conf:
            proc = proc.step('init_org_links', **conf['init_org_links'])
        if 'crawl_repo_page' in conf:
            proc = proc.step('crawl_repo_page' **conf['crawl_repo_page'])
        if 'post_process' in conf:
            proc = proc.step('post_process', **conf['post_process'])
        proc.done()


def gen_rank(config):
    config = config['RankingPipeline']
    if config['data_dir'] == "all":
        data_dir_base = Path(__file__).parents[2] / 'data'
        log_path = Path(__file__).parents[2] / f'logs/rank-all-{datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")}'
        for data_dir in sorted(data_dir_base.glob(r"????-??-??"))[1:]:
            proc = MergeAndRankingPipeline(data_dir, log_path/f"{data_dir.name}.log")
            if 'merge_models' in config:
                proc = proc.step('merge_models', **config['merge_models'])
            if 'merge_datasets' in config:
                proc = proc.step('merge_datasets', **config['merge_datasets'])
            if 'ranking' in config:
                proc = proc.step('ranking', **config['ranking'])
            proc.done()
    else:
        proc = MergeAndRankingPipeline(config['data_dir'])
        if 'merge_models' in config:
            proc = proc.step('merge_models', **config['merge_models'])
        if 'merge_datasets' in config:
            proc = proc.step('merge_datasets', **config['merge_datasets'])
        if 'ranking' in config:
            proc = proc.step('ranking', **config['ranking'])
        proc.done()
    

def accumulate(config):
    config = config['RankingPipeline']
    if config['data_dir'] == "all":
        data_dir_base = Path(__file__).parents[2] / 'data'
        log_path = Path(__file__).parents[2] / f'logs/rank-all-{datetime.now().strftime(r"%Y-%m-%d_%H-%M-%S")}'
        for data_dir in sorted(data_dir_base.glob(r"????-??-??"))[1:]:
            proc = AccumulateAndRankingPipeline(data_dir, log_path/f"{data_dir.name}.log")
            if 'accumulate' in config:
                proc = proc.step('accumulate')
            if 'ranking' in config:
                proc = proc.step('ranking', **config['ranking'])
            proc.done()
    else:
        proc = AccumulateAndRankingPipeline(config['data_dir'])
        if 'accumulate' in config:
            proc = proc.step('accumulate')
        if 'ranking' in config:
            proc = proc.step('ranking', **config['ranking'])
        proc.done()


def test_hf_pipeline():
    save_path = Path(__file__).parents[2] / 'tmp-data/hf-test'
    save_path.mkdir(exist_ok=True, parents=True)
    HFPipeline('hf-test', save_dir=save_path).step(
        'init_org_links', True, orgs=["Baichuan", "Huawei"]
    ).step(
        'crawl_repo_page', True
    ).step(
        'crawl_detail_page', True
    ).step(
        'post_process', True
    ).done()


@deprecated("Temporary function for web crawler, deprecated.")
def tmp_executor(target_org=None):
    from loguru import logger
    from tqdm import tqdm
    from pathlib import Path
    from .pipeline.readers import OrgLinksReader
    from .pipeline.crawlers import HFRepoPageCrawler, MSRepoPageCrawler
    from .pipeline.crawlers import HFDetailPageCrawler, MSDetailPageCrawler
    from .pipeline.crawlers import OpenDataLabCrawler, BAAIDatasetsCrawler
    from .pipeline.writers import JsonlineWriter
    from datetime import datetime
    import jsonlines
    
    cur_path = Path(__file__)
    data_path = cur_path.parents[2] / 'data/2025-09-07'
    data_path.mkdir(exist_ok=True)
    screenshot_path = cur_path.parents[2] / 'screenshots/2025-09-07'
    log_path = cur_path.parents[2] / 'logs' / f'tmp-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    logger.add(log_path, level="DEBUG")
    error_f = cur_path.parents[2] / 'data/2025-09-07/errors.jsonl'
    error_f.parent.mkdir(exist_ok=True, parents=True)
    error_f.touch()
    error_f = open(error_f, 'a', encoding='utf-8')
    error_writer = jsonlines.Writer(error_f)
    
    reader = OrgLinksReader(orgs=target_org)
    reader.parse_input()
    org_links = next(reader.run())
    logger.info(f"Target sources: {org_links.data['target_sources']}")
    logger.info(f"Target orgs: {org_links.message['target_orgs']}")
    logger.info(f"Total links: {org_links.message['total_links']}")
    from pprint import pprint
    pprint(org_links.data)

    hf_repo = HFRepoPageCrawler(threads=1)
    hf_repo.parse_input(org_links)
    count = len(hf_repo.input['link-category'])
    pbar = tqdm(total=count, desc="Crawling repo infos (HuggingFace)...")
    data_list = []
    for data in hf_repo.run():
        if data.data is None:
            # logger.error(f"Error in HFRepoPageCrawler with error message {data.error['error_msg']}")
            error_writer.write(data.error)
            error_f.flush()
            pbar.update(1)
            continue
        data_list.append(data)
        pbar.update(1)
        
    pbar.close()
    count = 0
    for data in data_list:
        if data.data is not None:
            count += len(data.data['detail_urls'])
    pbar = tqdm(total=count, desc="Crawling detail infos (HuggingFace)...")
    hf_detail = HFDetailPageCrawler(threads=1, screenshot_path=screenshot_path/'HuggingFace')
    model_writer = JsonlineWriter(data_path / 'HuggingFace/raw-models-info.jsonl', drop_keys=['repo_org_mapper'])
    dataset_writer = JsonlineWriter(data_path / 'HuggingFace/raw-datasets-info.jsonl', drop_keys=['repo_org_mapper'])
    for inp in data_list:
        hf_detail.parse_input(inp)
        for data in hf_detail.run():
            if data.data is None:
                error_writer.write(data.error)
                error_f.flush()
                pbar.update(1)
                continue
            if 'model_name' in data.data:
                model_writer.parse_input(data)
                res = next(model_writer.run())
            elif 'dataset_name' in data.data:
                dataset_writer.parse_input(data)
                res = next(dataset_writer.run())
            if res.error is not None:
                error_writer.write(res.error)
                error_f.flush()
            pbar.update(1)
            
    model_writer.close()
    dataset_writer.close()
    logger.info('HuggingFace done!')
    pbar.close()
    
    ms_repo = MSRepoPageCrawler(threads=1)
    ms_repo.parse_input(org_links)
    count = len(ms_repo.input['link-category'])
    pbar = tqdm(total=count, desc="Crawling repo infos (ModelScope)...")
    data_list = []
    for data in ms_repo.run():
        if data.data is None:
            print(data.error['error_msg'])
            pbar.update(1)
            continue
        data_list.append(data)
        pbar.update(1)
    pbar.close()
    
    count = 0
    for data in data_list:
        if data.data is not None:
            count += len(data.data['detail_urls'])
    pbar = tqdm(total=count, desc="Crawling detail infos (ModelScope)...")
    ms_detail = MSDetailPageCrawler(threads=1, screenshot_path=screenshot_path/'ModelScope')
    model_writer = JsonlineWriter(data_path / 'ModelScope/raw-models-info.jsonl', drop_keys=['repo_org_mapper'])
    dataset_writer = JsonlineWriter(data_path / 'ModelScope/raw-datasets-info.jsonl', drop_keys=['repo_org_mapper'])
    for inp in data_list:
        ms_detail.parse_input(inp)
        for data in ms_detail.run():
            if data.data is None:
                error_writer.write(data.error)
                error_f.flush()
                pbar.update(1)
                continue
            if 'model_name' in data.data:
                model_writer.parse_input(data)
                res = next(model_writer.run())
            elif 'dataset_name' in data.data:
                dataset_writer.parse_input(data)
                res = next(dataset_writer.run())
            if res.error is not None:
                error_writer.write(res.error)
                error_f.flush()
            pbar.update(1)
            
    model_writer.close()
    dataset_writer.close()
    logger.info("ModelScope done.")
    pbar.close()
    
    opendatalab = OpenDataLabCrawler()
    dataset_writer = JsonlineWriter(data_path/'OpenDataLab/raw-datasets-info.jsonl')
    opendatalab.parse_input(org_links)
    data_list = []
    for data in opendatalab.run():
        if data.error:
            error_writer.write(data.error)
            error_f.flush()
            continue
        dataset_writer.parse_input(data)
        res = next(dataset_writer.run())
        if res.error:
            error_writer.write(res.error)
            error_f.flush()
        
    dataset_writer.close()
    logger.info("OpenDataLab done.")

    baai = BAAIDatasetsCrawler()
    dataset_writer = JsonlineWriter(data_path/'BAAIData/raw-datasets-info.jsonl') 
    baai.parse_input(org_links)
    data_list = []
    for data in baai.run():
        if data.error:
            error_writer.write(data.error)
            error_f.flush()
            continue
        dataset_writer.parse_input(data)
        res = next(dataset_writer.run())
        if res.error:
            error_writer.write(res.error)
            error_f.flush()
            
    dataset_writer.close()
    logger.info("BAAIData done.")
