import random
import json
from pprint import pprint
from pathlib import Path
from oslm_crawler.pipeline.base import PipelineData
from oslm_crawler.pipeline.readers import JsonlineReader, OrgLinksReader
from oslm_crawler.pipeline.processors import HFInfoProcessor, MSInfoProcessor
from oslm_crawler.pipeline.processors import OpenDataLabInfoProcessor, BAAIDataInfoProcessor


def test_hfinfo_processor():
    
    data_path = Path(__file__).parents[2] / 'data'
    lst = list(sorted(data_path.glob('????-??-??')))
    assert len(lst) > 0, 'No valid data'
    models_path = lst[-1] / 'HuggingFace/raw-models-info.jsonl'
    datasets_path = lst[-1] / 'HuggingFace/raw-datasets-info.jsonl'
    model_info_path = data_path.parent / 'config/model-info.json'
    dataset_info_path = data_path.parent / 'config/dataset-info.json'
    with model_info_path.open('r') as f:
        model_gen = json.load(f)
    with dataset_info_path.open('r') as f:
        dataset_gen = json.load(f)
    
    org_links_reader = OrgLinksReader()
    org_links_reader.parse_input()
    repo_org_mapper = next(org_links_reader.run()).data['repo_org_mapper']
    
    models_reader = JsonlineReader(models_path)
    datasets_reader = JsonlineReader(datasets_path)
    
    all_infos = []
    other_infos = []
    models_reader.parse_input()
    models_info = next(models_reader.run()).data['content']
    for info in models_info:
        key = f"{info['repo']}/{info['model_name']}"
        if key in model_gen:
            all_infos.append(info)
        else:
            other_infos.append(info)
    datasets_reader.parse_input()
    datasets_info = next(datasets_reader.run()).data['content']
    for info in datasets_info:
        key = f"{info['repo']}/{info['dataset_name']}"
        if key in dataset_gen:
            all_infos.append(info)
        else:
            other_infos.append(info)
    all_infos.extend(random.sample(other_infos, min(len(other_infos), 24)))
    
    hfinfo_processor = HFInfoProcessor()
    all_res = []
    for info in all_infos:
        info['repo_org_mapper'] = repo_org_mapper
        hfinfo_processor.parse_input(PipelineData(info, None, None))
        for res in hfinfo_processor.run():
            all_res.append(res)
    for res in hfinfo_processor.flush(update_infos=False):
        all_res.append(res)
        
    pprint(all_res[0])
    assert len(all_res) <= len(all_infos)
    
    
def test_msinfo_processor():
    
    data_path = Path(__file__).parents[2] / 'data'
    lst = list(sorted(data_path.glob('????-??-??')))
    assert len(lst) > 0, 'No valid data'
    models_path = lst[-1] / 'ModelScope/raw-models-info.jsonl'
    datasets_path = lst[-1] / 'ModelScope/raw-datasets-info.jsonl'
    model_info_path = data_path.parent / 'config/model-info.json'
    dataset_info_path = data_path.parent / 'config/dataset-info.json'
    with model_info_path.open('r') as f:
        model_gen = json.load(f)
    with dataset_info_path.open('r') as f:
        dataset_gen = json.load(f)
    
    org_links_reader = OrgLinksReader()
    org_links_reader.parse_input()
    repo_org_mapper = next(org_links_reader.run()).data['repo_org_mapper']
    
    models_reader = JsonlineReader(models_path)
    datasets_reader = JsonlineReader(datasets_path)
    
    all_infos = []
    other_infos = []
    models_reader.parse_input()
    models_info = next(models_reader.run()).data['content']
    for info in models_info:
        key = f"{info['repo']}/{info['model_name']}"
        if key in model_gen:
            all_infos.append(info)
        else:
            other_infos.append(info)
    datasets_reader.parse_input()
    datasets_info = next(datasets_reader.run()).data['content']
    for info in datasets_info:
        key = f"{info['repo']}/{info['dataset_name']}"
        if key in dataset_gen:
            all_infos.append(info)
        else:
            other_infos.append(info)
    all_infos.extend(random.sample(other_infos, min(len(other_infos), 24)))
    
    msinfo_processor = MSInfoProcessor()
    all_res = []
    for info in all_infos:
        info['repo_org_mapper'] = repo_org_mapper
        msinfo_processor.parse_input(PipelineData(info, None, None))
        for res in msinfo_processor.run():
            all_res.append(res)
    for res in msinfo_processor.flush(update_infos=False):
        all_res.append(res)
        
    pprint(all_res[0])
    assert len(all_res) <= len(all_infos)
    
    
def test_odlinfo_processor():
    data_path = Path(__file__).parents[2] / 'data'
    lst = list(sorted(data_path.glob('????-??-??')))
    assert len(lst) > 0, 'No valid data'
    datasets_path = lst[-1] / 'OpenDataLab/raw-datasets-info.jsonl'
    dataset_info_path = data_path.parent / 'config/dataset-info.json'
    with dataset_info_path.open('r') as f:
        dataset_gen = json.load(f)
    
    datasets_reader = JsonlineReader(datasets_path)
    
    all_infos = []
    other_infos = []
    datasets_reader.parse_input()
    datasets_info = next(datasets_reader.run()).data['content']
    for info in datasets_info:
        key = f"{info['repo']}/{info['dataset_name']}"
        if key in dataset_gen:
            all_infos.append(info)
        else:
            other_infos.append(info)
    all_infos.extend(random.sample(other_infos, min(len(other_infos), 24)))
    
    msinfo_processor = OpenDataLabInfoProcessor()
    all_res = []
    for info in all_infos:
        msinfo_processor.parse_input(PipelineData(info, None, None))
        for res in msinfo_processor.run():
            all_res.append(res)
    for res in msinfo_processor.flush(update_infos=False):
        all_res.append(res)
        
    pprint(all_res[0])
    assert len(all_res) <= len(all_infos)
    
    
def test_baaiinfo_processor():
    data_path = Path(__file__).parents[2] / 'data'
    lst = list(sorted(data_path.glob('????-??-??')))
    assert len(lst) > 0, 'No valid data'
    datasets_path = lst[-1] / 'BAAIData/raw-datasets-info.jsonl'
    dataset_info_path = data_path.parent / 'config/dataset-info.json'
    with dataset_info_path.open('r') as f:
        dataset_gen = json.load(f)
    
    datasets_reader = JsonlineReader(datasets_path)
    
    all_infos = []
    other_infos = []
    datasets_reader.parse_input()
    datasets_info = next(datasets_reader.run()).data['content']
    for info in datasets_info:
        key = f"{info['repo']}/{info['dataset_name']}"
        if key in dataset_gen:
            all_infos.append(info)
        else:
            other_infos.append(info)
    all_infos.extend(random.sample(other_infos, min(len(other_infos), 24)))
    
    msinfo_processor = BAAIDataInfoProcessor()
    all_res = []
    for info in all_infos:
        msinfo_processor.parse_input(PipelineData(info, None, None))
        for res in msinfo_processor.run():
            all_res.append(res)
    for res in msinfo_processor.flush(update_infos=False):
        all_res.append(res)
        
    pprint(all_res[0])
    assert len(all_res) <= len(all_infos)
