from pathlib import Path
from oslm_crawler.core import HFPipeline, MSPipeline

class TestHFPipeline:
    
    def test_all_step(self):
        save_path = Path(__file__).parents[1] / 'tmp-data/hf-test'
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
        
        
class TestMSPipeline:
    
    def test_all_step(self):
        save_path = Path(__file__).parents[1] / 'tmp-data/ms-test'
        save_path.mkdir(exist_ok=True, parents=True)
        MSPipeline('ms-test', save_dir=save_path).step(
            'init_org_links', True, orgs=["Baichuan", "Huawei"]
        ).step(
            'crawl_repo_page', True
        ).step(
            'crawl_detail_page', True
        ).step(
            'post_process', True
        ).done()
