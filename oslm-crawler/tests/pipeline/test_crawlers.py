from pprint import pprint
from oslm_crawler.pipeline.base import PipelineData
from oslm_crawler.pipeline.crawlers import HFRepoPageCrawler
from oslm_crawler.pipeline.crawlers import MSRepoPageCrawler
from oslm_crawler.pipeline.crawlers import HFDetailPageCrawler
from oslm_crawler.pipeline.crawlers import MSDetailPageCrawler
from oslm_crawler.pipeline.crawlers import OpenDataLabCrawler
from oslm_crawler.pipeline.crawlers import BAAIDatasetsCrawler


class TestRepoPageCrawler:
    
    def test_hf_repo_page_crawler(self):
        links = [
            "https://huggingface.co/swiss-ai",
            "https://huggingface.co/stepfun-ai",
            "https://huggingface.co/rednote-hilab",
            "https://huggingface.co/Marvis-AI",
        ]
        crawler = HFRepoPageCrawler(threads=4)
        crawler.parse_input(PipelineData({
            "HuggingFace": links, 
            "target_sources": ["HuggingFace"],
            "repo_org_mapper": {}
        }, {"total_links": len(links)}, None))
        for pdata in crawler.run():
            assert pdata.error is None
            assert "repo_org_mapper" in pdata.data
            msg = pdata.message
            print(f"HF Repo {msg['repo']} has {msg['total_links']} {msg['category']} links.")

    def test_ms_repo_page_crawler(self):
        links = [
            "https://modelscope.cn/organization/moonshotai",
            "https://modelscope.cn/organization/ByteDance-Seed",
            "https://modelscope.cn/organization/XenArcAI",
            "https://modelscope.cn/organization/lightx2v"
        ]
        crawler = MSRepoPageCrawler(threads=4)
        crawler.parse_input(PipelineData({
            "ModelScope": links,
            "target_sources": ["ModelScope"],
            "repo_org_mapper": {}
        }, {"total_links": len(links)}, None))
        for pdata in crawler.run():
            assert pdata.error is None
            assert "repo_org_mapper" in pdata.data
            msg = pdata.message
            print(f"MS Repo {msg['repo']} has {msg['total_links']} {msg['category']} links.")


class TestHFDetailPageCrawler:
    
    def test_hf_datasets_crawler(self):
        links = [
            "https://huggingface.co/datasets/EMBO/soda-vec-data-full_pmc_title_abstract",
            "https://huggingface.co/datasets/fka/awesome-chatgpt-prompts",
            "https://huggingface.co/datasets/Kunbyte/OmniTry-Bench",
            "https://huggingface.co/datasets/ESZER/H",
            "https://huggingface.co/datasets/Doohae/modern_music_re",
            "https://huggingface.co/datasets/openai/gsm8k",
            "https://huggingface.co/datasets/openbmb/RLAIF-V-Dataset",
            "https://huggingface.co/datasets/racineai/OGC_History_Geography",
            "https://huggingface.co/datasets/BAAI/CCI4.0-M2-CoT-v1"
        ]
        crawler = HFDetailPageCrawler(threads=4)
        crawler.parse_input(PipelineData({
            "category": "datasets",
            "repo_org_mapper": {},
            "detail_urls": links
        }, None, None))
        for pdata in crawler.run():
            assert pdata.error is None
            assert "repo_org_mapper" in pdata.data
            msg = pdata.message
            pprint(msg)

    def test_hf_models_crawler(self):
        links = [
            "https://huggingface.co/zai-org/GLM-4.5",
            "https://huggingface.co/openbmb/MiniCPM-V-4_5",
            "https://huggingface.co/GaboxR67/MelBandRoformers",
            "https://huggingface.co/quadranttechnologies/AutomatedMedicalCoding",
            "https://huggingface.co/voyageai/voyage-3-m-exp",
            "https://huggingface.co/pharmapsychotic/CLIPtion",
            "https://huggingface.co/LifuWang/DistillT5",
            "https://huggingface.co/openai/gpt-oss-20b",
            "https://huggingface.co/deepseek-ai/DeepSeek-V3.1"
        ]
        crawler = HFDetailPageCrawler(threads=4)
        crawler.parse_input(PipelineData({
            "category": "models",
            "repo_org_mapper": {},
            "detail_urls": links
        }, None, None))
        for pdata in crawler.run():
            assert pdata.error is None
            assert "repo_org_mapper" in pdata.data
            msg = pdata.message
            pprint(msg)

    
class TestMSDetailPageCrawler:
    
    def test_ms_datasets_crawler(self):
        links = [
            "https://modelscope.cn/datasets/agibot_world/agibot_world_beta/1",
            "https://modelscope.cn/datasets/huangjintao/latex_ocr_test/1",
            "https://modelscope.cn/datasets/DiffSynth-Studio/ImagePulse-StyleTransfer/1",
            "https://modelscope.cn/datasets/DanteQ/gsm8k-fixed/1",
            "https://modelscope.cn/datasets/citest/test_upload_file_folder_dataset_c0c204/1",
        ]
        crawler = MSDetailPageCrawler(threads=2)
        crawler.parse_input(PipelineData({
            "category": "datasets",
            "repo_org_mapper": {},
            "detail_urls": links
        }, None, None))
        for pdata in crawler.run():
            assert pdata.error is None
            assert "repo_org_mapper" in pdata.data
            msg = pdata.message
            pprint(msg)
    
    def test_ms_models_crawler(self):
        links = [
            "https://modelscope.cn/models/Qwen/Qwen-Image-Edit",
            "https://modelscope.cn/models/MTWLDFC/miratsu_style",
            "https://modelscope.cn/models/Qwen/Qwen3-32B",
            "https://modelscope.cn/models/AI-ModelScope/ZhengPeng7-BiRefNet",
            "https://modelscope.cn/models/okwinds/phi4-bf16",
        ]
        crawler = MSDetailPageCrawler(threads=2)
        crawler.parse_input(PipelineData({
            "category": "models",
            "repo_org_mapper": {},
            "detail_urls": links
        }, None, None))
        for pdata in crawler.run():
            assert pdata.error is None
            assert "repo_org_mapper" in pdata.data
            msg = pdata.message
            pprint(msg)
            
            
class TestOpenDataLabCrawler:

    def test_open_data_lab_crawler(self):
        links = [
            "https://opendatalab.com/?createdBy=12199&pageNo=0&pageSize=12&sort=downloadCount",
            "https://opendatalab.com/?createdBy=11828&pageNo=0&pageSize=12&sort=downloadCount",
            "https://opendatalab.com/?createdBy=12157&pageNo=0&pageSize=12&sort=downloadCount",
            "https://opendatalab.com/?createdBy=12589&pageNo=0&pageSize=12&sort=downloadCount",
            "https://opendatalab.com/?createdBy=1678533&pageNo=0&pageSize=12&sort=downloadCount"
        ]
        crawler = OpenDataLabCrawler(threads=4)
        crawler.parse_input(PipelineData({
            "OpenDataLab": links,
            "target_sources": ["OpenDataLab"],
            "repo_org_mapper": {}
        }, {"total_links": len(links)}, None))
        for pdata in crawler.run():
            assert pdata.error is None
            assert "repo_org_mapper" not in pdata.data

            
class TestBAAIDataCrawler:
    
    def test_baai_data_crawler(self):
        crawler = BAAIDatasetsCrawler()
        crawler.parse_input(PipelineData({
            "BAAI Data": ["https://data.baai.ac.cn/dataset"],
            "target_sources": ["BAAI Data"],
            "repo_org_mapper": {}
        }, {"total_links": 1}, None))
        for pdata in crawler.run():
            assert pdata.error is None
            assert "repo_org_mapper" not in pdata.data
