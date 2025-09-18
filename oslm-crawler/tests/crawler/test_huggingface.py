import pytest
from oslm_crawler.crawler.utils import init_driver
from oslm_crawler.crawler.huggingface import (
    HFRepoPage, HFModelPage, HFDatasetPage
)


@pytest.fixture(scope='class')
def driver(request):
    web_driver = init_driver()
    request.cls.driver = web_driver
    yield
    web_driver.quit()


@pytest.mark.parametrize("link", [
    "https://huggingface.co/baidu",
    "https://huggingface.co/CofeAI",
    "https://huggingface.co/PaddlePaddle",
    "https://huggingface.co/fka",
])
@pytest.mark.usefixtures("driver")
class TestHFRepoPage:
    
    def test_crawl_datasets(self, link):
        page = HFRepoPage(self.driver, link)
        info = page.scrape('datasets')
        assert info.error_msg is None
        
    def test_crawl_models(self, link):
        page = HFRepoPage(self.driver, link)
        info = page.scrape('models')
        assert info.error_msg is None


@pytest.mark.parametrize("link", [
    "https://huggingface.co/zai-org/GLM-4.5",
    "https://huggingface.co/openbmb/MiniCPM-V-4_5",
    "https://huggingface.co/GaboxR67/MelBandRoformers",
    "https://huggingface.co/quadranttechnologies/AutomatedMedicalCoding",
    "https://huggingface.co/voyageai/voyage-3-m-exp",
    "https://huggingface.co/pharmapsychotic/CLIPtion",
    "https://huggingface.co/LifuWang/DistillT5",
    "https://huggingface.co/openai/gpt-oss-20b",
    "https://huggingface.co/deepseek-ai/DeepSeek-V3.1"
])
@pytest.mark.usefixtures("driver")
class TestHFModelPage:
    
    def test_crawl(self, link):
        page = HFModelPage(self.driver, link)
        info = page.scrape()
        print(info)
        assert info.link == link
        assert info.error_msg is None
    

@pytest.mark.parametrize("link", [
    "https://huggingface.co/datasets/EMBO/soda-vec-data-full_pmc_title_abstract",
    "https://huggingface.co/datasets/fka/awesome-chatgpt-prompts",
    "https://huggingface.co/datasets/Kunbyte/OmniTry-Bench",
    "https://huggingface.co/datasets/ESZER/H",
    "https://huggingface.co/datasets/Doohae/modern_music_re",
    "https://huggingface.co/datasets/openai/gsm8k",
    "https://huggingface.co/datasets/openbmb/RLAIF-V-Dataset",
    "https://huggingface.co/datasets/racineai/OGC_History_Geography",
    "https://huggingface.co/datasets/BAAI/CCI4.0-M2-CoT-v1"
])
@pytest.mark.usefixtures("driver")
class TestHFDatasetPage:
    
    def test_crawl(self, link):
        page = HFDatasetPage(self.driver, link)
        info = page.scrape()
        print(info)
        assert info.link == link
        assert info.error_msg is None
    