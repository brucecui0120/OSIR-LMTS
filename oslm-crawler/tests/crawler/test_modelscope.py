import pytest
from oslm_crawler.crawler.utils import init_driver
from oslm_crawler.crawler.modelscope import (
    MSRepoPage, MSModelPage, MSDatasetPage
)
    
    
@pytest.fixture(scope='class')
def driver(request):
    web_driver = init_driver()
    request.cls.driver = web_driver
    yield
    web_driver.quit()


@pytest.mark.parametrize("link", [
    "https://modelscope.cn/organization/ByteDance-Seed",
    "https://modelscope.cn/organization/moonshotai",
    "https://modelscope.cn/organization/XenArcAI"
])
@pytest.mark.usefixtures("driver")
class TestMSRepoPage:
    
    def test_crawl_datasets(self, link):
        page = MSRepoPage(self.driver, link)
        info = page.scrape('datasets')
        assert info.error_msg is None
        
    def test_crawl_models(self, link):
        page = MSRepoPage(self.driver, link)
        info = page.scrape('models')
        assert info.error_msg is None
        

@pytest.mark.parametrize("link", [
    "https://modelscope.cn/models/Qwen/Qwen-Image-Edit",
    "https://modelscope.cn/models/MTWLDFC/miratsu_style",
    "https://modelscope.cn/models/Qwen/Qwen3-32B",
    "https://modelscope.cn/models/AI-ModelScope/ZhengPeng7-BiRefNet",
    "https://modelscope.cn/models/okwinds/phi4-bf16",
])
@pytest.mark.usefixtures("driver")
class TestMSModelPage:
    
    def test_crawl(self, link):
        page = MSModelPage(self.driver, link)
        info = page.scrape()
        print(info)
        assert info.link == link
        assert info.error_msg is None
        
# TODO wait until modelscope dataset page shows number of likes
@pytest.mark.parametrize("link", [
    "https://modelscope.cn/datasets/agibot_world/agibot_world_beta/1",
    "https://modelscope.cn/datasets/huangjintao/latex_ocr_test/1",
    "https://modelscope.cn/datasets/DiffSynth-Studio/ImagePulse-StyleTransfer/1",
    "https://modelscope.cn/datasets/DanteQ/gsm8k-fixed/1",
    "https://modelscope.cn/datasets/citest/test_upload_file_folder_dataset_c0c204/1",
])
@pytest.mark.usefixtures("driver")
class TestMSDatasetPage:
    
    def test_crawl(self, link):
        page = MSDatasetPage(self.driver, link)
        info = page.scrape()
        print(info)
        assert info.link + '/1' == link
        assert info.error_msg is None
