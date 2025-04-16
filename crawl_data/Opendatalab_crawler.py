# -*- coding: utf-8 -*-
# @Time       : 2024/12/5 9:50
# @Author     : Marverlises
# @File       : OpenDatalab_crawler.py
# @Description: PyCharm

import os
import logging
import re
import time
import tqdm
from selenium.webdriver.common.by import By
from typing_extensions import override

from crawl_data.Data_post_process import DataPostProcess
from utils import init_driver, save_json_data, read_json_file
from typing import Dict, List, Tuple


class OpenDataLabCrawler:
    """
    爬取OpenDataLab官网的数据集
    """

    def __init__(self, headless=True, screen_shot_save_path='./result/OpenDataLab', log_level=logging.INFO,
                 keyword=None, sort_method=None, final_url=None):
        """
        初始化
        :param headless:                    是否使用无头浏览器
        :param screen_shot_save_path:       截图保存路径
        :param log_level:                   日志级别
        :param keyword:                     需要爬取的关键词
        :param sort_method:                 排序方式
        :param final_url:                   最终要爬取的URL
        """
        self.result = {}  # 保存结果
        self.exception_links = []  # 保存异常链接
        self.total_links = []  # 保存所有数据集链接
        sort_method_map = {"综合排序": 'all', "周下载量": 'downloadCountWeek', "周查看量": 'viewCountWeek',
                           "下载量": 'downloadCount', "查看量": 'viewCount', "更新时间": 'firstRelease'}
        self.base_url = "https://opendatalab.org.cn/"
        self.driver = init_driver(headless=headless)
        self.sort_method = sort_method_map[sort_method] if sort_method in sort_method_map else 'downloadCount'
        self.screen_shot_save_path = screen_shot_save_path + '/OpenDataLab_dataset_info_screenshots'
        if os.path.exists(self.screen_shot_save_path) is False:
            os.makedirs(self.screen_shot_save_path)
        # TODO: 未完成根据关键词进行搜索爬取数据
        # if keyword is None:
        #     self.final_url = self.base_url + f'?sort={self.sort_method}'
        # else:
        #     self.final_url = self.base_url + f'?keywords={keyword}&sort={self.sort_method}'
        self.final_url = final_url
        self._init_logger(log_level=log_level)
        self._init_relevant_element_xpath()

    def _init_logger(self, log_level: int = logging.INFO) -> None:
        """
        初始化日志
        :return:
        """
        log_dir = './logs/OpenDataLab'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, 'OpenDataLab_log.log')
        logging.basicConfig(level=log_level,
                            format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                            handlers=[
                                logging.FileHandler(log_file),  # 写入文件
                                logging.StreamHandler()  # 控制台输出
                            ])
        logging.info("Start crawling OpenDataLab dataset info")

    def _init_relevant_element_xpath(self) -> None:
        """
        初始化相关元素的Xpath
        :return:
        """
        # 总数 //*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[1]/div/div[1]/div
        self.total_count_xpath = '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[1]/div/div[1]/div'
        # 总元素//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[2]/div[2]/div
        self.page_elements_xpath = '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[2]/div[2]/div'
        # dataset name //*[@id="root"]/div/div/main/div/div[2]/div[1]/div/div/div[1]/h3
        self.dataset_name_xpath = '//*[@id="root"]/div/div/main/div/div[2]/div[1]/div/div/div[1]/h3'
        # dataset like num //*[@id="root"]/div/div/main/div/div[2]/div[1]/div/div/span
        self.dataset_like_num_xpath = '//*[@id="root"]/div/div/main/div/div[2]/div[1]/div/div/span'
        # dataset tags //*[@id="root"]/div/div/main/div/div[2]/div[1]/div/div/div[3]
        self.dataset_tags_xpath = '//*[@id="root"]/div/div/main/div/div[2]/div[1]/div/div/div[3]'
        # dataset download num / view num / size base xpath //*[@id="root"]/div/div/main/div/div[2]/div[1]/div/div/div[2]
        self.dataset_download_view_size_base_xpath = '//*[@id="root"]/div/div/main/div/div[2]/div[1]/div/div/div[2]'
        # dataset readme //*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div[1]/div[1]
        self.dataset_readme_xpath = '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div[1]/div[1]'
        # dataset release info //*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div[2]/div[1]
        self.dataset_release_info_xpath = '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div[2]/div[1]'

    def crawl_dataset_links(self):
        """
        爬取数据集链接
        """
        for url in self.final_url:
            try:
                self.driver.get(url)
                time.sleep(10)
                # 获取数据集总数
                total_count = self.driver.find_element(By.XPATH, self.total_count_xpath).text
                logging.info(f"Total count: {total_count}")
                # 计算总页数
                each_page_size = int(url.split('pageSize=')[-1].split('&')[0])
                total_page = int(total_count.split(' ')[0]) // each_page_size + 1
                # 获取数据集元素
                each_page_url = [
                    url.replace(f'pageNo=0&pageSize={each_page_size}', f'pageNo={page}&pageSize={each_page_size}') for
                    page in range(1, total_page + 1)]
                for page_url in each_page_url:
                    self.driver.get(page_url)
                    logging.info(f"Current page is: {page_url} ")
                    time.sleep(5)
                    page_elements = self.driver.find_elements(By.XPATH, self.page_elements_xpath + '/div')
                    for element in page_elements:
                        dataset_link = element.find_element(By.TAG_NAME, 'a').get_attribute('href')
                        self.total_links.append(dataset_link)
            except Exception as e:
                logging.error(f"Failed to crawl dataset links: {e}")
                raise Exception(f"Failed to crawl dataset links: {e}")

    def _get_dataset_info(self):
        """
        获取数据集信息
        """
        dataset_info = {}
        try:
            dataset_name = self.driver.find_element(By.XPATH, self.dataset_name_xpath).text
            dataset_like_num = self.driver.find_element(By.XPATH, self.dataset_like_num_xpath).text
            dataset_tags = self.driver.find_element(By.XPATH, self.dataset_tags_xpath).text
            dataset_readme = self.driver.find_element(By.XPATH, self.dataset_readme_xpath).text
            dataset_release_info = self.driver.find_element(By.XPATH, self.dataset_release_info_xpath).text
            # 根据use标签内容获取下载量，浏览量，大小
            base_element = self.driver.find_element(By.XPATH, self.dataset_download_view_size_base_xpath)
            divs = base_element.find_elements(By.XPATH, 'div')
            dataset_size = None
            dataset_view_num = None
            dataset_download_num = None
            for div in divs:
                try:
                    use_element = div.find_element(By.TAG_NAME, 'use')
                    xlink_href = use_element.get_attribute('xlink:href')
                    if xlink_href:
                        icon_id = xlink_href.split('-')[-1]  # 获取#号后面的ID部分
                        # 根据icon_id判断并赋值
                        if "shujujidaxiao" in icon_id:
                            dataset_size = div.text
                        elif "ShowOutlined" in icon_id:
                            dataset_view_num = div.text
                        elif "xiazai" in icon_id:
                            dataset_download_num = div.text
                except Exception as e:
                    continue
            dataset_download_view_size = {
                'dataset_download_num': dataset_download_num,
                'dataset_view_num': dataset_view_num,
                'dataset_size': dataset_size
            }
            dataset_info = {
                'name': dataset_name,
                'like_num': dataset_like_num,
                'tags': dataset_tags,
                'download': dataset_download_view_size['dataset_download_num'],
                'view': dataset_download_view_size['dataset_view_num'],
                'size': dataset_download_view_size['dataset_size'],
                'readme': dataset_readme,
                'release_info': dataset_release_info
            }
            logging.info(f"Got dataset info: {dataset_info['name']}")
            return dataset_info
        except Exception as e:
            logging.error(f"Failed to get dataset info: {e}")
            raise Exception(f"Failed to get dataset info: {e}")

    def crawl(self) -> Tuple[Dict, List]:
        """
        爬取数据集信息
        :return:
        """
        # 首先爬取数据集链接
        self.crawl_dataset_links()
        # 输出数据集链接
        save_json_data(self.total_links, './result/OpenDataLab/OpenDataLab_dataset_links.json')
        # 爬取数据集信息
        for link in tqdm.tqdm(self.total_links):
            try:
                logging.info(f"Current dataset is: {link}")
                self.driver.get(link)
                time.sleep(2)
                # 截图
                self.driver.save_screenshot(f'{self.screen_shot_save_path}/{link.split("/")[-1]}.png')
                # 获取数据集信息
                dataset_info = self._get_dataset_info()
                self.result[link] = dataset_info
            except Exception as e:
                logging.error(f"Failed to crawl dataset info:", e)
                self.exception_links.append(link)

        return self.result, self.exception_links

    # set 方法设置final_url
    def set_final_url(self, final_url):
        self.final_url = final_url
        self.result = {}
        self.exception_links = []
        self.total_links = final_url


class OpenDataLabPostProcess(DataPostProcess):
    organization_type = "DL"

    def __init__(self, dataset_info_file_path):
        super(OpenDataLabPostProcess, self).__init__(dataset_info_file_path)

    @override
    def post_process(self):
        # 取出上月的数据 —— 统计渠道列对应的值为opendatalab
        ms_data = self.last_month_data[self.last_month_data['统计渠道'].str.strip() == 'opendatalab']
        # 构造一个按照链接为key，其它信息为value的字典
        ms_data_dict = {}
        for index, row in ms_data.iterrows():
            ms_data_dict[row['链接']] = row

        result = []
        for key, value in self.dataset_info.items():
            dataset_name = value['name']
            dataset_like_num = value['like_num']
            dataset_tags = value['tags']
            dataset_download_num = self.parse_download_num(
                value['download'] if value['download'] else 0)  # None -> 0 因为根本没有这个标签
            dataset_view_num = value['view']
            dataset_size = value['size']
            dataset_readme = value['readme']
            dataset_release_info = value['release_info']
            # 从链接中获取子组织com/OpenScienceLab/
            dataset_link = key
            sub_organization = re.findall("com/(.*?)/", key)[0]
            try:
                release_time = re.findall("发布时间\n(.*?)\n", dataset_release_info)[0].replace('-', '.')
            except:
                release_time = ''
            if key in ms_data_dict:
                is_new = '否'
                last_month_data = ms_data_dict[key]
                download_num_last_month = dataset_download_num - self.parse_download_num(
                    last_month_data['下载量'])
                modality = last_month_data['模态']
                life_cycle = last_month_data['生命周期']
            else:
                is_new = '是'
                download_num_last_month = dataset_download_num
                modality = ''
                life_cycle = ''

            result.append({
                '组织': 'OpenXLab',
                '机构': sub_organization,
                '数据集名称': dataset_name,
                '模态': modality,
                '生命周期': life_cycle,
                '链接': dataset_link,
                '上月下载量': download_num_last_month,
                '统计渠道': 'opendatalab',
                '是否新增': is_new, '发布时间': release_time,
                '下载量': dataset_download_num,
                'dataset_like_num': dataset_like_num,
                'dataset_tags': dataset_tags,
                'dataset_view_num': dataset_view_num,
                'dataset_size': dataset_size,
                'dataset_readme': dataset_readme
            })
        self.save_to_excel(result, './result/OpenDataLab/OpenDataLab_dataset_info.xlsx')

    @override
    def _extract_need_info(self):
        pass

    @override
    def check_with_last_month(self, data):
        pass


if __name__ == '__main__':
    # # =========================== 爬取数据集信息 ================================
    # need_data = read_json_file('./organization_links/opendatalab_organization_links.json')
    # need_urls = []
    # for key, url in need_data.items():
    #     need_urls.append(url)
    # OpenDataLab_crawler = OpenDataLabCrawler(keyword=None, headless=True,
    #                                          screen_shot_save_path='./result/OpenDataLab',
    #                                          final_url=need_urls)
    #
    # OpenDataLab_dataset_info, exception_links = OpenDataLab_crawler.crawl()
    #
    # logging.info(f"Total dataset: {len(OpenDataLab_dataset_info)}")
    # logging.info(f"Exception links: {exception_links}")
    # # 存储
    # save_json_data(OpenDataLab_dataset_info, './result/OpenDataLab/OpenDataLab_dataset_info.json')
    # if exception_links:
    #     save_json_data(exception_links, './result/OpenDataLab/OpenDataLab_exception_links.json')
    # # =========================== 数据后处理 ================================
    # # 重新爬取失败的链接
    # exception_links = read_json_file('./result/OpenDataLab/OpenDataLab_exception_links.json')
    # OpenDataLab_crawler = OpenDataLabCrawler(keyword=None, headless=False,
    #                                          screen_shot_save_path='./result/OpenDataLab',
    #                                          final_url=exception_links)
    # if exception_links:
    #     OpenDataLab_crawler.set_final_url(exception_links)
    #     OpenDataLab_dataset_info, exception_links = OpenDataLab_crawler.crawl()
    #     logging.info(f"Total dataset: {len(OpenDataLab_dataset_info)}")
    #     logging.info(f"Exception links: {exception_links}")
    #     save_json_data(OpenDataLab_dataset_info, './result/OpenDataLab/OpenDataLab_dataset_info_exceptions.json')
    #     if exception_links:
    #         save_json_data(exception_links, './result/OpenDataLab/OpenDataLab_exception_links.json')
    # assert len(exception_links) == 0, "Some links are not crawled successfully"
    dataset_info_file_path = './result/OpenDataLab/OpenDataLab_dataset_info.json'
    OpenDataLab_post_process = OpenDataLabPostProcess(dataset_info_file_path)
    OpenDataLab_post_process.post_process()
