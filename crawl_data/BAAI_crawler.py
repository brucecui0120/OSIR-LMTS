# -*- coding: utf-8 -*-
# @Time       : 2024/11/19 10:05
# @Author     : Marverlises
# @File       : BAAI_crawler.py
# @Description: 爬取BAAI官网的数据集
import os
import time
import logging
import requests
import json
from typing import Dict, List, Tuple

from crawl_data.Data_post_process import DataPostProcess
from utils import init_driver, save_json_data
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from deprecated import deprecated


class BAAICrawler:
    """
    爬取BAAI官网的数据集
    """

    def __init__(self, sort_method_index=1, headless=True, screen_shot_save_path='./result/BAAI',
                 log_level=logging.INFO):
        """
        初始化
        :param sort_method_index:           排序方法 【downloads, views, updated】 -> 【1, 2, 3】
        :param headless:                    是否使用无头浏览器
        """
        self.base_url = "https://data.baai.ac.cn/data"
        self.driver = init_driver(headless=headless)
        self.sort_method_index = sort_method_index
        self.screen_shot_save_path = screen_shot_save_path + '/BAAI_dataset_info_screenshots'
        if os.path.exists(self.screen_shot_save_path) is False:
            os.makedirs(self.screen_shot_save_path)

        # 初始化相关元素的Xpath
        self._init_logger(log_level=log_level)
        self._init_relevant_element_xpath()
        # 初始化爬取目标——替换掉以往的自动化逐页面爬取
        self.targets = self._init_targets()

    def _init_logger(self, log_level: int = logging.INFO) -> None:
        """
        初始化日志
        :return:
        """
        log_dir = './logs/BAAI'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, 'BAAI_crawl_log.log')
        logging.basicConfig(level=log_level,
                            format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                            handlers=[
                                logging.FileHandler(log_file),  # 写入文件
                                logging.StreamHandler()  # 控制台输出
                            ])
        logging.info("Start crawling BAAI dataset info")

    def _init_relevant_element_xpath(self) -> None:
        """
        初始化相关元素的Xpath
        :return:
        """
        # 所有数据集元素div
        self.page_elements_xpath = '//*[@id="app"]/div/section/main/div/div/div[1]/div[2]/div[5]'
        # 数据集详情描述
        self.dataset_description_xpath = '//*[@id="app"]/div/section/main/div[1]/div[2]/div[1]/div'
        # 下一页
        self.next_page_button_xpath = '//*[@id="app"]/div/section/main/div/div/div[1]/div[2]/div[6]/div/button[2]'
        # 出现排序方法
        self.sort_method_button_xpath = '//*[@id="app"]/div/section/main/div/div/div[1]/div[2]/div[4]/div[2]/div/div/div'
        # 排序方法
        self.sort_method_xpath = f'/html/body/div[2]/div/div/div/div[1]/ul/li[{self.sort_method_index}]'
        # 总页数
        self.total_page_xpath = '//*[@id="app"]/div/section/main/div/div/div[1]/div[2]/div[6]/div/ul/li'

    def _init_targets(self) -> List[str]:
        """
        初始化爬取目标
        根据BAAI官网的数据集页面，请求后端接口获取到所有页面的链接
        :return:
        """
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Cookie': 'remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d=eyJpdiI6InpNbWpGclRRZGZuZEdkMzZYL09YSVE9PSIsInZhbHVlIjoiMGtVTzh2SWJ0L3hHb1NLeEl6SU9FUC9VbUk5YzVUWWpjeTdsOHpoenB2aG5Vb29wcm9zUlBaV3FudUFwc2VobC8zaU5SSEZhNmZSNCtwUWdJcG5KeFJJUXVUVy92Nnk0MmhiSXVkS1NIVzY1WHo3bSsvcHlyYlp3VXlsdXpYbEdwYVloeDIxUzUzMi9YeDBLUGMxLzdnPT0iLCJtYWMiOiJkZjhmYzkzZmZkZmFkYjAwMDVjMjNlYTdjMDNiMWZmNGJiYTBlNmRlZmEzNzUyMzA0MmRkNzA2MDE1ZmExMDdjIn0%3D; guidance-profile-2024=yes; hubapi_session=eyJpdiI6ImtkUEhPcFd2QmNTV0VhSDJ1b3oremc9PSIsInZhbHVlIjoiTUZBMVFJcXZ1UVJIbVoxZkwvTHAwYjVFbGxUME9aNE95RThzeUNxUWo3Q3d0eUhIeG03SzllazNLcjZyelFsRk9lTGNHWnAvbm1GcWt1bVppNWc3STU4ejRWSG5KVDFiTU4xaHR5WGFodnA5K0Z0bG44UHhkQ3NTdit2UGlXQVUiLCJtYWMiOiI0OWI2YmQxMTQ0YmY1MmY3ZDRjNGRlMjcwMjUwN2VhODM2NTFjMTMzYmQ5OGI4NGQ4YTIzMzdjNzJlMGZmMGNkIn0%3D',
            'LanguageType': 'ch',
            'Origin': 'https://data.baai.ac.cn',
            'Referer': 'https://data.baai.ac.cn/data',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'UserSystem': '3',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        data = {
            "pageSize": 1000,  # 一页1000个数据集，一次性全部获取
            "currentNumb": 1,
            "optionType": "1",
            "dataName": None,
            "tagIDList": []
        }
        # 模拟请求发送
        response = requests.post(
            'https://data.baai.ac.cn/api/service/data/v1/getDataList',
            headers=headers,
            data=json.dumps(data)
        )
        response_json = response.json()
        total_targets = response_json['data']['total']
        logging.info(f"Total targets: {total_targets}")
        data_list = response_json['data']['dataList']
        # 实际爬取的URL
        URLS = ['https://data.baai.ac.cn/details/' + item['uriName'] for item in data_list]
        return URLS

    def _get_page_info(self, cur_page: int) -> Tuple[Dict, List]:
        """
        获取当前页面的数据集信息
        :param cur_page:            当前页数
        :return:                    当前页面的数据集信息, 异常链接
        """
        dataset_links_elements = {}
        exception_links = []
        # 返回到当前页
        self.return_to_current_page(cur_page)
        page_elements = self.driver.find_elements(By.XPATH, self.page_elements_xpath + '/div')
        total_dataset_len = len(page_elements)
        logging.info(f"Total dataset in current page: {total_dataset_len}")

        # 逐个点击数据集
        cur_dataset_link = None
        for index in range(total_dataset_len):
            try:
                dataset_item_xpath = f'{self.page_elements_xpath}/div[{index + 1}]'
                cur_dataset = self.driver.find_element(By.XPATH, dataset_item_xpath)
                # 获取数据集名称，简介，标签，下载量，浏览量，更新时间
                cur_dataset_divs = cur_dataset.find_elements(By.XPATH, 'div/div/div')
                dataset_name = cur_dataset_divs[0].text
                dataset_tags = cur_dataset_divs[1].find_elements(By.XPATH, 'span')
                dataset_tag = [tag.text for tag in dataset_tags]
                dataset_intro = cur_dataset_divs[2].text
                dataset_info = cur_dataset_divs[3].find_elements(By.XPATH, 'div')
                dataset_download_num = dataset_info[0].text
                dataset_view_num = dataset_info[1].text
                dataset_update_time = dataset_info[2].text

                # 使用 ActionChains 来模拟点击
                action = ActionChains(self.driver)
                action.move_to_element(cur_dataset).click().perform()
                time.sleep(1)

                # 对页面截图
                self.driver.save_screenshot(f'{self.screen_shot_save_path}/{dataset_name}.png')

                # 详情页
                cur_dataset_link = self.driver.current_url
                cur_dataset_description = self.driver.find_element(By.XPATH, self.dataset_description_xpath).text
                self.driver.back()
                time.sleep(2)

                dataset_links_elements[cur_dataset_link] = {
                    'dataset_link': cur_dataset_link,
                    'dataset_name': dataset_name,
                    'dataset_tags': dataset_tag,
                    'dataset_intro': dataset_intro,
                    'dataset_download_num': dataset_download_num,
                    'dataset_view_num': dataset_view_num,
                    'dataset_update_time': dataset_update_time,
                    'dataset_description': cur_dataset_description
                }
                # 返回到当前页
                self.return_to_current_page(cur_page)
                logging.info(
                    f"Got dataset: {dataset_links_elements[cur_dataset_link]['dataset_link']}, dataset name: {dataset_name}")
            except Exception as e:
                logging.error(f"Error: {e}, when crawling page {cur_page} dataset {index}")
                if cur_dataset_link:
                    exception_links.append(cur_dataset_link)

        return dataset_links_elements, exception_links

    def return_to_current_page(self, cur_page):
        for i in range(cur_page):
            time.sleep(0.3)
            self.driver.find_element(By.XPATH, self.next_page_button_xpath).click()
        time.sleep(2)

    # 过时的方法，不再使用
    @deprecated(version='1.0', reason="This method is deprecated and is replaced by crawl method")
    def crawl_dataset_info(self):
        """
        爬取数据集所有信息
        :return:                None
        """
        all_info = {}
        exception_links = []
        # 打开网页,点击排序按钮选择排序方法
        self.driver.get(self.base_url)
        time.sleep(2)
        self.driver.find_elements(By.XPATH, self.sort_method_button_xpath)[0].click()
        time.sleep(1)
        self.driver.find_element(By.XPATH, self.sort_method_xpath).click()
        # 获取总页数
        total_page = self.driver.find_elements(By.XPATH, self.total_page_xpath)[-1].text
        logging.info(f"Total page: {total_page}")
        # 遍历所有页
        for page in range(int(total_page)):
            page_details_dict, exception_link = self._get_page_info(page)
            exception_links.extend(exception_link)
            intersection = set(all_info.keys()) & set(page_details_dict.keys())
            logging.info(f"Intersection: {intersection}")

            all_info.update(page_details_dict)
            logging.info(f"Current intersection: {intersection}")

        return all_info, exception_links

    def crawl(self):
        """
        根据request请求从后端接口获取到的数据集链接，爬取数据集信息
        """
        return self.get_single_item_data(self.targets, self.driver)

    def get_single_item_data(self, need_data_link_list: List, driver: object) -> Tuple[List, List]:
        """
        获取单个数据集的信息
        :param need_data_link_list:     需要获取信息的数据集链接
        :param driver:                  浏览器驱动
        :return:                        数据集信息，异常链接
        """
        result = []
        exception_links = []
        for link in need_data_link_list:
            try:
                self.driver.get(link)

                logging.info(f"Current link: {link}")
                time.sleep(1.5)
                # 获取数据集名称，简介，标签，下载量，浏览量，更新时间，描述
                info_divs = self.driver.find_elements(By.XPATH,
                                                 '//*[@id="app"]/div/section/main/div[1]/div[1]/div[1]/div/div')
                dataset_name = info_divs[0].text
                dataset_intro = info_divs[1].text
                dataset_tags = info_divs[2].find_elements(By.XPATH, 'span')
                dataset_tag = [tag.text for tag in dataset_tags]
                dataset_info = info_divs[3].find_elements(By.XPATH, 'div')
                dataset_download_num = dataset_info[0].text
                dataset_view_num = dataset_info[1].text
                dataset_update_time = dataset_info[2].text
                dataset_description = self.driver.find_element(By.XPATH,
                                                          '//*[@id="app"]/div/section/main/div[1]/div[2]/div[1]/div/div[2]/div[2]/div[1]').text

                # 截图
                self.driver.save_screenshot(f'{self.screen_shot_save_path}/{dataset_name}.png')
                link_data = {link: {
                    "dataset_link": link,
                    "dataset_name": dataset_name,
                    "dataset_tags": dataset_tag,
                    "dataset_intro": dataset_intro,
                    "dataset_download_num": dataset_download_num,
                    "dataset_view_num": dataset_view_num,
                    "dataset_update_time": dataset_update_time,
                    "dataset_description": dataset_description
                }}
                result.append(link_data)
            except Exception as e:
                logging.error(f"Error: {e}, when crawling {link}")
                exception_links.append(link)

        return result, exception_links


class BAAIDataPostProcess(DataPostProcess):
    """ BAAI 数据后处理 """
    organization_type = "BAAI"  # 用于获取映射

    def __init__(self, dataset_info_file_path: str):
        super().__init__(dataset_info_file_path)

    def post_process(self):
        # 取出上月的数据 —— 统计渠道列对应的值为
        BAAI_data = self.last_month_data[self.last_month_data['统计渠道'].str.strip() == 'BAAI']
        # 构造一个按照链接为key，其它信息为value的字典
        BAAI_data_dict = {}
        for index, row in BAAI_data.iterrows():
            BAAI_data_dict[row['链接']] = row

        result = []
        for data in self.dataset_info:
            data = data[list(data.keys())[0]]
            dataset_link = data.get('dataset_link', '')
            dataset_name = data.get('dataset_name', '')
            dataset_tags = data.get('dataset_tags', '')
            dataset_download_num = data.get('dataset_download_num', '')
            dataset_view_num = data.get('dataset_view_num', '')
            dataset_update_time = data.get('dataset_update_time', '').replace('更新', '').strip()
            dataset_description = self.remove_illegal_characters(data.get('dataset_description', ''))

            organization = '智源'
            sub_organization = 'BAAI'
            if dataset_link in BAAI_data_dict:
                is_new = '否'
                modality = BAAI_data_dict[dataset_link]['模态']
                life_cycle = BAAI_data_dict[dataset_link]['生命周期']
                last_month_download_num = self.parse_download_num(dataset_download_num) - BAAI_data_dict[dataset_link][
                    '下载量']
            else:
                is_new = '是'
                modality = None
                life_cycle = None
                last_month_download_num = self.parse_download_num(dataset_download_num)

            result.append(
                {'组织': organization, '机构': sub_organization, '数据集名称': dataset_name, '模态': modality,
                 '生命周期': life_cycle,
                 '链接': dataset_link, '上月下载量': last_month_download_num,
                 '数据集被使用量': '',
                 '统计渠道': 'BAAI',
                 '是否新增': is_new, '发布时间': dataset_update_time,
                 '下载量': self.parse_download_num(dataset_download_num),
                 'dataset_view_num': dataset_view_num,
                 'dataset_tags': dataset_tags, 'dataset_description': dataset_description
                 })

        # 保存数据
        self.save_to_excel(result, './result/BAAI/BAAI_dataset_info.xlsx')

    def _extract_need_info(self, data):
        pass

    def check_with_last_month(self, data):
        pass


if __name__ == '__main__':
    # ============================ 爬取数据 ============================
    # 重要参数
    sort_method = '下载量排序'
    sorted_method_map = {"下载量排序": 1, "浏览量排序": 2, "最新更新排序": 3}
    sort_method_index = sorted_method_map[sort_method]

    # 爬取数据集
    BAAI_crawler = BAAICrawler(sort_method_index=sort_method_index, headless=True,
                               screen_shot_save_path='./result/BAAI')
    BAAI_dataset_all_info, exception_links = BAAI_crawler.crawl()
    logging.info(f"Total dataset: {len(BAAI_dataset_all_info)}")
    logging.info(f"Exception links: {exception_links}")
    # 存储
    save_json_data(BAAI_dataset_all_info, './result/BAAI/BAAI_dataset_all_info.json')
    save_json_data(exception_links, './result/BAAI/BAAI_exception_links.json')
    # ============================ 数据后处理 ============================
    assert len(exception_links) == 0, "Some links are not crawled successfully"
    BAAI_post_process = BAAIDataPostProcess('./result/BAAI/BAAI_dataset_all_info.json')
    BAAI_post_process.post_process()
